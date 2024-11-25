package org.shikshalokam.job.dashboard.creator.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.BaseProcessFunction
import org.shikshalokam.job.dashboard.creator.domain.Event
import org.shikshalokam.job.util.PostgresUtil
import org.shikshalokam.job.dashboard.creator.task.MetabaseDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory


import scala.collection.immutable._

class MetabaseDashboardFunction(config: MetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[MetabaseDashboardFunction])

  override def metricsList(): List[String] = {
    List(config.metabaseDashboardCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val pgHost: String = config.pgHost
    val pgPort: String = config.pgPort
    val pgUsername: String = config.pgUsername
    val pgPassword: String = config.pgPassword
    val pgDataBase: String = config.pgDataBase
    val metabaseUrl: String = config.metabaseUrl
    val metabaseUsername: String = config.metabaseUsername
    val metabasePassword: String = config.metabasePassword
    val connectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$pgDataBase"
    postgresUtil = new PostgresUtil(connectionUrl, pgUsername, pgPassword)
    metabaseUtil = new MetabaseUtil(metabaseUrl, metabaseUsername, metabasePassword)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {

    println(s"***************** Start of Processing the Metabase Dashboard Event with Id = ${event._id}*****************")

    val targetedStateId = event.targetedState
    val targetedProgramId = event.targetedProgram
    val targetedDistrictId = event.targetedDistrict
    val admin = event.admin
    // Printing the targetedState ID
    println(s"Targeted State ID: $targetedStateId")
    println(s"Targeted Program ID: $targetedProgramId")
    println(s"Targeted District ID: $targetedDistrictId")
    println(s"admin: $admin")

    if (event.reportType == "Project") {
      println(s">>>>>>>>>>> Started Processing Metabase Project Dashboards >>>>>>>>>>>>")
      val mainDir: String = "metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson"
      val mainDirAbsolutePath: String = new java.io.File(mainDir).getCanonicalPath
        if (admin.nonEmpty) {
          println(s"********** Started Processing Metabase Admin Dashboard ***********")
          val AdminIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM admin_dashboard_metadata WHERE name = '$admin') THEN CASE WHEN COALESCE((SELECT status FROM admin_dashboard_metadata WHERE name = '$admin'), '') = 'Success' THEN 'success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
          val AdminIdStatus = postgresUtil.fetchData(AdminIdCheckQuery) match {
            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
            case _ => ""
          }
          if (AdminIdStatus == "Failed") {
            try {
              val collectionName: String = s"Admin Collection"
              val dashboardName: String = s"Project Admin Report"
              val GroupName: String = s"Report_Admin"
              val metabaseDatabase: String = config.metabaseDatabase
              val parameterFilePath: String = "metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/admin-parameter.json"
              val parameterFileAbsPath: String = new java.io.File(parameterFilePath).getCanonicalPath
              val CollectionQuery = s"UPDATE admin_dashboard_metadata SET status = 'Failed',error_message = 'errorMessage'  WHERE name = '$admin';"
              val collectionId : Int = CreateDashboard.checkAndCreateCollection(collectionName = collectionName, ReportType = "Project", metabaseUtil = metabaseUtil, postgresUtil = postgresUtil,metaTableQuery = CollectionQuery)
              val DashboardQuery = s"UPDATE admin_dashboard_metadata SET status = 'Failed',error_message = 'errorMessage'  WHERE name = '$admin';"
              val dashboardId : Int = CreateDashboard.checkAndCreateDashboard(collectionId = collectionId, dashboardName = dashboardName, metabaseUtil = metabaseUtil, postgresUtil = postgresUtil, metaTableQuery = DashboardQuery)
              val databaseId : Int = CreateDashboard.getDatabaseId(metabaseDatabase = metabaseDatabase, metabaseUtil = metabaseUtil)
              println(s"collectionId = $collectionId")
              println(s"dashboardId = $dashboardId")
              println(s"databaseId = $databaseId")
              val statenameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId,"projects", "statename")
              val districtnameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId, "projects", "districtname")
              val programnameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId, "solutions", "programname")
              val questionCardIdList = UpdateAdminJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDirAbsolutePath, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil)
              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
              println(s"questionIdsString = $questionIdsString")
              addQuestionCards.addQuestionCardsFunction(metabaseUtil, mainDirAbsolutePath, dashboardId)
              UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parameterFileAbsPath, dashboardId)
              val updateTableQuery = s"UPDATE admin_dashboard_metadata SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success', error_message = '' WHERE name = 'Admin';"
              println(s"Generated query: $updateTableQuery")
              postgresUtil.insertData(updateTableQuery)
              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil,GroupName,collectionId)
            } catch {
              case e: Exception =>
                val updateTableQuery = s"UPDATE admin_dashboard_metadata SET status = 'Failed',error_message = '${e.getMessage}'  WHERE name = 'Admin';"
                postgresUtil.insertData(updateTableQuery)
                println(s"An error occurred: ${e.getMessage}")
                e.printStackTrace()
            }
            println(s"********** Completed Processing Metabase Admin Dashboard ***********")
          } else {
            println(s"Admin Dashboard has already created hence skipping the process !!!!!!!!!!!!")
          }
        }else {
          println(s"admin key is not present or is empty")
        }

      if (targetedStateId.nonEmpty) {
        println(s"********** Started Processing Metabase State Dashboard ***********")
        val stateIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM state_dashboard_metadata WHERE id = '$targetedStateId') THEN CASE WHEN COALESCE((SELECT status FROM state_dashboard_metadata WHERE id = '$targetedStateId'), '') = 'Success' THEN 'Success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
        val stateIdStatus = postgresUtil.fetchData(stateIdCheckQuery) match {
          case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
          case _ => ""
        }
        if (stateIdStatus == "Failed") {
          try {
            val stateNameQuery = s"SELECT name from state_dashboard_metadata where id = '$targetedStateId'"
            val stateName = postgresUtil.fetchData(stateNameQuery) match {
              case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
              case _ => ""
            }
            println(s"stateName = $stateName")
            val collectionName = s"State Collection [$stateName]"
            val dashboardName = s"Project State Report [$stateName]"
            val GroupName: String = s"${stateName}_State_Manager"
            val metabaseDatabase: String = config.metabaseDatabase
            val parameterFilePath: String = "metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/state-parameter.json"
            val parameterFileAbsPath: String = new java.io.File(parameterFilePath).getCanonicalPath
            val CollectionQuery = s"UPDATE state_dashboard_metadata SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$targetedStateId';"
            val collectionId : Int = CreateDashboard.checkAndCreateCollection(collectionName = collectionName, ReportType = "Project", metabaseUtil = metabaseUtil, postgresUtil = postgresUtil,metaTableQuery = CollectionQuery)
            val DashboardQuery = s"UPDATE state_dashboard_metadata SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$targetedStateId';"
            val dashboardId : Int = CreateDashboard.checkAndCreateDashboard(collectionId = collectionId, dashboardName = dashboardName, metabaseUtil = metabaseUtil, postgresUtil = postgresUtil, metaTableQuery = DashboardQuery)
            val databaseId : Int = CreateDashboard.getDatabaseId(metabaseDatabase = metabaseDatabase, metabaseUtil = metabaseUtil)
            val statenameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId,"projects", "statename")
            val districtnameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId, "projects", "districtname")
            val programnameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId, "solutions", "programname")
            val questionCardIdList = UpdateStateJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDirAbsolutePath, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil, stateName)
            val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
            addQuestionCards.addQuestionCardsFunction(metabaseUtil, mainDirAbsolutePath, dashboardId)
            val filterFilePath: String = "metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/state-filter.json"
            val filterFileAbsPath: String = new java.io.File(filterFilePath).getCanonicalPath
            val stateIdFilter: Int = UpdateAndAddStateFilter.updateAndAddFilter(metabaseUtil = metabaseUtil, filterFilePath = filterFileAbsPath, statename = s"$stateName", districtname = null, programname = null, collectionId, databaseId)
            UpdateParameters.updateStateParameterFunction(metabaseUtil, parameterFileAbsPath, dashboardId, stateIdFilter)
            val updateTableQuery = s"UPDATE state_dashboard_metadata SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success',error_message = '' WHERE id = '$targetedStateId';"
            postgresUtil.insertData(updateTableQuery)
            CreateAndAssignGroup.createGroupToDashboard(metabaseUtil,GroupName,collectionId)
          } catch {
            case e: Exception =>
              val updateTableQuery = s"UPDATE state_dashboard_metadata SET status = 'Failed',error_message = '${e.getMessage}'  WHERE id = '$targetedStateId';"
              postgresUtil.insertData(updateTableQuery)
              println(s"An error occurred: ${e.getMessage}")
              e.printStackTrace()
          }
          println(s"********** Completed Processing Metabase State Dashboard ***********")
        } else {
          println(s"state report has already created hence skipping the process !!!!!!!!!!!!")
        }
      }else {
        println("targetedState is not present or is empty")
      }


      if (targetedProgramId.nonEmpty) {
        println(s"********** Started Processing Metabase Program Dashboard ***********")
        val programIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM program_dashboard_metadata WHERE id = '$targetedProgramId') THEN CASE WHEN COALESCE((SELECT status FROM program_dashboard_metadata WHERE id = '$targetedProgramId'), '') = 'Success' THEN 'Success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
        val programIdStatus = postgresUtil.fetchData(programIdCheckQuery) match {
          case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
          case _ => ""
        }
        if (programIdStatus == "Failed") {
          try {
            val programNameQuery = s"SELECT name from program_dashboard_metadata where id = '$targetedProgramId'"
            val programName = postgresUtil.fetchData(programNameQuery) match {
              case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
              case _ => ""
            }
            val collectionName = s"Program Collection [$programName]"
            val dashboardName = s"Project Program Report [$programName]"
            val GroupName: String = s"Program_Manager[$programName]"
            val parameterFilePath: String = "metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/program-parameter.json"
            val parameterFileAbsPath: String = new java.io.File(parameterFilePath).getCanonicalPath
            val metabaseDatabase: String = config.metabaseDatabase
            val CollectionQuery = s"UPDATE admin_dashboard_metadata SET status = 'Failed',error_message = 'errorMessage'  WHERE name = '$admin';"
            val collectionId : Int = CreateDashboard.checkAndCreateCollection(collectionName = collectionName, ReportType = "Project", metabaseUtil = metabaseUtil, postgresUtil = postgresUtil,metaTableQuery = CollectionQuery)
            val DashboardQuery = s"UPDATE admin_dashboard_metadata SET status = 'Failed',error_message = 'errorMessage'  WHERE name = '$admin';"
            val dashboardId : Int = CreateDashboard.checkAndCreateDashboard(collectionId = collectionId, dashboardName = dashboardName, metabaseUtil = metabaseUtil, postgresUtil = postgresUtil, metaTableQuery = DashboardQuery)
            val databaseId : Int = CreateDashboard.getDatabaseId(metabaseDatabase = metabaseDatabase, metabaseUtil = metabaseUtil)
            val statenameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId,"projects", "statename")
            val districtnameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId, "projects", "districtname")
            val programnameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId, "solutions", "programname")
            val questionCardIdList = UpdateProgramJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDirAbsolutePath, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil, programName)
            val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
            addQuestionCards.addQuestionCardsFunction(metabaseUtil, mainDirAbsolutePath, dashboardId)
            val filterFilePath: String = "metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/program-filter.json"
            val filterFileAbsPath: String = new java.io.File(filterFilePath).getCanonicalPath
            val programIdFilter: Int = UpdateAndAddProgramFilter.updateAndAddFilter(metabaseUtil = metabaseUtil, filterFilePath = filterFileAbsPath, statename = null, districtname = null, programname = s"$programName", collectionId, databaseId)
            UpdateParameters.UpdateProgramParameterFunction(metabaseUtil, parameterFileAbsPath, dashboardId, programName, programIdFilter)
            val updateTableQuery = s"UPDATE program_dashboard_metadata SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success',error_message = '' WHERE id = '$targetedProgramId';"
            postgresUtil.insertData(updateTableQuery)
            CreateAndAssignGroup.createGroupToDashboard(metabaseUtil,GroupName,collectionId)
          } catch {
            case e: Exception =>
              val updateTableQuery = s"UPDATE program_dashboard_metadata SET status = 'Failed',error_message = '${e.getMessage}'  WHERE id = '$targetedProgramId';"
              postgresUtil.insertData(updateTableQuery)
              println(s"An error occurred: ${e.getMessage}")
              e.printStackTrace()
          }
          println(s"********** Completed Processing Metabase Program Dashboard ***********")
        } else {
          println("program report has already created hence skipping the process !!!!!!!!!!!!")
        }
      }else{
        println("targetedProgram key is either not present or empty")
      }

      if (targetedDistrictId.nonEmpty){
        println(s"********** Started Processing Metabase District Dashboard ***********")
        val districtIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM district_dashboard_metadata WHERE id = '$targetedDistrictId') THEN CASE WHEN COALESCE((SELECT status FROM district_dashboard_metadata WHERE id = '$targetedDistrictId'), '') = 'Success' THEN 'Success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
        val districtIdStatus = postgresUtil.fetchData(districtIdCheckQuery) match {
          case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
          case _ => ""
        }
        if (districtIdStatus == "Failed") {
          try {
            val DistrictNameQuery = s"SELECT name from district_dashboard_metadata where id = '$targetedDistrictId'"
            val districtname = postgresUtil.fetchData(DistrictNameQuery) match {
              case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
              case _ => ""
            }
            val statenamequery = s"SELECT distinct(statename) AS name from projects where districtid = '$targetedDistrictId'"
            val statename = postgresUtil.fetchData(statenamequery) match {
              case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
              case _ => ""
            }
            val collectionName = s"District collection [$districtname - $statename]"
            val dashboardName = s"Project District Report [$districtname - $statename]"
            val GroupName: String = s"${districtname}_District_Manager[$statename]"
            val metabaseDatabase: String = config.metabaseDatabase
            val parameterFilePath:String = "metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/district-parameter.json"
            val parameterFileAbsPath: String = new java.io.File(parameterFilePath).getCanonicalPath
            val CollectionQuery = s"UPDATE district_dashboard_metadata SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$targetedDistrictId';"
            val collectionId : Int = CreateDashboard.checkAndCreateCollection(collectionName = collectionName, ReportType = "Project", metabaseUtil = metabaseUtil, postgresUtil = postgresUtil,metaTableQuery = CollectionQuery)
            val DashboardQuery = s"UPDATE district_dashboard_metadata SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$targetedDistrictId';"
            val dashboardId : Int = CreateDashboard.checkAndCreateDashboard(collectionId = collectionId, dashboardName = dashboardName, metabaseUtil = metabaseUtil, postgresUtil = postgresUtil, metaTableQuery = DashboardQuery)
            val databaseId : Int = CreateDashboard.getDatabaseId(metabaseDatabase = metabaseDatabase, metabaseUtil = metabaseUtil)
            val statenameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId,"projects", "statename")
            val districtnameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId, "projects", "districtname")
            val programnameId: Int = GetTableData.getFieldId(metabaseUtil: MetabaseUtil,databaseId, "solutions", "programname")
            val questionCardIdList = UpdateDistrictJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDirAbsolutePath, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil,statename,districtname)
            val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
            addQuestionCards.addQuestionCardsFunction(metabaseUtil, mainDirAbsolutePath, dashboardId)
            val filterFilePath:String = "metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/district-filter.json"
            val filterFileAbsPath: String = new java.io.File(filterFilePath).getCanonicalPath
            val districtIdFilter : Int = UpdateAndAddDistrictFilter.updateAndAddFilter(metabaseUtil = metabaseUtil, filterFilePath = filterFileAbsPath, statename = s"$statename", districtname = s"$districtname", programname = null,collectionId,databaseId)
            UpdateParameters.UpdateDistrictParameterFunction(metabaseUtil,parameterFileAbsPath,dashboardId,statename,districtname,districtIdFilter)
            val updateTableQuery = s"UPDATE district_dashboard_metadata SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success',error_message = '' WHERE id = '$targetedDistrictId';"
            postgresUtil.insertData(updateTableQuery)
            CreateAndAssignGroup.createGroupToDashboard(metabaseUtil,GroupName,collectionId)
          } catch {
            case e: Exception =>
              val updateTableQuery = s"UPDATE district_dashboard_metadata SET status = 'Failed',error_message = '${e.getMessage}'  WHERE id = '$targetedDistrictId';"
              postgresUtil.insertData(updateTableQuery)
              println(s"An error occurred: ${e.getMessage}")
              e.printStackTrace()
          }
          println(s"********** Completed Processing Metabase District Dashboard ***********")
        }else {
          println("district report has already created hence skipping the process !!!!!!!!!!!!")
        }
      }else {
        println("targetedDistrict key is either not present or empty")
      }
    println(s"***************** End of Processing the Metabase Project Dashboard *****************\n")
  }
}
}
