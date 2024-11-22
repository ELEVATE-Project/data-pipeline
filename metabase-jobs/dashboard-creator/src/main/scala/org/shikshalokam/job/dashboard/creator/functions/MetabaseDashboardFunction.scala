package org.shikshalokam.job.dashboard.creator.functions

import com.twitter.util.Config.intoOption
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
    println(event)
    val dashboardData = event.readOrDefault[Map[String, Any]]("dashboardData", Map.empty[String, Any])

    val targetedStateId = dashboardData.get("targetedState") match {
      case Some(state: List[_]) if state.nonEmpty => state.head.toString
      case Some(state: String) => state
      case _ => ""
    }
    val targetedProgramId = dashboardData.get("targetedProgram") match {
      case Some(program: List[_]) if program.nonEmpty => program.head.toString
      case Some(program: String) => program
      case _ => ""
    }
    val targetedDistrictId = dashboardData.get("targetedDistrict") match {
      case Some(district: List[_]) if district.nonEmpty => district.head.toString
      case Some(district: String) => district
      case _ => ""
    }
    val Admin = dashboardData.get("admin") match {
      case Some(admin: List[_]) if admin.nonEmpty => admin.head.toString
      case Some(admin: String) => admin
      case _ => ""
    }

    // Printing the targetedState ID
    println(s"Targeted State ID: $targetedStateId")
    println(s"Targeted Program ID: $targetedProgramId")
    println(s"Targeted District ID: $targetedDistrictId")
    println(s"admin: $Admin")

    if (event.reportType == "Project") {
      println(s">>>>>>>>>>> Started Processing Metabase Project Dashboards >>>>>>>>>>>>")
      val mainDir: String = "/home/user1/Documents/elevate-data-pipeline3/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson"
        if (Admin.nonEmpty) {
          println(s"********** Started Processing Metabase Admin Dashboard ***********")
          try {
            val collectionName: String = s"Admin Collection"
            val dashboardName: String = s"Project Admin Report"
            val parameterFilePath:String = "/home/user1/Documents/elevate-data-pipeline3/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/admin-parameter.json"
            val (collectionId, databaseId, dashboardId) = CreateDashboard.Get_the_required_ids(CollectionName = collectionName, DashboardName = dashboardName, reportType = event.reportType,metabaseUtil,config)
            val (statenameId,districtnameId,programnameId) = GetTableData.getTableMetadata(databaseId, metabaseUtil, config)
            val questionCardIdList = UpdateAdminJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDir, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil)
            println(s"questionCardIdList = $questionCardIdList")
            val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
            println(s"questionIdsString = $questionIdsString")
            addQuestionCards.addQuestionCardsFunction(metabaseUtil, mainDir, dashboardId)
            UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parameterFilePath, dashboardId)
            val updateTableQuery = s"UPDATE admin_dashboard_metadata SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Completed' WHERE name = 'Admin';"
            postgresUtil.insertData(updateTableQuery)
          } catch {
            case e: Exception =>
              val updateTableQuery = s"UPDATE admin_dashboard_metadata SET status = 'Incomplete',error_message = '${e.getMessage}'  WHERE name = 'Admin';"
              postgresUtil.insertData(updateTableQuery)
              println(s"An error occurred: ${e.getMessage}")
              e.printStackTrace()
          }
          println(s"********** Completed Processing Metabase Admin Dashboard ***********")
        }else{
          println(s"Admin Dashboard Might Be Already Created")
        }

      if (targetedStateId.nonEmpty){
        println(s"********** Started Processing Metabase State Dashboard ***********")
        val stateId = targetedStateId
        try {
          val stateNameQuery = s"SELECT name from state_dashboard_metadata where id = '$stateId'"
          val stateName = postgresUtil.fetchData(stateNameQuery) match {
            case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
            case _ => ""
          }
          println(s"stateName = $stateName")
          val collectionName = s"State Collection [$stateName]"
          val dashboardName = s"Project State Report [$stateName]"
          val parameterFilePath:String = "/home/user1/Documents/elevate-data-pipeline3/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/state-parameter.json"
          val (collectionId, databaseId, dashboardId) = CreateDashboard.Get_the_required_ids(CollectionName = collectionName, DashboardName = dashboardName, reportType = event.reportType,metabaseUtil,config)
          val (statenameId,districtnameId,programnameId) = GetTableData.getTableMetadata(databaseId, metabaseUtil, config)
          val questionCardIdList =  UpdateStateJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDir, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil,stateName)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          addQuestionCards.addQuestionCardsFunction(metabaseUtil, mainDir, dashboardId)
          val filterFilePath:String = "/home/user1/Documents/elevate-data-pipeline3/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/state-filter.json"
          val stateIdFilter : Int = UpdateAndAddStateFilter.updateAndAddFilter(metabaseUtil = metabaseUtil, filterFilePath = filterFilePath, statename = s"$stateName", districtname = null, programname = null,collectionId,databaseId)
          UpdateParameters.updateStateParameterFunction(metabaseUtil, parameterFilePath, dashboardId,stateIdFilter)
          val updateTableQuery = s"UPDATE state_dashboard_metadata SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Completed' WHERE id = '$stateId';"
          postgresUtil.insertData(updateTableQuery)
        } catch {
          case e: Exception =>
            val updateTableQuery = s"UPDATE state_dashboard_metadata SET status = 'Incomplete',error_message = '${e.getMessage}'  WHERE id = '$stateId';"
            postgresUtil.insertData(updateTableQuery)
            println(s"An error occurred: ${e.getMessage}")
            e.printStackTrace()
        }
        println(s"********** Completed Processing Metabase State Dashboard ***********")
      } else {
        println("targetedState is not present or is empty")
      }

      if (targetedProgramId.nonEmpty){
        println(s"********** Started Processing Metabase Program Dashboard ***********")
        val programId = targetedProgramId
        try {
          val programNameQuery = s"SELECT name from program_dashboard_metadata where id = '$programId'"
          val programName = postgresUtil.fetchData(programNameQuery) match {
            case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
            case _ => ""
          }
          val collectionName = s"Program Collection [$programName]"
          val dashboardName = s"Project Program Report [$programName]"
          val parameterFilePath: String = "/home/user1/Documents/elevate-data-pipeline3/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/program-parameter.json"
          val (collectionId, databaseId, dashboardId) = CreateDashboard.Get_the_required_ids(CollectionName = collectionName, DashboardName = dashboardName, reportType = event.reportType, metabaseUtil, config)
          val (statenameId, districtnameId, programnameId) = GetTableData.getTableMetadata(databaseId, metabaseUtil, config)
          val questionCardIdList = UpdateProgramJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDir, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil,programName)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          addQuestionCards.addQuestionCardsFunction(metabaseUtil, mainDir, dashboardId)
          val filterFilePath:String = "/home/user1/Documents/elevate-data-pipeline3/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/program-filter.json"
          val programIdFilter : Int = UpdateAndAddProgramFilter.updateAndAddFilter(metabaseUtil = metabaseUtil, filterFilePath = filterFilePath, statename = null, districtname = null, programname = s"$programName",collectionId,databaseId)
          UpdateParameters.UpdateProgramParameterFunction(metabaseUtil, parameterFilePath, dashboardId, programName,programIdFilter)
          val updateTableQuery = s"UPDATE program_dashboard_metadata SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Completed' WHERE id = '$programId';"
          postgresUtil.insertData(updateTableQuery)
        } catch {
          case e: Exception =>
            val updateTableQuery = s"UPDATE program_dashboard_metadata SET status = 'Incomplete',error_message = '${e.getMessage}'  WHERE id = '$programId';"
            postgresUtil.insertData(updateTableQuery)
            println(s"An error occurred: ${e.getMessage}")
            e.printStackTrace()
        }
        println(s"********** Completed Processing Metabase Program Dashboard ***********")
      }else {
        println("targetedProgram is not present or is empty")
      }

      if (targetedDistrictId.nonEmpty){
        println(s"********** Started Processing Metabase District Dashboard ***********")
        val DistrictId = targetedDistrictId
        try {
          val DistrictNameQuery = s"SELECT name from district_dashboard_metadata where id = '$DistrictId'"
          val districtname = postgresUtil.fetchData(DistrictNameQuery) match {
            case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
            case _ => ""
          }
          val statenamequery = s"SELECT distinct(statename) AS name from projects where districtid = '$DistrictId'"
          val statename = postgresUtil.fetchData(statenamequery) match {
            case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
            case _ => ""
          }
          val collectionName = s"District collection [$districtname - $statename]"
          val dashboardName = s"Project District Report [$districtname - $statename]"
          val parameterFilePath:String = "/home/user1/Documents/elevate-data-pipeline3/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/district-parameter.json"
          val (collectionId, databaseId, dashboardId) = CreateDashboard.Get_the_required_ids(CollectionName = collectionName, DashboardName = dashboardName, reportType = event.reportType,metabaseUtil,config)
          val (statenameId,districtnameId,programnameId) = GetTableData.getTableMetadata(databaseId, metabaseUtil, config)
          val questionCardIdList = UpdateDistrictJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDir, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil,statename,districtname)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          addQuestionCards.addQuestionCardsFunction(metabaseUtil, mainDir, dashboardId)
          val filterFilePath:String = "/home/user1/Documents/elevate-data-pipeline3/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/district-filter.json"
          val districtIdFilter : Int = UpdateAndAddDistrictFilter.updateAndAddFilter(metabaseUtil = metabaseUtil, filterFilePath = filterFilePath, statename = s"$statename", districtname = s"$districtname", programname = null,collectionId,databaseId)
          UpdateParameters.UpdateDistrictParameterFunction(metabaseUtil,parameterFilePath,dashboardId,statename,districtname,districtIdFilter)
          val updateTableQuery = s"UPDATE district_dashboard_metadata SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Completed' WHERE id = '$DistrictId';"
          postgresUtil.insertData(updateTableQuery)
        } catch {
          case e: Exception =>
            val updateTableQuery = s"UPDATE district_dashboard_metadata SET status = 'Incomplete',error_message = '${e.getMessage}'  WHERE id = '$DistrictId';"
            postgresUtil.insertData(updateTableQuery)
            println(s"An error occurred: ${e.getMessage}")
            e.printStackTrace()
        }
        println(s"********** Completed Processing Metabase District Dashboard ***********")
      }
    println(s"***************** End of Processing the Metabase Project Dashboard *****************\n")
  }
}
}
