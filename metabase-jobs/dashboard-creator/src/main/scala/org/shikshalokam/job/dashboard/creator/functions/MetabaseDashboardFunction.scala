package org.shikshalokam.job.dashboard.creator.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.dashboard.creator.domain.Event
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

    event.reportType match {
      case "Project" =>
        println(s">>>>>>>>>>> Started Processing Metabase Project Dashboards >>>>>>>>>>>>")
        val mainDir = config.projectJsonPath
        println(s"mainDir = $mainDir")
        if (admin.nonEmpty) {
          println(s"********** Started Processing Metabase Admin Dashboard ***********")
          val adminMetaDataTable = config.pgAdminTable
          val adminIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM $adminMetaDataTable WHERE name = '$admin') THEN CASE WHEN COALESCE((SELECT status FROM $adminMetaDataTable WHERE name = '$admin'), '') = 'Success' THEN 'success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
          val adminIdStatus = postgresUtil.fetchData(adminIdCheckQuery) match {
            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
            case _ => ""
          }
          if (adminIdStatus == "Failed") {
            try {
              val collectionName: String = s"Admin Collection"
              val dashboardName: String = s"Project Admin Report"
              val groupName: String = s"Report_Admin"
              val metabaseDatabase: String = config.metabaseDatabase
              val parameterFilePath: String = s"$mainDir/admin-parameter.json"
              println(s"parameterFilePath = $parameterFilePath")
              val createDashboardQuery = s"UPDATE $adminMetaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE name = '$admin';"
              val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"Admin Report", metabaseUtil, postgresUtil, createDashboardQuery)
              val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
              val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
              val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "projects", "statename", postgresUtil, createDashboardQuery)
              val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "projects", "districtname", postgresUtil, createDashboardQuery)
              val programnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "solutions", "programname", postgresUtil, createDashboardQuery)
              val questionCardIdList = UpdateAdminJsonFiles.ProcessAndUpdateJsonFiles(mainDir, collectionId, databaseId, dashboardId, statenameId, districtnameId, programnameId, metabaseUtil)
              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
              println(s"questionIdsString = $questionIdsString")
              UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parameterFilePath, dashboardId)
              val updateTableQuery = s"UPDATE $adminMetaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success', error_message = '' WHERE name = 'Admin';"
              println(s"Generated query: $updateTableQuery")
              postgresUtil.insertData(updateTableQuery)
              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
            } catch {
              case e: Exception =>
                val updateTableQuery = s"UPDATE $adminMetaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE name = 'Admin';"
                postgresUtil.insertData(updateTableQuery)
                println(s"An error occurred: ${e.getMessage}")
                e.printStackTrace()
            }
            println(s"********** Completed Processing Metabase Admin Dashboard ***********")
          } else {
            println(s"Admin Dashboard has already created hence skipping the process !!!!!!!!!!!!")
          }
        } else {
          println(s"admin key is not present or is empty")
        }

        if (targetedStateId.nonEmpty) {
          println(s"********** Started Processing Metabase State Dashboard ***********")
          val stateMetaDataTable = config.pgStateTable
          val stateIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM $stateMetaDataTable WHERE id = '$targetedStateId') THEN CASE WHEN COALESCE((SELECT status FROM $stateMetaDataTable WHERE id = '$targetedStateId'), '') = 'Success' THEN 'Success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
          val stateIdStatus = postgresUtil.fetchData(stateIdCheckQuery) match {
            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
            case _ => ""
          }
          if (stateIdStatus == "Failed") {
            try {
              val stateNameQuery = s"SELECT name from $stateMetaDataTable where id = '$targetedStateId'"
              val stateName = postgresUtil.fetchData(stateNameQuery) match {
                case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
                case _ => ""
              }
              println(s"stateName = $stateName")
              val collectionName = s"State Collection [$stateName]"
              val dashboardName = s"Project State Report [$stateName]"
              val groupName: String = s"${stateName}_State_Manager"
              val metabaseDatabase: String = config.metabaseDatabase
              val parameterFilePath: String = s"$mainDir/state-parameter.json"
              val createDashboardQuery = s"UPDATE $stateMetaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$targetedStateId';"
              val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"State Report [$stateName]", metabaseUtil, postgresUtil, createDashboardQuery)
              val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
              val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
              val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "projects", "statename", postgresUtil, createDashboardQuery)
              val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "projects", "districtname", postgresUtil, createDashboardQuery)
              val programnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "solutions", "programname", postgresUtil, createDashboardQuery)
              val questionCardIdList = UpdateStateJsonFiles.ProcessAndUpdateJsonFiles(mainDir, collectionId, databaseId, dashboardId, statenameId, districtnameId, programnameId, metabaseUtil, stateName)
              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
              val filterFilePath: String = s"$mainDir/state-filter.json"
              val stateIdFilter: Int = UpdateAndAddStateFilter.updateAndAddFilter(metabaseUtil, filterFilePath, s"$stateName", null, null, collectionId, databaseId)
              UpdateParameters.updateStateParameterFunction(metabaseUtil, parameterFilePath, dashboardId, stateIdFilter)
              val updateTableQuery = s"UPDATE $stateMetaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success',error_message = '' WHERE id = '$targetedStateId';"
              postgresUtil.insertData(updateTableQuery)
              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
            } catch {
              case e: Exception =>
                val updateTableQuery = s"UPDATE $stateMetaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE id = '$targetedStateId';"
                postgresUtil.insertData(updateTableQuery)
                println(s"An error occurred: ${e.getMessage}")
                e.printStackTrace()
            }
            println(s"********** Completed Processing Metabase State Dashboard ***********")
          } else {
            println(s"state report has already created hence skipping the process !!!!!!!!!!!!")
          }
        } else {
          println("targetedState is not present or is empty")
        }


        if (targetedProgramId.nonEmpty) {
          println(s"********** Started Processing Metabase Program Dashboard ***********")
          val programMetaDataTable = config.pgProgramTable
          val programIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM $programMetaDataTable WHERE id = '$targetedProgramId') THEN CASE WHEN COALESCE((SELECT status FROM $programMetaDataTable WHERE id = '$targetedProgramId'), '') = 'Success' THEN 'Success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
          val programIdStatus = postgresUtil.fetchData(programIdCheckQuery) match {
            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
            case _ => ""
          }
          if (programIdStatus == "Failed") {
            try {
              val programNameQuery = s"SELECT name from $programMetaDataTable where id = '$targetedProgramId'"
              val programName = postgresUtil.fetchData(programNameQuery) match {
                case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
                case _ => ""
              }
              val collectionName = s"Program Collection [$programName]"
              val dashboardName = s"Project Program Report [$programName]"
              val groupName: String = s"Program_Manager[$programName]"
              val parameterFilePath: String = s"$mainDir/program-parameter.json"
              val metabaseDatabase: String = config.metabaseDatabase
              val createDashboardQuery = s"UPDATE $programMetaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$targetedProgramId';"
              val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"Program Report [$programName]", metabaseUtil, postgresUtil, createDashboardQuery)
              val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
              val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
              val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "projects", "statename", postgresUtil, createDashboardQuery)
              val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "projects", "districtname", postgresUtil, createDashboardQuery)
              val programnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "solutions", "programname", postgresUtil, createDashboardQuery)
              val questionCardIdList = UpdateProgramJsonFiles.ProcessAndUpdateJsonFiles(mainDir, collectionId, databaseId, dashboardId, statenameId, districtnameId, programnameId, metabaseUtil, programName)
              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
              val filterFilePath: String = s"$mainDir/program-filter.json"
              val programIdFilter: Int = UpdateAndAddProgramFilter.updateAndAddFilter(metabaseUtil, filterFilePath, null, null, s"$programName", collectionId, databaseId)
              UpdateParameters.UpdateProgramParameterFunction(metabaseUtil, parameterFilePath, dashboardId, programIdFilter)
              val updateTableQuery = s"UPDATE $programMetaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success',error_message = '' WHERE id = '$targetedProgramId';"
              postgresUtil.insertData(updateTableQuery)
              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
            } catch {
              case e: Exception =>
                val updateTableQuery = s"UPDATE $programMetaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE id = '$targetedProgramId';"
                postgresUtil.insertData(updateTableQuery)
                println(s"An error occurred: ${e.getMessage}")
                e.printStackTrace()
            }
            println(s"********** Completed Processing Metabase Program Dashboard ***********")
          } else {
            println("program report has already created hence skipping the process !!!!!!!!!!!!")
          }
        } else {
          println("targetedProgram key is either not present or empty")
        }

        if (targetedDistrictId.nonEmpty) {
          println(s"********** Started Processing Metabase District Dashboard ***********")
          val districtMetaDataTable = config.pgDistrictTable
          val districtIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM $districtMetaDataTable WHERE id = '$targetedDistrictId') THEN CASE WHEN COALESCE((SELECT status FROM $districtMetaDataTable WHERE id = '$targetedDistrictId'), '') = 'Success' THEN 'Success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
          val districtIdStatus = postgresUtil.fetchData(districtIdCheckQuery) match {
            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
            case _ => ""
          }
          if (districtIdStatus == "Failed") {
            try {
              val districtNameQuery = s"SELECT name from $districtMetaDataTable where id = '$targetedDistrictId'"
              val districtname = postgresUtil.fetchData(districtNameQuery) match {
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
              val groupName: String = s"${districtname}_District_Manager[$statename]"
              val metabaseDatabase: String = config.metabaseDatabase
              val parameterFilePath: String = s"$mainDir/district-parameter.json"
              val createDashboardQuery = s"UPDATE $districtMetaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$targetedDistrictId';"
              val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"District Report [$districtname - $statename]", metabaseUtil, postgresUtil, createDashboardQuery)
              val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
              val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
              val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "projects", "statename", postgresUtil, createDashboardQuery)
              val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "projects", "districtname", postgresUtil, createDashboardQuery)
              val programnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, "solutions", "programname", postgresUtil, createDashboardQuery)
              val questionCardIdList = UpdateDistrictJsonFiles.ProcessAndUpdateJsonFiles(mainDir, collectionId, databaseId, dashboardId, statenameId, districtnameId, programnameId, metabaseUtil, statename, districtname)
              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
              val filterFilePath: String = s"$mainDir/district-filter.json"
              val districtIdFilter: Int = UpdateAndAddDistrictFilter.updateAndAddFilter(metabaseUtil, filterFilePath, s"$statename", s"$districtname", null, collectionId, databaseId)
              UpdateParameters.UpdateDistrictParameterFunction(metabaseUtil, parameterFilePath, dashboardId, districtIdFilter)
              val updateTableQuery = s"UPDATE $districtMetaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success',error_message = '' WHERE id = '$targetedDistrictId';"
              postgresUtil.insertData(updateTableQuery)
              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
            } catch {
              case e: Exception =>
                val updateTableQuery = s"UPDATE $districtMetaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE id = '$targetedDistrictId';"
                postgresUtil.insertData(updateTableQuery)
                println(s"An error occurred: ${e.getMessage}")
                e.printStackTrace()
            }
            println(s"********** Completed Processing Metabase District Dashboard ***********")
          } else {
            println("district report has already created hence skipping the process !!!!!!!!!!!!")
          }
        } else {
          println("targetedDistrict key is either not present or empty")
        }
        println(s"***************** End of Processing the Metabase Project Dashboard *****************\n")
    }
  }
}
