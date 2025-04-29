package org.shikshalokam.job.survey.dashboard.creator.functions

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.survey.dashboard.creator.domain.Event
import org.shikshalokam.job.survey.dashboard.creator.task.MetabaseDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import scala.collection.mutable
import org.slf4j.LoggerFactory

import scala.collection.immutable._

class SurveyMetabaseDashboardFunction(config: MetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[SurveyMetabaseDashboardFunction])

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

    val targetedProgramId = event.targetedProgram
    val admin = event.admin
    val solutions: String = config.solutions
    val projects: String = config.projects
    val tasks: String = config.tasks
    val metaDataTable = config.dashboard_metadata
    val report_config: String = config.report_config
    val metabaseDatabase: String = config.metabaseDatabase
    val surveyQuestionTable = s"${targetedProgramId}"
    println(s"surveyQuestionTable: $surveyQuestionTable")
    println(s"reportType: ${event.reportType}")
    // Printing the targetedState ID
    println(s"Targeted Program ID: $targetedProgramId")
    println(s"admin: $admin")
    println(s"reportType: ${event.reportType}")

    event.reportType match {
      //      case "Project" =>
      //        println(s">>>>>>>>>>> Started Processing Metabase Project Dashboards >>>>>>>>>>>>")
      //        if (admin.nonEmpty) {
      //          println(s"********** Started Processing Metabase Admin Dashboard ***********")
      //          val adminIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM $metaDataTable WHERE id = '$admin') THEN CASE WHEN COALESCE((SELECT status FROM $metaDataTable WHERE id = '$admin'), '') = 'Success' THEN 'success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
      //          val adminIdStatus = postgresUtil.fetchData(adminIdCheckQuery) match {
      //            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
      //            case _ => ""
      //          }
      //          if (adminIdStatus == "Failed") {
      //            try {
      //              val collectionName: String = s"Admin Collection"
      //              val dashboardName: String = s"Project Admin Report"
      //              val groupName: String = s"Report_Admin"
      //              val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$admin';"
      //              val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"Admin Report", metabaseUtil, postgresUtil, createDashboardQuery)
      //              val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
      //              val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
      //              val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "state_name", postgresUtil, createDashboardQuery)
      //              val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "district_name", postgresUtil, createDashboardQuery)
      //              val programnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, solutions, "program_name", postgresUtil, createDashboardQuery)
      //              val blocknameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "block_name", postgresUtil, createDashboardQuery)
      //              val clusternameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "cluster_name", postgresUtil, createDashboardQuery)
      //              val orgnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "org_name", postgresUtil, createDashboardQuery)
      //              val reportConfigQuery: String = s"SELECT question_type, config FROM $report_config WHERE dashboard_name = 'Admin';"
      //              val questionCardIdList = UpdateAdminJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, statenameId, districtnameId, programnameId, blocknameId, clusternameId, orgnameId, projects, solutions, metabaseUtil, postgresUtil)
      //              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
      //              val parametersQuery: String = s"SELECT config FROM $report_config WHERE report_name = 'Project-Parameter' AND question_type = 'admin-parameter'"
      //              UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
      //              val updateTableQuery = s"UPDATE $metaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success', error_message = '' WHERE entity_id = '$admin';"
      //              postgresUtil.insertData(updateTableQuery)
      //              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
      //            } catch {
      //              case e: Exception =>
      //                val updateTableQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE entity_id = '$admin';"
      //                postgresUtil.insertData(updateTableQuery)
      //                println(s"An error occurred: ${e.getMessage}")
      //                e.printStackTrace()
      //            }
      //            println(s"********** Completed Processing Metabase Admin Dashboard ***********")
      //          } else {
      //            println(s"Admin Dashboard has already created hence skipping the process !!!!!!!!!!!!")
      //          }
      //        } else {
      //          println(s"admin key is not present or is empty")
      //        }
      //
      //        if (targetedStateId.nonEmpty) {
      //          println(s"********** Started Processing Metabase State Dashboard ***********")
      //          val stateIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM $metaDataTable WHERE entity_id = '$targetedStateId') THEN CASE WHEN COALESCE((SELECT status FROM $metaDataTable WHERE entity_id = '$targetedStateId'), '') = 'Success' THEN 'Success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
      //          val stateIdStatus = postgresUtil.fetchData(stateIdCheckQuery) match {
      //            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
      //            case _ => ""
      //          }
      //          if (stateIdStatus == "Failed") {
      //            try {
      //              val stateNameQuery = s"SELECT entity_name from $metaDataTable where entity_id = '$targetedStateId'"
      //              val stateName = postgresUtil.fetchData(stateNameQuery) match {
      //                case List(map: Map[_, _]) => map.get("entity_name").map(_.toString).getOrElse("")
      //                case _ => ""
      //              }
      //              println(s"stateName = $stateName")
      //              val collectionName = s"State Collection [$stateName]"
      //              val dashboardName = s"Project State Report [$stateName]"
      //              val groupName: String = s"${stateName}_State_Manager"
      //              val metabaseDatabase: String = config.metabaseDatabase
      //              val parametersQuery: String = s"SELECT config FROM $report_config WHERE report_name = 'Project-Parameter' AND question_type = 'state-parameter'"
      //              println(s"parametersQuery = $parametersQuery")
      //              val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedStateId';"
      //              val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"State Report [$stateName]", metabaseUtil, postgresUtil, createDashboardQuery)
      //              val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
      //              val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
      //              val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "state_name", postgresUtil, createDashboardQuery)
      //              val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "district_name", postgresUtil, createDashboardQuery)
      //              val programnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, solutions, "program_name", postgresUtil, createDashboardQuery)
      //              val blocknameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "block_name", postgresUtil, createDashboardQuery)
      //              val clusternameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "cluster_name", postgresUtil, createDashboardQuery)
      //              val orgnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "org_name", postgresUtil, createDashboardQuery)
      //              val reportConfigQuery: String = s"SELECT question_type , config FROM $report_config WHERE dashboard_name = 'State';"
      //              val questionCardIdList = UpdateStateJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, statenameId, districtnameId, programnameId, blocknameId, clusternameId, orgnameId, projects, solutions, metabaseUtil, postgresUtil, targetedStateId)
      //              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
      //              val filterQuery: String = s"SELECT config FROM $report_config WHERE report_name = 'Project-Filters' AND question_type = 'state-filter'"
      //              val filterResults: List[Map[String, Any]] = postgresUtil.fetchData(filterQuery)
      //              val objectMapper = new ObjectMapper()
      //              val slugNameToStateIdFilterMap = mutable.Map[String, Int]()
      //              for (result <- filterResults) {
      //                val configString = result.get("config").map(_.toString).getOrElse("")
      //                val configJson = objectMapper.readTree(configString)
      //                val slugName = configJson.findValue("name").asText()
      //                val stateIdFilter: Int = UpdateAndAddStateFilter.updateAndAddFilter(metabaseUtil, configJson: JsonNode, s"$targetedStateId", collectionId, databaseId, projects, solutions)
      //                slugNameToStateIdFilterMap(slugName) = stateIdFilter
      //              }
      //              val immutableSlugNameToStateIdFilterMap: Map[String, Int] = slugNameToStateIdFilterMap.toMap
      //              UpdateParameters.updateParameterFunction(metabaseUtil, postgresUtil, parametersQuery, immutableSlugNameToStateIdFilterMap, dashboardId)
      //              val updateTableQuery = s"UPDATE $metaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success',error_message = '' WHERE entity_id = '$targetedStateId';"
      //              postgresUtil.insertData(updateTableQuery)
      //              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
      //            } catch {
      //              case e: Exception =>
      //                val updateTableQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE entity_id = '$targetedStateId';"
      //                postgresUtil.insertData(updateTableQuery)
      //                println(s"An error occurred: ${e.getMessage}")
      //                e.printStackTrace()
      //            }
      //            println(s"********** Completed Processing Metabase State Dashboard ***********")
      //          } else {
      //            println(s"state report has already created hence skipping the process !!!!!!!!!!!!")
      //          }
      //        } else {
      //          println("targetedState is not present or is empty")
      //        }
      //
      //        if (targetedProgramId.nonEmpty) {
      //          println(s"********** Started Processing Metabase Program Dashboard ***********")
      //          val programIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM $metaDataTable WHERE entity_id = '$targetedProgramId') THEN CASE WHEN COALESCE((SELECT status FROM $metaDataTable WHERE entity_id = '$targetedProgramId'), '') = 'Success' THEN 'Success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
      //          val programIdStatus = postgresUtil.fetchData(programIdCheckQuery) match {
      //            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
      //            case _ => ""
      //          }
      //          if (programIdStatus == "Failed") {
      //            try {
      //              val programNameQuery = s"SELECT entity_name from $metaDataTable where entity_id = '$targetedProgramId'"
      //              val programName = postgresUtil.fetchData(programNameQuery) match {
      //                case List(map: Map[_, _]) => map.get("entity_name").map(_.toString).getOrElse("")
      //                case _ => ""
      //              }
      //              println(s"programName = $programName")
      //              val collectionName = s"Program Collection [$programName]"
      //              val dashboardName = s"Project Program Report [$programName]"
      //              val groupName: String = s"Program_Manager[$programName]"
      //              val parametersQuery: String = s"SELECT config FROM $report_config WHERE report_name = 'Project-Parameter' AND question_type = 'program-parameter'"
      //              val metabaseDatabase: String = config.metabaseDatabase
      //              val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      //              val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"Program Report [$programName]", metabaseUtil, postgresUtil, createDashboardQuery)
      //              val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
      //              val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
      //              val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "state_name", postgresUtil, createDashboardQuery)
      //              val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "district_name", postgresUtil, createDashboardQuery)
      //              val programnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, solutions, "program_name", postgresUtil, createDashboardQuery)
      //              val blocknameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "block_name", postgresUtil, createDashboardQuery)
      //              val clusternameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "cluster_name", postgresUtil, createDashboardQuery)
      //              val orgnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "org_name", postgresUtil, createDashboardQuery)
      //              val reportConfigQuery: String = s"SELECT question_type , config FROM $report_config WHERE dashboard_name = 'Program';"
      //              val questionCardIdList = UpdateProgramJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, statenameId, districtnameId, programnameId, blocknameId, clusternameId, orgnameId, projects, solutions, tasks, metabaseUtil, postgresUtil, targetedProgramId)
      //              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
      //              val filterQuery: String = s"SELECT config FROM $report_config WHERE report_name = 'Project-Filters' AND question_type = 'program-filter'"
      //              val filterResults: List[Map[String, Any]] = postgresUtil.fetchData(filterQuery)
      //              val objectMapper = new ObjectMapper()
      //              val slugNameToProgramIdFilterMap = mutable.Map[String, Int]()
      //              for (result <- filterResults) {
      //                val configString = result.get("config").map(_.toString).getOrElse("")
      //                val configJson = objectMapper.readTree(configString)
      //                val slugName = configJson.findValue("name").asText()
      //                val programIdFilter: Int = UpdateAndAddProgramFilter.updateAndAddFilter(metabaseUtil, configJson, targetedProgramId, collectionId, databaseId, projects, solutions)
      //                slugNameToProgramIdFilterMap(slugName) = programIdFilter
      //              }
      //              val immutableSlugNameToProgramIdFilterMap: Map[String, Int] = slugNameToProgramIdFilterMap.toMap
      //              UpdateParameters.updateParameterFunction(metabaseUtil, postgresUtil, parametersQuery, immutableSlugNameToProgramIdFilterMap, dashboardId)
      //              val updateTableQuery = s"UPDATE $metaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success',error_message = '' WHERE entity_id = '$targetedProgramId';"
      //              postgresUtil.insertData(updateTableQuery)
      //              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
      //            } catch {
      //              case e: Exception =>
      //                val updateTableQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE entity_id = '$targetedProgramId';"
      //                postgresUtil.insertData(updateTableQuery)
      //                println(s"An error occurred: ${e.getMessage}")
      //                e.printStackTrace()
      //            }
      //            println(s"********** Completed Processing Metabase Program Dashboard ***********")
      //          } else {
      //            println("program report has already created hence skipping the process !!!!!!!!!!!!")
      //          }
      //        } else {
      //          println("targetedProgram key is either not present or empty")
      //        }
      //
      //        if (targetedDistrictId.nonEmpty) {
      //          println(s"********** Started Processing Metabase District Dashboard ***********")
      //          val districtIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM $metaDataTable WHERE entity_id = '$targetedDistrictId') THEN CASE WHEN COALESCE((SELECT status FROM $metaDataTable WHERE entity_id = '$targetedDistrictId'), '') = 'Success' THEN 'Success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
      //          val districtIdStatus = postgresUtil.fetchData(districtIdCheckQuery) match {
      //            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
      //            case _ => ""
      //          }
      //          if (districtIdStatus == "Failed") {
      //            try {
      //              val districtNameQuery = s"SELECT entity_name from $metaDataTable where entity_id = '$targetedDistrictId'"
      //              val districtname = postgresUtil.fetchData(districtNameQuery) match {
      //                case List(map: Map[_, _]) => map.get("entity_name").map(_.toString).getOrElse("")
      //                case _ => ""
      //              }
      //              println(s"districtname = $districtname")
      //              val statenamequery = s"SELECT distinct(state_name) AS name from $projects where district_id = '$targetedDistrictId'"
      //              val statename = postgresUtil.fetchData(statenamequery) match {
      //                case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
      //                case _ => ""
      //              }
      //              println(s"statename = $statename")
      //              val collectionName = s"District collection [$districtname - $statename]"
      //              val dashboardName = s"Project District Report [$districtname - $statename]"
      //              val groupName: String = s"${districtname}_District_Manager[$statename]"
      //              val metabaseDatabase: String = config.metabaseDatabase
      //              val parametersQuery: String = s"SELECT config FROM $report_config WHERE report_name = 'Project-Parameter' AND question_type = 'district-parameter'"
      //              val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedDistrictId';"
      //              val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"District Report [$districtname - $statename]", metabaseUtil, postgresUtil, createDashboardQuery)
      //              val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
      //              val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
      //              val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "state_name", postgresUtil, createDashboardQuery)
      //              val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "district_name", postgresUtil, createDashboardQuery)
      //              val programnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, solutions, "program_name", postgresUtil, createDashboardQuery)
      //              val blocknameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "block_name", postgresUtil, createDashboardQuery)
      //              val clusternameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "cluster_name", postgresUtil, createDashboardQuery)
      //              val orgnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, projects, "org_name", postgresUtil, createDashboardQuery)
      //              val reportConfigQuery: String = s"SELECT question_type , config FROM $report_config WHERE dashboard_name = 'District';"
      //              val questionCardIdList = UpdateDistrictJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, statenameId, districtnameId, programnameId, blocknameId, clusternameId, orgnameId, metabaseUtil, postgresUtil, projects, solutions, targetedStateId, targetedDistrictId)
      //              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
      //              val filterQuery: String = s"SELECT config FROM $report_config WHERE report_name = 'Project-Filters' AND question_type = 'district-filter'"
      //              val filterResults: List[Map[String, Any]] = postgresUtil.fetchData(filterQuery)
      //              val objectMapper = new ObjectMapper()
      //              val slugNameToDistrictIdFilterMap = mutable.Map[String, Int]()
      //              for (result <- filterResults) {
      //                val configString = result.get("config").map(_.toString).getOrElse("")
      //                val configJson = objectMapper.readTree(configString)
      //                val slugName = configJson.findValue("name").asText()
      //                val districtIdFilter: Int = UpdateAndAddDistrictFilter.updateAndAddFilter(metabaseUtil, configJson, targetedStateId, targetedDistrictId, collectionId, databaseId, projects, solutions)
      //                slugNameToDistrictIdFilterMap(slugName) = districtIdFilter
      //              }
      //              val immutableSlugNameToDistrictIdFilterMap: Map[String, Int] = slugNameToDistrictIdFilterMap.toMap
      //              UpdateParameters.updateParameterFunction(metabaseUtil, postgresUtil, parametersQuery, immutableSlugNameToDistrictIdFilterMap, dashboardId)
      //              val updateTableQuery = s"UPDATE $metaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success',error_message = '' WHERE entity_id = '$targetedDistrictId';"
      //              postgresUtil.insertData(updateTableQuery)
      //              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
      //            } catch {
      //              case e: Exception =>
      //                val updateTableQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE entity_id = '$targetedDistrictId';"
      //                postgresUtil.insertData(updateTableQuery)
      //                println(s"An error occurred: ${e.getMessage}")
      //                e.printStackTrace()
      //            }
      //            println(s"********** Completed Processing Metabase District Dashboard ***********")
      //          } else {
      //            println("district report has already created hence skipping the process !!!!!!!!!!!!")
      //          }
      //        } else {
      //          println("targetedDistrict key is either not present or empty")
      //        }
      //        println(s"***************** End of Processing the Metabase Project Dashboard *****************\n")
      case "Survey" =>
        println(s">>>>>>>>>>> Started Processing Metabase Project Dashboards >>>>>>>>>>>>")
        if (admin.nonEmpty) {
          println(s"********** Started Processing Metabase Admin Dashboard ***********")
          //TODO change the logic
          val adminIdCheckQuery: String = s"SELECT CASE WHEN EXISTS (SELECT 1 FROM $metaDataTable WHERE id = '$admin') THEN CASE WHEN COALESCE((SELECT status FROM $metaDataTable WHERE id = '$admin'), '') = 'Success' THEN 'success' ELSE 'Failed' END ELSE 'Failed' END AS result;"
          val adminIdStatus = postgresUtil.fetchData(adminIdCheckQuery) match {
            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
            case _ => ""
          }
          if (adminIdStatus == "Failed") {
            try {

              val solutionNameQuery = s"SELECT name from $solutions where solution_id = '$targetedProgramId'"
              val solutionName = postgresUtil.fetchData(solutionNameQuery) match {
                case List(map: Map[_, _]) => map.get("name").map(_.toString).getOrElse("")
                case _ => ""
              }
              println(solutionNameQuery)
              println(solutionName)
              //              val solutionName: String = programName
              val collectionName: String = s"Survey Question Collection[$solutionName]"
              val dashboardName: String = s"Survey Question Report[$solutionName]"
              val groupName: String = s"${solutionName}_Question_Program_Manager"
              val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
              val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"${solutionName}_question Report", metabaseUtil, postgresUtil, createDashboardQuery)
              val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
              val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
              metabaseUtil.syncDatabaseAndRescanValues(databaseId)
              val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "state_name", postgresUtil, createDashboardQuery)
              val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "district_name", postgresUtil, createDashboardQuery)
              val clusterId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "cluster_name", postgresUtil, createDashboardQuery)
              metabaseUtil.updateColumnCategory(statenameId, "State")
              metabaseUtil.updateColumnCategory(districtnameId, "City")
              val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "school_name", postgresUtil, createDashboardQuery)
              val questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, surveyQuestionTable, metabaseUtil, postgresUtil, report_config)
              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
              val parametersQuery: String = s"SELECT config FROM $report_config WHERE dashboard_name = 'Survey' AND question_type = 'Question-Parameter'"
              UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
              val updateTableQuery = s"UPDATE $metaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success', error_message = '' WHERE entity_id = '$targetedProgramId';"
              postgresUtil.insertData(updateTableQuery)
              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
            } catch {
              case e: Exception =>
                val updateTableQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE entity_id = '$targetedProgramId';"
                postgresUtil.insertData(updateTableQuery)
                println(s"An error occurred: ${e.getMessage}")
                e.printStackTrace()
            }

          }
        }
    }
  }
}