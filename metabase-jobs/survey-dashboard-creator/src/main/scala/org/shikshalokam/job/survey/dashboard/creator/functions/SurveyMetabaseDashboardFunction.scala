package org.shikshalokam.job.survey.dashboard.creator.functions

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.dashboard.creator.functions.UpdateStatusJsonFiles
import org.shikshalokam.job.survey.dashboard.creator.domain.Event
import org.shikshalokam.job.survey.dashboard.creator.task.SurveyMetabaseDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.collection.immutable._

class SurveyMetabaseDashboardFunction(config: SurveyMetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
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

    println(s"***************** Start of Processing the Survey Metabase Dashboard Event with Id = ${event._id}*****************")


    //TODO search success an d failed keyword
    val solutions: String = config.solutions
    val metaDataTable = config.dashboard_metadata
    val reportConfig: String = config.report_config
    val metabaseDatabase: String = config.metabaseDatabase
    val admin = event.admin
    val targetedSolutionId = event.targetedSolution
    val surveyQuestionTable = s"${targetedSolutionId}"
    val solutionName = postgresUtil.fetchData(s"""SELECT entity_name FROM $metaDataTable WHERE entity_id = '$targetedSolutionId'""").collectFirst { case map: Map[_, _] => map.getOrElse("entity_name", "").toString }.getOrElse("")
    val surveyStatusTable = s"""${targetedSolutionId}_survey_status"""
    val targetedProgramId: String = {
      val id = Option(event.targetedProgram).map(_.trim).getOrElse("")
      if (id.nonEmpty) id
      else {
        val query = s"SELECT program_id FROM $solutions WHERE solution_id = '$targetedSolutionId'"
        println(query)
        postgresUtil.fetchData(query) match {
          case List(map: Map[_, _]) => map.get("program_id").map(_.toString).getOrElse("")
          case _ => ""
        }
      }
    }
    val programName = postgresUtil.fetchData(s"""SELECT entity_name from $metaDataTable where entity_id = '$targetedProgramId'""").collectFirst { case map: Map[_, _] => map.get("entity_name").map(_.toString).getOrElse("") }.getOrElse("")
    println(s"surveyQuestionTable: $surveyQuestionTable")
    println(s"reportType: ${event.reportType}")
    println(s"admin: 1")
    println(s"Targeted Program ID: $targetedProgramId")
    println(s"Targeted Program Name: $programName")
    println(s"Targeted Solution ID: $targetedSolutionId")

    event.reportType match {
      case "Survey" =>
        /**
         * Logic to process and create Survey Admin Dashboard
         */
        println("~~~~~~~~ Start Survey Admin Dashboard Processing~~~~~~~~")
        val adminCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Admin Collection"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '1';"""
        val adminCollectionPresent = postgresUtil.fetchData(adminCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
        val adminCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Admin Collection' AND entity_id = '1';"
        val adminCollectionId = postgresUtil.executeQuery[Int](adminCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
        if (adminCollectionPresent == "Yes") {
          val surveyCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Survey Collection"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '1';"""
          val surveyCollectionPresent = postgresUtil.fetchData(surveyCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
          val surveyCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Survey Collection' AND entity_id = '1';"
          val surveyCollectionId = postgresUtil.executeQuery[Int](surveyCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
          if (surveyCollectionPresent == "Yes") {
            println("=====> Admin and Survey collection present creating Solution collection & dashboard")
            val solutionCollectionName: String = s"$solutionName [Admin]"
            val parentCollectionId = createSurveyQuestionDashboard(surveyCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable)
            createSurveyStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable)
          } else {
            println("=====> Only Admin collection is present creating Survey Collection then Solution collection & dashboard")
            val surveyCollectionId = createSurveyCollectionInsideAdmin(adminCollectionId)
            val solutionCollectionName: String = s"$solutionName [Admin]"
            val parentCollectionId = createSurveyQuestionDashboard(surveyCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable)
            createSurveyStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable)
          }
        } else {
          println("=====> Admin collection is not present creating Admin, Survey Collection then Solution collection & dashboard")
          val adminCollectionId = createAdminCollection
          val surveyCollectionId = createSurveyCollectionInsideAdmin(adminCollectionId)
          val solutionCollectionName: String = s"$solutionName [Admin]"
          val parentCollectionId = createSurveyQuestionDashboard(surveyCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable)
          createSurveyStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable)
        }
        println("~~~~~~~~ End Survey Admin Dashboard Processing~~~~~~~~")

        def createAdminCollection: Int = {
          val (adminCollectionName, adminCollectionDescription) = ("Admin Collection", "Admin Collection which contains all the sub-collections and questions")
          val groupName: String = s"Report_Admin"
          val adminCollectionId: Int = Utils.checkAndCreateCollection(adminCollectionName, adminCollectionDescription, metabaseUtil)
          val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", adminCollectionId).put("collectionName", adminCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb WHERE entity_id = '1';")
          CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, adminCollectionId)
          adminCollectionId
        }

        def createProgramCollection(programName: String): Int = {
          val (programCollectionName, programCollectionDescription) = (s"Program Collection [$programName]", "Program Collection which contains all the sub-collections and questions")
          val groupName: String = s"Program_Manager[$programName]"
          val programCollectionId: Int = Utils.checkAndCreateCollection(programCollectionName, programCollectionDescription, metabaseUtil)
          val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJson' ::jsonb WHERE entity_id = '$targetedProgramId';")
          CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, programCollectionId)
          programCollectionId
        }

        def createSurveyCollectionInsideAdmin(adminCollectionId: Int): Int = {
          val (surveyCollectionName, surveyCollectionDescription) = ("Survey Collection", "This collection contains sub-collection, questions and dashboards required for Survey")
          val surveyCollectionId = Utils.createCollection(surveyCollectionName, surveyCollectionDescription, metabaseUtil, Some(adminCollectionId))
          val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", surveyCollectionId).put("collectionName", surveyCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '1';")
          surveyCollectionId
        }

        def createSurveyCollectionInsideProgram(adminCollectionId: Int): Int = {
          val (surveyCollectionName, surveyCollectionDescription) = ("Survey Collection", "This collection contains sub-collection, questions and dashboards required for Survey")
          val surveyCollectionId = Utils.createCollection(surveyCollectionName, surveyCollectionDescription, metabaseUtil, Some(adminCollectionId))
          val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", surveyCollectionId).put("collectionName", surveyCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedProgramId';")
          surveyCollectionId
        }

        /**
         * Logic to process and create Program Dashboard for Survey
         */
        println("~~~~~~~~ Start Survey Program Dashboard Processing~~~~~~~~")
        val programCollectionCheckQuery = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Program Collection [$programName]"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId'"""
        val programCollectionPresent = postgresUtil.fetchData(programCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
        val programCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Program Collection [$programName]' AND entity_id = '$targetedProgramId';"
        val programCollectionId = postgresUtil.executeQuery[Int](programCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
        if (programCollectionPresent == "Yes") {
          val surveyCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Survey Collection"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId';"""
          val surveyCollectionPresent = postgresUtil.fetchData(surveyCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
          val surveyCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Survey Collection' AND entity_id = '$targetedProgramId';"
          val surveyCollectionId = postgresUtil.executeQuery[Int](surveyCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
          if (surveyCollectionPresent == "Yes") {
            println("=====> Program and Survey collection present creating Solution collection & dashboard")
            val solutionCollectionName: String = s"$solutionName [Program]"
            val parentCollectionId = createSurveyQuestionDashboard(surveyCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable)
            createSurveyStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable)
          } else {
            println("=====> Only Program collection is present creating Survey Collection then Solution collection & dashboard")
            val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId)
            val solutionCollectionName: String = s"$solutionName [Program]"
            val parentCollectionId = createSurveyQuestionDashboard(surveyCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable)
            createSurveyStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable)
          }
        } else {
          println("=====> Program collection is not present creating Program, Survey Collection then Solution collection & dashboard")
          val programCollectionId = createProgramCollection(programName)
          val surveyCollectionId = createSurveyCollectionInsideAdmin(programCollectionId)
          val solutionCollectionName: String = s"$solutionName [Program]"
          val parentCollectionId = createSurveyQuestionDashboard(surveyCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable)
          createSurveyStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable)
        }
        println("~~~~~~~~ End Survey Program Dashboard Processing~~~~~~~~")

        println(s"***************** Processing Completed for Survey Metabase Dashboard Event with Id = ${event._id}*****************\n\n")
    }
  }

  /**
   * Logic for Survey Question Dashboard
   */
  def createSurveyQuestionDashboard(parentCollectionId: Int, collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyQuestionTable: String): Int = {
    try {
      val dashboardName: String = s"Survey Question Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val collectionId: Int = Utils.checkAndCreateCollection(collectionName, s"Solution Collection which contains all the dashboards and questions", metabaseUtil, Some(parentCollectionId))
      val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil)
      val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
      metabaseUtil.syncDatabaseAndRescanValues(databaseId)
      val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "state_name", postgresUtil, createDashboardQuery)
      val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "district_name", postgresUtil, createDashboardQuery)
      val clusterId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "cluster_name", postgresUtil, createDashboardQuery)
      metabaseUtil.updateColumnCategory(statenameId, "State")
      metabaseUtil.updateColumnCategory(districtnameId, "City")
      val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "school_name", postgresUtil, createDashboardQuery)
      val questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, surveyQuestionTable, metabaseUtil, postgresUtil, reportConfig)
      val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
      val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Survey' AND question_type = 'Question-Parameter'"
      UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
      val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString))
      postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
      collectionId
    }
    catch {
      case e: Exception =>
        postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
        -1
    }
  }

  def createSurveyStatusDashboard(parentCollectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyStatusTable: String): Unit = {
    try {
      val dashboardName: String = s"Survey Status Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val dashboardId: Int = Utils.createDashboard(parentCollectionId, dashboardName, metabaseUtil, postgresUtil)
      val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
      metabaseUtil.syncDatabaseAndRescanValues(databaseId)
      val statenNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyStatusTable, "state_name", postgresUtil, createDashboardQuery)
      val districtNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyStatusTable, "district_name", postgresUtil, createDashboardQuery)
      val blockNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyStatusTable, "block_name", postgresUtil, createDashboardQuery)
      val clusterNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyStatusTable, "cluster_name", postgresUtil, createDashboardQuery)
      val organisationNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyStatusTable, "organisation_name", postgresUtil, createDashboardQuery)
      metabaseUtil.updateColumnCategory(statenNameId, "State")
      metabaseUtil.updateColumnCategory(districtNameId, "City")
      val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Status-Report' AND question_type IN ('big-number', 'table');"
      val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, statenNameId, districtNameId, blockNameId, clusterNameId, organisationNameId, surveyStatusTable, metabaseUtil, postgresUtil)
      val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
      val parametersQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Status-Report' AND question_type = 'Status-Parameter';"
      UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
      val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString))
      postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
    }
    catch {
      case e: Exception =>
        postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    }
  }

}