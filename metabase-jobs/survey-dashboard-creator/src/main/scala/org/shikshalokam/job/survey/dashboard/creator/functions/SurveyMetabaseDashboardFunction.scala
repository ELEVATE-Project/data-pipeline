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

    println(s"***************** Start of Processing the Metabase Dashboard Event with Id = ${event._id}*****************")

    val admin = event.admin
    val targetedProgramId = event.targetedProgram
    val targetedSolutionId = event.targetedSolution
    val solutions: String = config.solutions
    val metaDataTable = config.dashboard_metadata
    val reportConfig: String = config.report_config
    val metabaseDatabase: String = config.metabaseDatabase
    val surveyQuestionTable = s"${targetedSolutionId}"
    val surveyStatusTable = s"""${targetedSolutionId}_survey_status"""
    val programNameQuery = s"SELECT entity_name from $metaDataTable where entity_id = '$targetedProgramId'"
    val programName = postgresUtil.fetchData(programNameQuery) match {
      case List(map: Map[_, _]) => map.get("entity_name").map(_.toString).getOrElse("")
      case _ => ""
    }
    println(s"surveyQuestionTable: $surveyQuestionTable")
    println(s"reportType: ${event.reportType}")
    println(s"admin: $admin")
    println(s"Targeted Program ID: $targetedProgramId")
    println(s"Targeted Program Name: $programName")
    println(s"Targeted Solution ID: $targetedSolutionId")

    event.reportType match {
      case "Survey" =>
        println(s">>>>>>>>>>> Started Processing Metabase Project Dashboards >>>>>>>>>>>>")

        /**
         * Logic to process and create Survey Admin Dashboard
         */
        if (admin.nonEmpty) {
          println(s"********** Started Processing Metabase Admin Dashboard For Survey ***********")
          val adminSurveyIdCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Admin Collection"}]'::jsonb AND main_metadata::jsonb @> '[{"collectionName":"Survey Collection"}]'::jsonb THEN 'Success' ELSE 'Failed' END AS result FROM $metaDataTable WHERE entity_id = '$admin';"""
          val adminSurveyIdStatus = postgresUtil.fetchData(adminSurveyIdCheckQuery) match {
            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
            case _ => ""
          }
          if (adminSurveyIdStatus == "Failed") {
            val adminIdCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Admin Collection"}]'::jsonb THEN 'Success' ELSE 'Failed' END AS result FROM $metaDataTable WHERE entity_id = '$admin';"""
            val adminIdStatus = postgresUtil.fetchData(adminIdCheckQuery) match {
              case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
              case _ => ""
            }
            if (adminIdStatus == "Failed") {
              try {
                val (adminCollectionName, adminCollectionDescription) = ("Admin Collection", "Admin Report")
                val groupName: String = s"Report_Admin"
                val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$admin';"
                val adminCollectionId: Int = CreateDashboard.checkAndCreateCollection(adminCollectionName, adminCollectionDescription, metabaseUtil, postgresUtil, createDashboardQuery)
                val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", adminCollectionId).put("collectionName", adminCollectionName))
                postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb WHERE entity_id = '$admin';")
                val (surveyCollectionName, surveyCollectionDescription) = ("Survey Collection", "This collection contains sub-collection, questions and dashboards required for Survey")
                val surveyCollectionId = Utils.createCollection(surveyCollectionName, surveyCollectionDescription, metabaseUtil, Some(adminCollectionId))
                val parentCollectionId = createSurveyQuestionDashboard(surveyCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable)
                createSurveyStatusDashboard(parentCollectionId, solutions, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable)
                val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", surveyCollectionId).put("collectionName", surveyCollectionName))
                postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$admin';")
                CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, adminCollectionId)
              }
              catch {
                case e: Exception =>
                  postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$admin';")
                  println(s"An error occurred: ${e.getMessage}")
                  e.printStackTrace()
              }

            } else {
              try {
                val adminCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Admin Collection' AND entity_name = 'Admin';"
                val adminCollectionId = postgresUtil.executeQuery[Int](adminCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
                val (surveyCollectionName, surveyCollectionDescription) = ("Survey Collection", "This collection contains sub-collection, questions and dashboards required for Survey")
                val surveyCollectionId = Utils.createCollection(surveyCollectionName, surveyCollectionDescription, metabaseUtil, Some(adminCollectionId))
                val parentCollectionId = createSurveyQuestionDashboard(surveyCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable)
                createSurveyStatusDashboard(parentCollectionId, solutions, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable)
                val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", surveyCollectionId).put("collectionName", surveyCollectionName))
                postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$admin';")

              }
              catch {
                case e: Exception =>
                  postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$admin';")
                  println(s"An error occurred: ${e.getMessage}")
                  e.printStackTrace()
              }
            }
          } else {
            println(s"Admin & Survey Collections has already been created hence skipping the process !!!!!!!!!!!!")
          }
        } else {
          println(s"admin key is not present or is empty")
        }

        /**
         * Logic to process and create Program Dashboard
         */
        if (targetedProgramId.nonEmpty) {
          println(s"********** Started Processing Metabase Program Dashboard For Survey ***********")
          val programIdCheckQuery =
            s"""SELECT CASE WHEN
                    EXISTS (SELECT 1 FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS e WHERE entity_id = '$targetedProgramId' AND e ->> 'collectionName' = ('Program Collection [' || entity_name || ']'))
                    AND
                    EXISTS (SELECT 1 FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS e WHERE entity_id = '$targetedProgramId' AND e ->> 'collectionName' = 'Survey Collection')
                    THEN 'Success' ELSE 'Failed' END AS result""".stripMargin.replaceAll("\n", " ")
          val programSurveyIdStatus = postgresUtil.fetchData(programIdCheckQuery) match {
            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
            case _ => ""
          }
          if (programSurveyIdStatus == "Failed") {
            val programIdCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Program Collection [$programName]"}]'::jsonb THEN 'Success' ELSE 'Failed' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId';"""
            val programIdStatus = postgresUtil.fetchData(programIdCheckQuery) match {
              case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
              case _ => ""
            }
            if (programIdStatus == "Failed") {
              try {
                val (programCollectionName, programCollectionDescription) = (s"Program Collection [$programName]", s"Program Report [$programName]")
                val groupName: String = s"Program_Manager[$programName]"
                val programCollectionId: Int = Utils.checkAndCreateCollection(programCollectionName, programCollectionDescription, metabaseUtil)
                val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName))
                postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJson' ::jsonb WHERE entity_id = '$targetedProgramId';")
                val (surveyCollectionName, surveyCollectionDescription) = ("Survey Collection", "This collection contains sub-collection, questions and dashboards required for Survey")
                val surveyCollectionId = Utils.createCollection(surveyCollectionName, surveyCollectionDescription, metabaseUtil, Some(programCollectionId))
                val parentCollectionId = createSurveyQuestionDashboard(surveyCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable)
                createSurveyStatusDashboard(parentCollectionId, solutions, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable)
                val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", surveyCollectionId).put("collectionName", surveyCollectionName))
                postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedProgramId';")
                CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, programCollectionId)
              }
              catch {
                case e: Exception =>
                  postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedProgramId';")
                  println(s"An error occurred: ${e.getMessage}")
                  e.printStackTrace()
              }

            } else {
              try {
                val programCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Program Collection [$programName]' AND entity_id = '$targetedProgramId';"
                val programCollectionId = postgresUtil.executeQuery[Int](programCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
                val (surveyCollectionName, surveyCollectionDescription) = ("Survey Collection", "This collection contains sub-collection, questions and dashboards required for Survey")
                val surveyCollectionId = Utils.createCollection(surveyCollectionName, surveyCollectionDescription, metabaseUtil, Some(programCollectionId))
                val parentCollectionId = createSurveyQuestionDashboard(surveyCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable)
                createSurveyStatusDashboard(parentCollectionId, solutions, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable)
                val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", surveyCollectionId).put("collectionName", surveyCollectionName))
                postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedProgramId';")
              }
              catch {
                case e: Exception =>
                  postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedProgramId';")
                  println(s"An error occurred: ${e.getMessage}")
                  e.printStackTrace()
              }
            }
          } else {
            println(s"Program & Survey Collections has already been created hence skipping the process !!!!!!!!!!!!")
          }
        } else {
          println(s"program key is not present or is empty")
        }
    }
  }

  /**
   * Logic for Survey Question Dashboard
   */
  def createSurveyQuestionDashboard(parentCollectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyQuestionTable: String): Int = {
    try {
      val solutionNameQuery = s"""SELECT entity_name from $metaDataTable where entity_id = '$targetedSolutionId' """
      val solutionName = postgresUtil.fetchData(solutionNameQuery) match {
        case List(map: Map[_, _]) => map.get("entity_name").map(_.toString).getOrElse("")
        case _ => ""
      }
      val collectionName: String = s"Survey [$solutionName]"
      val dashboardName: String = s"Survey Question Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val collectionId: Int = Utils.createCollection(collectionName, s"${solutionName} - Question Report", metabaseUtil, Some(parentCollectionId))
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

  def createSurveyStatusDashboard(parentCollectionId: Int, solutions: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyStatusTable: String): Unit = {
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