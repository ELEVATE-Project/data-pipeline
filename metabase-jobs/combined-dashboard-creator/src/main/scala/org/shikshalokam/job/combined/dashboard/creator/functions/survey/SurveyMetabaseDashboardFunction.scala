package org.shikshalokam.job.combined.dashboard.creator.functions.survey

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.collection.JavaConverters._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.combined.dashboard.creator.domain.SurveyEvent
import org.shikshalokam.job.combined.dashboard.creator.task.CombinedDashboardCreatorConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.collection.immutable._

class SurveyMetabaseDashboardFunction(config: CombinedDashboardCreatorConfig)(implicit val mapTypeInfo: TypeInformation[SurveyEvent], @transient var postgresUtil: PostgresUtil = null, @transient var metabasePostgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[SurveyEvent, SurveyEvent](config) {

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
    val metabasePgDb: String = config.metabasePgDatabase
    val connectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$pgDataBase"
    val metabaseConnectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$metabasePgDb"
    postgresUtil = new PostgresUtil(connectionUrl, pgUsername, pgPassword)
    metabasePostgresUtil = new PostgresUtil(metabaseConnectionUrl, pgUsername, pgPassword)
    metabaseUtil = new MetabaseUtil(metabaseUrl, metabaseUsername, metabasePassword, metabasePostgresUtil, postgresUtil)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: SurveyEvent, context: ProcessFunction[SurveyEvent, SurveyEvent]#Context, metrics: Metrics): Unit = {

    try {
      logger.info(s"***************** Start of Processing the Metabase Survey Dashboard Event with Id = ${event._id}*****************")

      val startTime = System.currentTimeMillis()
      val solutions: String = config.solutions
      val evidenceBaseUrl: String = config.evidenceBaseUrl
      val metaDataTable = config.dashboard_metadata
      val reportConfig: String = config.report_config
      val metabaseApiKey: String = config.metabaseApiKey
      val metabaseDatabase: String = config.metabaseDatabase
      val filterSync: String = event.filterSync
      val filterTable: String = event.filterTable
      val targetedSolutionId = event.targetedSolution
      if (targetedSolutionId.nonEmpty) {
        val databaseId = metabaseUtil.getDatabaseID(metabaseDatabase); if (databaseId == -1) { println(s"[ERROR] Metabase database '$metabaseDatabase' not found"); return }
        val surveyQuestionTable = s"${targetedSolutionId}"
        val dashboardDescription = s"Analytical overview of the data for solutionId $targetedSolutionId"
        val solutionName = postgresUtil.fetchData(s"""SELECT entity_name FROM $metaDataTable WHERE entity_id = '$targetedSolutionId'""").collectFirst { case map: Map[_, _] => map.getOrElse("entity_name", "").toString }.getOrElse("")
        val surveyStatusTable = s"""${targetedSolutionId}_survey_status"""
        val targetedProgramId: String = {
          val id = Option(event.targetedProgram).map(_.trim).getOrElse("")
          if (id.nonEmpty) id
          else {
            val query = s"SELECT program_id FROM $solutions WHERE solution_id = '$targetedSolutionId'"
            logger.info(query)
            postgresUtil.fetchData(query) match {
              case List(map: Map[_, _]) => Option(map.get("program_id")).flatten.map(_.toString).getOrElse("")
              case _ => ""
            }
          }
        }

        val query = s"SELECT external_id, program_external_id, program_description, description FROM $solutions WHERE solution_id = '$targetedSolutionId'"
        val resultMap = postgresUtil.fetchData(query).collectFirst { case map: Map[_, _] => map }.getOrElse(Map.empty)

        val solutionExternalId = resultMap.get("external_id").map(_.toString).getOrElse("")
        val programExternalId = resultMap.get("program_external_id").map(_.toString).getOrElse("")
        val programDescription = resultMap.get("program_description").map(_.toString).getOrElse("")
        val solutionDescription = resultMap.get("description").map(_.toString).getOrElse("")

        val programName = postgresUtil.fetchData(s"""SELECT entity_name from $metaDataTable where entity_id = '$targetedProgramId'""").collectFirst { case map: Map[_, _] => map.get("entity_name").map(_.toString).getOrElse("") }.getOrElse("")

        val orgId = postgresUtil.fetchData(s"""SELECT org_id FROM $solutions WHERE program_id = '$targetedProgramId' AND org_id IS NOT NULL AND TRIM(org_id) <> '' LIMIT 1 """).collectFirst { case map: Map[_, _] => map.getOrElse("org_id", "").toString }.getOrElse("")
        val programCollectionName = s"$programName"
        val solutionCollectionName = s"$solutionName [Survey]"
        val tabList: List[String] = List("Status Report", "Question Report", "Status CSV", "Question CSV")

        logger.info(s"surveyQuestionTable: $surveyQuestionTable")
        logger.info(s"reportType: ${event.reportType}")
        logger.info(s"admin: 1")
        logger.info(s"Targeted Program ID: $targetedProgramId")
        logger.info(s"Targeted Program Name: $programName")
        logger.info(s"Targeted Solution ID: $targetedSolutionId")


        if (solutionName.nonEmpty && targetedProgramId.nonEmpty && programName.nonEmpty) {
          event.reportType match {
            case "Survey" =>

              /**
               * Logic to process and create Survey Admin Dashboard
               */
              logger.info("\n=>> Logic to process and create Survey Admin Dashboard")
              val (adminCollectionPresent, adminCollectionId) = metabaseUtil.validateCollection(s"Programs", "Admin")
              if (adminCollectionPresent && adminCollectionId != 0) {
                val (programCollectionPresent, programCollectionId) = metabaseUtil.validateCollection(programCollectionName, "Admin", Some(targetedProgramId), Some("Program"))
                if (programCollectionPresent && programCollectionId != 0) {
                  val (solutionCollectionPresent, solutionCollectionId) = metabaseUtil.validateCollection(solutionCollectionName, "Admin", Some(targetedSolutionId), Some("Solution"))
                  if (solutionCollectionPresent && solutionCollectionId != 0) {
                    logger.info(s"=====> $solutionCollectionName collection is present, hence skipping the process ......")
                  } else {
                    val solutionCollectionId = createSurveyCollectionInsideProgram(programCollectionId, targetedSolutionId, solutionExternalId, s"$solutionName [Survey]", solutionDescription, "Admin")
                    val dashboardId: Int = Utils.createDashboard(solutionCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
                    val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                    createAdminDashboard(solutionCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Admin")
                    createSurveyCsvDashboard(solutionCollectionId, databaseId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Admin", metabaseApiKey)
                  }
                } else {
                  logger.info(s"=====> $programCollectionName collection is not present, creating $programCollectionName collection for Admin ......")
                  val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId, targetedProgramId, programExternalId, programCollectionName, programDescription, "Admin")
                  val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId, targetedSolutionId, solutionExternalId, s"$solutionName [Survey]", solutionDescription, "Admin")
                  val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
                  val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                  createAdminDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Admin")
                  createSurveyCsvDashboard(surveyCollectionId, databaseId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Admin", metabaseApiKey)
                }
              } else {
                logger.info(s"=====> Programs Collection is not present, creating Programs Collection ......")
                val adminCollectionId = createAdminCollection
                val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId, targetedProgramId, programExternalId, programCollectionName, programDescription, "Admin")
                val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId, targetedSolutionId, solutionExternalId, s"$solutionName [Survey]", solutionDescription, "Admin")
                val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
                val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                createAdminDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Admin")
                createSurveyCsvDashboard(surveyCollectionId, databaseId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Admin", metabaseApiKey)
              }

              /**
               * Logic to process and create Program Dashboard for Survey
               */
              logger.info("=>> Logic to process and create Program Dashboard for Survey")
              val (programCollectionPresent, programCollectionId) = metabaseUtil.validateCollection(programCollectionName, "Program Manager", Some(targetedProgramId), Some("Program"))
              if (programCollectionPresent && programCollectionId != 0) {
                logger.info(s"=====> $programCollectionName collection is present hence skipping the process ......")
                val (solutionCollectionPresent, solutionCollectionId) = metabaseUtil.validateCollection(solutionCollectionName, "Program Manager", Some(targetedSolutionId), Some("Solution"))
                if (solutionCollectionPresent && solutionCollectionId != 0) {
                  logger.info(s"=====> $solutionCollectionName collection is present, hence skipping the process ......")
                } else {
                  logger.info(s"=====> $solutionCollectionName collection is not present, creating $solutionCollectionName collection for Program Manager ......")
                  val solutionCollectionId = createSurveyCollectionInsideProgram(programCollectionId, targetedSolutionId, solutionExternalId, solutionCollectionName, solutionDescription, "Program Manager")
                  val dashboardId: Int = Utils.createDashboard(solutionCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
                  val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                  createSurveyCsvDashboard(solutionCollectionId, databaseId, dashboardId, tabIdMap, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Program", metabaseApiKey)
                  createProgramDashboard(solutionCollectionId, dashboardId, tabIdMap, solutionCollectionName, "Program")
                }
              } else {
                logger.info(s"=====> $programCollectionName collection is not present, creating $programCollectionName collection for Program Manager ......")
                val programCollectionId = createProgramCollection(programCollectionName, targetedProgramId, programExternalId, programDescription, "Program Manager")
                val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId, targetedSolutionId, solutionExternalId, solutionCollectionName, solutionDescription, "Program Manager")
                val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
                val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                createSurveyCsvDashboard(surveyCollectionId, databaseId, dashboardId, tabIdMap, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Program", metabaseApiKey)
                createProgramDashboard(surveyCollectionId, dashboardId, tabIdMap, solutionCollectionName, "Program")
              }
          }
          logger.info(s"***************** Processing Completed for Survey Metabase Dashboard Event with Id = ${event._id}*****************\n\n")
        }

        if (filterSync.nonEmpty){
          val filterTableId: Int = metabaseUtil.searchTable(filterTable, databaseId)
          if (filterTableId != -1) {
            metabaseUtil.discardValues(filterTableId)
            metabaseUtil.rescanValues(filterTableId)
          } else {
            logger.info(s"Table does not exits in the metabase DB")
          }
          logger.info("Successfully updated the filters values")
        }

        def createAdminDashboard(solutionCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], solutionCollectionName: String, category: String): Unit = {
          val parentCollectionId = createSurveyQuestionDashboard(solutionCollectionId, databaseId, dashboardId, tabIdMap, solutionCollectionName, metaDataTable, reportConfig, targetedProgramId, targetedSolutionId, surveyQuestionTable, evidenceBaseUrl, category, metabaseApiKey)
          createSurveyStatusDashboard(parentCollectionId, databaseId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable, category, metabaseApiKey)
        }

        def createProgramDashboard(solutionCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], solutionCollectionName: String, category: String): Unit = {
          val parentCollectionId = createSurveyQuestionDashboard(solutionCollectionId, databaseId, dashboardId, tabIdMap, solutionCollectionName, metaDataTable, reportConfig, targetedProgramId, targetedSolutionId, surveyQuestionTable, evidenceBaseUrl, category, metabaseApiKey)
          createSurveyStatusDashboard(parentCollectionId, databaseId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable, category, metabaseApiKey)
        }

        def createAdminCollection: Int = {
          val (adminCollectionName, adminCollectionDescription) = ("Programs", s"All programs made available on the platform are stored in this collection.\n\nCollection For: Admin")
          val adminCollectionId: Int = Utils.createCollection(adminCollectionName, adminCollectionDescription, metabaseUtil)
          val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", adminCollectionId).put("collectionName", adminCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb, status = 'Success' WHERE entity_id = '1';")
          Utils.createGroupToDashboard(metabaseUtil, "Report_Admin_Programs", adminCollectionId)
          adminCollectionId
        }

        def createProgramCollection(programCollectionName: String, targetedProgramId: String, programExternalId: String, programDescription: String, reportFor: String): Int = {
          val programCollectionDescription = s"Program Id: $targetedProgramId\n\nProgram External Id: $programExternalId\n\nCreator Organisation: $orgId\n\nCollection For: $reportFor\n\nProgram Description: $programDescription"
          val programCollectionId: Int = Utils.createCollection(programCollectionName, programCollectionDescription, metabaseUtil)
          if (programCollectionId != -1) {
            val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName).put("Collection For", reportFor))
            val programMetadataJsonString = programMetadataJson.toString.replace("'", "''")
            postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJsonString' ::jsonb, status = 'Success' WHERE entity_id = '$targetedProgramId';")
            Utils.createGroupToDashboard(metabaseUtil, s"Program_Manager_$targetedProgramId", programCollectionId)
            programCollectionId
          } else {
            logger.info(s"$programName [$targetedProgramId] returned -1")
            -1
          }
        }

        def createProgramCollectionInsideAdmin(adminCollectionId: Int, targetedProgramId: String, programExternalId: String, programCollectionName: String, programDescription: String, reportFor: String): Int = {
          val programCollectionDescription = s"Program Id: $targetedProgramId\n\nExternal Id: $programExternalId\n\nCreator Organisation: $orgId\n\nCollection For: $reportFor\n\nProgram Description: $programDescription"
          val programCollectionId = Utils.createCollection(programCollectionName, programCollectionDescription, metabaseUtil, Some(adminCollectionId))
          val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName).put("Collection For", reportFor))
          val programMetadataJsonString = programMetadataJson.toString.replace("'", "''")
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJsonString' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedProgramId';")
          programCollectionId
        }

        def createSurveyCollectionInsideProgram(programCollectionId: Int, targetedSolutionId: String, solutionExternalId: String, surveyCollectionName: String, surveyDescription: String, reportFor: String): Int = {
          val solutionCollectionDescription = s"Solution Id: $targetedSolutionId\n\nSolution External Id: $solutionExternalId\n\nCollection For: $reportFor\n\nSolution Description: $surveyDescription"
          val surveyCollectionId = Utils.createCollection(surveyCollectionName, solutionCollectionDescription, metabaseUtil, Some(programCollectionId))
          val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", surveyCollectionId).put("collectionName", surveyCollectionName).put("Collection For", reportFor))
          val surveyMetadataJsonString = surveyMetadataJson.toString.replace("'", "''")
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJsonString' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
          surveyCollectionId
        }

        /**
         * Logic for Survey Question Dashboard
         */
        def createSurveyQuestionDashboard(parentCollectionId: Int, databaseId: Int, dashboardId: Int, tabIdMap: Map[String, Int], collectionName: String, metaDataTable: String, reportConfig: String, targetedProgramId: String, targetedSolutionId: String, surveyQuestionTable: String, evidenceBaseUrl: String, reportFor: String, metabaseApiKey: String): Int = {
          try {
            val dashboardName: String = s"Question Report"
            val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
            if (parentCollectionId != -1) {
              val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
              val stateNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyQuestionTable, "state_name", metabaseApiKey, createDashboardQuery)
              val districtNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyQuestionTable, "district_name", metabaseApiKey, createDashboardQuery)
              val blockNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyQuestionTable, "block_name", metabaseApiKey, createDashboardQuery)
              val clusterNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyQuestionTable, "cluster_name", metabaseApiKey, createDashboardQuery)
              val schoolNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyQuestionTable, "school_name", metabaseApiKey, createDashboardQuery)
              val orgNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyQuestionTable, "organisation_name", metabaseApiKey, createDashboardQuery)
              metabaseUtil.updateColumnCategory(stateNameId, "State")
              metabaseUtil.updateColumnCategory(districtNameId, "City")
              val questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(parentCollectionId, databaseId, dashboardId, tabId, stateNameId, districtNameId, blockNameId: Int, clusterNameId, schoolNameId, orgNameId, surveyQuestionTable, metabaseUtil, postgresUtil, reportConfig, evidenceBaseUrl)
              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
              val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Survey' AND question_type = 'Question-Parameter'"
              UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
              val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("Collection For", reportFor))
              val surveyMetadataJsonString = surveyMetadataJson.toString.replace("'", "''")
              postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJsonString' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
              parentCollectionId
            } else {
              logger.info("Solution Collection Id returned -1")
              -1
            }
          }
          catch {
            case e: Exception =>
              val escapedMsg = Option(e.getMessage).getOrElse("Unknown error").replace("'", "''")
              postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '$escapedMsg' WHERE entity_id = '$targetedSolutionId';")
              -1
          }
        }

        def createSurveyCsvDashboard(parentCollectionId: Int, databaseId: Int, dashboardId: Int, tabIdMap: Map[String, Int], collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyQuestionTable: String, surveyStatusTable: String, evidenceBaseUrl: String, reportFor: String, metabaseApiKey: String): Unit = {
          try {
            val questionDashboardName: String = s"Question CSV"
            val questionReportConfigQuery: String = s"SELECT * FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Question-Report' AND question_type = 'table';"
            val questionParametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Question-Report' AND question_type = 'Question-Parameter'"
            val questionReplacements: Map[String, String] = Map(
              "${questionTable}" -> s""""$surveyQuestionTable"""",
              "${evidenceBaseUrl}" -> s"""'$evidenceBaseUrl'"""
            )
            val statusDashboardName: String = s"Status CSV"
            val statusReportConfigQuery: String = s"SELECT * FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Status-Report' AND question_type = 'table';"
            val statusParametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Status-Report' AND question_type = 'Status-Parameter';"
            val statusReplacements: Map[String, String] = Map(
              "${statusTable}" -> s""""${surveyStatusTable}"""",
            )
            val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"

            def commonStepsToCreateCsvDashboard(reportConfigQuery: String, databaseId: Int, dashboardName: String, dashboardId: Int, tabIdMap: Map[String, Int], parentCollectionId: Int, parametersQuery: String, surveyTable: String, replacements: Map[String, String], metabaseApiKey: String): Unit = {
              val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
              val stateNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyTable, "state_name", metabaseApiKey, createDashboardQuery)
              val districtNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyTable, "district_name", metabaseApiKey, createDashboardQuery)
              val blockNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyTable, "block_name", metabaseApiKey, createDashboardQuery)
              val clusterNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyTable, "cluster_name", metabaseApiKey, createDashboardQuery)
              val schoolNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyTable, "school_name", metabaseApiKey, createDashboardQuery)
              val orgNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyTable, "organisation_name", metabaseApiKey, createDashboardQuery)
              metabaseUtil.updateColumnCategory(stateNameId, "State")
              metabaseUtil.updateColumnCategory(districtNameId, "City")
              val questionCardIdList = UpdateCsvDownloadJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, stateNameId, districtNameId, blockNameId, clusterNameId, schoolNameId, orgNameId, replacements, metabaseUtil, postgresUtil)
              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
              val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("Collection For", reportFor))
              val surveyMetadataJsonString = surveyMetadataJson.toString.replace("'", "''")
              postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJsonString' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
            }

            logger.info(s"===========> Started processing CSV dashboard for Question report <===========")
            commonStepsToCreateCsvDashboard(questionReportConfigQuery, databaseId, questionDashboardName, dashboardId, tabIdMap, parentCollectionId, questionParametersQuery, surveyQuestionTable, questionReplacements, metabaseApiKey)
            logger.info(s"===========> Completed processing CSV dashboard for Question report <===========")
            logger.info(s"===========> Started processing CSV dashboard for Status report <===========")
            commonStepsToCreateCsvDashboard(statusReportConfigQuery, databaseId, statusDashboardName, dashboardId, tabIdMap, parentCollectionId, statusParametersQuery, surveyStatusTable, statusReplacements, metabaseApiKey)
            logger.info(s"===========> Completed processing CSV dashboard for Status report <===========")
          } catch {
            case e: Exception =>
              postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
              logger.error(s"An error occurred: ${e.getMessage}")
              e.printStackTrace()
              -1
          }
        }

        def createSurveyStatusDashboard(parentCollectionId: Int, databaseId: Int, dashboardId: Int, tabIdMap: Map[String, Int], metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyStatusTable: String, reportFor: String, metabaseApiKey: String): Unit = {
          try {
            val dashboardName: String = s"Status Report"
            val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
            val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
            val stateNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyStatusTable, "state_name", metabaseApiKey, createDashboardQuery)
            val districtNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyStatusTable, "district_name", metabaseApiKey, createDashboardQuery)
            val blockNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyStatusTable, "block_name", metabaseApiKey, createDashboardQuery)
            val clusterNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyStatusTable, "cluster_name", metabaseApiKey, createDashboardQuery)
            val schoolNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyStatusTable, "school_name", metabaseApiKey, createDashboardQuery)
            val orgNameId: Int = metabaseUtil.getTheColumnId(databaseId, surveyStatusTable, "organisation_name", metabaseApiKey, createDashboardQuery)
            metabaseUtil.updateColumnCategory(stateNameId, "State")
            metabaseUtil.updateColumnCategory(districtNameId, "City")
            val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Status-Report' AND question_type IN ('big-number', 'table');"
            val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, stateNameId, districtNameId, blockNameId, clusterNameId, schoolNameId, orgNameId, surveyStatusTable, metabaseUtil, postgresUtil)
            val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
            val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("Collection For", reportFor))
            val surveyMetadataJsonString = surveyMetadataJson.toString.replace("'", "''")
            postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJsonString' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
          }
          catch {
            case e: Exception =>
              postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
              logger.error(s"An error occurred: ${e.getMessage}")
              e.printStackTrace()
          }
        }

        val endTime = System.currentTimeMillis()
        val totalTimeSeconds = (endTime - startTime) / 1000
        logger.info(s"Total time taken: $totalTimeSeconds seconds")
        logger.info(s"***************** End of Processing the Metabase Survey Dashboard *****************\n")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error processing event: ${event._id}: ${e.getMessage}", e)
    }
  }

}