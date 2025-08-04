package org.shikshalokam.job.survey.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.collection.JavaConverters._
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
    val evidenceBaseUrl: String = config.evidenceBaseUrl
    val metaDataTable = config.dashboard_metadata
    val reportConfig: String = config.report_config
    val metabaseDatabase: String = config.metabaseDatabase
    val admin = event.admin
    val targetedSolutionId = event.targetedSolution
    val surveyQuestionTable = s"${targetedSolutionId}"
    val dashboardDescription = s"Analytical overview of the data for solutionId $targetedSolutionId"
    val solutionName = postgresUtil.fetchData(s"""SELECT entity_name FROM $metaDataTable WHERE entity_id = '$targetedSolutionId'""").collectFirst { case map: Map[_, _] => map.getOrElse("entity_name", "").toString }.getOrElse("")
    val surveyStatusTable = s"""${targetedSolutionId}_survey_status"""
    val targetedProgramId: String = {
      val id = Option(event.targetedProgram).map(_.trim).getOrElse("")
      if (id.nonEmpty) id
      else {
        val query = s"SELECT program_id FROM $solutions WHERE solution_id = '$targetedSolutionId'"
        println(query)
        postgresUtil.fetchData(query) match {
          case List(map: Map[_, _]) => Option(map.get("program_id")).flatten.map(_.toString).getOrElse("")
          case _ => ""
        }
      }
    }

    val solutionExternalId = postgresUtil.fetchData(s"SELECT external_id from $solutions where solution_id = '$targetedSolutionId'") match {
      case List(map: Map[_, _]) => map.get("external_id").map(_.toString).getOrElse("")
      case _ => ""
    }

    val programExternalId = postgresUtil.fetchData(s"SELECT program_external_id from $solutions where solution_id = '$targetedSolutionId'") match {
      case List(map: Map[_, _]) => map.get("program_external_id").map(_.toString).getOrElse("")
      case _ => ""
    }

    val programName = postgresUtil.fetchData(s"""SELECT entity_name from $metaDataTable where entity_id = '$targetedProgramId'""").collectFirst { case map: Map[_, _] => map.get("entity_name").map(_.toString).getOrElse("") }.getOrElse("")

    val orgNameQuery = s"""SELECT organisation_name from "$targetedSolutionId" where solution_id = '$targetedSolutionId' group by organisation_name limit 1"""
    println(s"orgNameQuery: $orgNameQuery")
    val orgName = postgresUtil.fetchData(orgNameQuery)
      .collectFirst { case map: Map[_, _] =>
        map.get("organisation_name") match {
          case Some(s: String) if s.trim.nonEmpty && s != "null" => s
          case _ => null
        }
      }
      .filter(_ != null)

    val programCollectionName = s"$programName [org : $orgName]"
    val solutionCollectionName = s"$solutionName [Survey]"

    val programDescriptionQuery = s"SELECT program_description from $solutions where solution_id = '$targetedSolutionId'"
    val programDescription = postgresUtil.fetchData(programDescriptionQuery) match {
      case List(map: Map[_, _]) => Option(map.get("program_description")).flatten.map(_.toString).getOrElse("")
      case _ => ""
    }

    val externalIdQuery = s"SELECT external_id from $solutions where solution_id = '$targetedSolutionId'"
    val externalId = postgresUtil.fetchData(externalIdQuery) match {
      case List(map: Map[_, _]) => Option(map.get("external_id")).flatten.map(_.toString).getOrElse("")
      case _ => ""
    }

    val solutionDescriptionQuery = s"SELECT description from $solutions where solution_id = '$targetedSolutionId'"
    val solutionDescription = postgresUtil.fetchData(solutionDescriptionQuery) match {
      case List(map: Map[_, _]) => Option(map.get("description")).flatten.map(_.toString).getOrElse("")
      case _ => ""
    }

    val tabList: List[String] = List("Status Report", "Question Report", "Status CSV", "Question CSV")

    println(s"surveyQuestionTable: $surveyQuestionTable")
    println(s"reportType: ${event.reportType}")
    println(s"admin: 1")
    println(s"Targeted Program ID: $targetedProgramId")
    println(s"Targeted Program Name: $programName")
    println(s"Targeted Solution ID: $targetedSolutionId")


    if (targetedSolutionId.nonEmpty && solutionName.nonEmpty && targetedProgramId.nonEmpty && programName.nonEmpty){
      event.reportType match {
        case "Survey" =>
          /**
           * Logic to process and create Survey Admin Dashboard
           */
          if (solutionName != null && solutionName.nonEmpty) {
            val (adminCollectionPresent, adminCollectionId) = validateCollection(s"Programs", "Admin")
            if (adminCollectionPresent && adminCollectionId != 0) {
              val (programCollectionPresent, programCollectionId) = validateCollection(programCollectionName, "Admin", Some(targetedProgramId))
              if (programCollectionPresent && programCollectionId != 0) {
                val (solutionCollectionPresent, solutionCollectionId) = validateCollection(solutionCollectionName, "Admin", Some(targetedSolutionId))
                if (solutionCollectionPresent && solutionCollectionId != 0) {
                  println(s"=====> $solutionCollectionName collection is present, hence skipping the process ......")
                } else {
                  val solutionCollectionId = createSurveyCollectionInsideProgram(programCollectionId, targetedSolutionId, solutionExternalId, s"$solutionName [Survey]", solutionDescription, "Admin")
                  val dashboardId: Int = Utils.createDashboard(solutionCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
                  val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                  createAdminDashboard(solutionCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Admin")
                  createSurveyCsvDashboard(solutionCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Admin")
                }
              } else {
                println(s"=====> $programCollectionName collection is not present, creating $programCollectionName collection for Admin ......")
                val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId, targetedProgramId, programExternalId, s"$programName [org : $orgName]", programDescription, "Admin")
                val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId, targetedSolutionId, solutionExternalId, s"$solutionName [Survey]", solutionDescription, "Admin")
                val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
                val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                createAdminDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Admin")
                createSurveyCsvDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Admin")
              }
            } else {
              println(s"=====> Programs Collection is not present, creating Programs Collection ......")
              val adminCollectionId = createAdminCollection
              val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId, targetedProgramId, programExternalId, s"$programName [org : $orgName]", programDescription, "Admin")
              val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId, targetedSolutionId, solutionExternalId, s"$solutionName [Survey]", solutionDescription, "Admin")
              val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
              val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
              createAdminDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Admin")
              createSurveyCsvDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Admin")
            }
          }

          /**
           * Logic to process and create Program Dashboard for Survey
           */
          if (programName != null && programName.nonEmpty && solutionName != null && solutionName.nonEmpty) {
            println("~~~~~~~~ Start Survey Program Dashboard Processing~~~~~~~~")

            val (programCollectionPresent, programCollectionId) = validateCollection(programCollectionName, "Program Manager", Some(targetedProgramId))
            if (programCollectionPresent && programCollectionId != 0) {
              println(s"=====> $programCollectionName collection is present hence skipping the process ......")
              val (solutionCollectionPresent, solutionCollectionId) = validateCollection(solutionCollectionName, "Program Manager", Some(targetedSolutionId))
              if (solutionCollectionPresent && solutionCollectionId != 0) {
                println(s"=====> $solutionCollectionName collection is present, hence skipping the process ......")
              } else {
                println(s"=====> $solutionCollectionName collection is not present, creating $solutionCollectionName collection for Program Manager ......")
                val solutionCollectionId = createSurveyCollectionInsideProgram(programCollectionId, targetedSolutionId, solutionExternalId, solutionCollectionName, solutionDescription, "Program Manager")
                val dashboardId: Int = Utils.createDashboard(solutionCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
                val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                createSurveyCsvDashboard(solutionCollectionId, dashboardId, tabIdMap, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Program")
                createProgramDashboard(solutionCollectionId, dashboardId, tabIdMap, solutionCollectionName, "Program")
              }
            } else {
              println(s"=====> $programCollectionName collection is not present, creating $programCollectionName collection for Program Manager ......")
              val programCollectionId = createProgramCollection(programCollectionName, targetedProgramId, programExternalId, programDescription, "Program Manager")
              val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId, targetedSolutionId, solutionExternalId, solutionCollectionName, solutionDescription, "Program Manager")
              val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
              val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
              createSurveyCsvDashboard(surveyCollectionId, dashboardId, tabIdMap, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Program")
              createProgramDashboard(surveyCollectionId, dashboardId, tabIdMap, solutionCollectionName, "Program")
            }
            println(s"***************** Processing Completed for Survey Metabase Dashboard Event with Id = ${event._id}*****************\n\n")
          }

          def createAdminDashboard(solutionCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], solutionCollectionName: String, category: String): Unit = {
            val parentCollectionId = createSurveyQuestionDashboard(solutionCollectionId, dashboardId, tabIdMap, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, evidenceBaseUrl, category)
            createSurveyStatusDashboard(parentCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable, category)
          }

          def createProgramDashboard(solutionCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], solutionCollectionName: String, category: String): Unit = {
            val parentCollectionId = createSurveyQuestionDashboard(solutionCollectionId, dashboardId, tabIdMap, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, evidenceBaseUrl, category)
            createSurveyStatusDashboard(parentCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable, category)
          }

          def createAdminCollection: Int = {
            val (adminCollectionName, adminCollectionDescription) = ("Programs", s"All programs made available on the platform are stored in this collection.\n\nCollection For: Admin")
            val adminCollectionId: Int = Utils.createCollection(adminCollectionName, adminCollectionDescription, metabaseUtil)
            val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", adminCollectionId).put("collectionName", adminCollectionName))
            postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb WHERE entity_id = '1';")
            Utils.createGroupToDashboard(metabaseUtil, "Report_Admin_Programs", adminCollectionId)
            adminCollectionId
          }

          def createProgramCollection(programCollectionName: String, targetedProgramId: String, programExternalId: String, programDescription: String, reportFor: String): Int = {
            val programCollectionDescription = s"Program Id: $targetedProgramId\n\nProgram External Id: $programExternalId\n\nCollection For: $reportFor\n\nProgram Description: $programDescription"
            val programCollectionId: Int = Utils.createCollection(programCollectionName, programCollectionDescription, metabaseUtil)
            if (programCollectionId != -1) {
              val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName).put("Collection For", reportFor))
              postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJson' ::jsonb WHERE entity_id = '$targetedProgramId';")
              Utils.createGroupToDashboard(metabaseUtil, s"Program_Manager_$targetedProgramId", programCollectionId)
              programCollectionId
            } else {
              println(s"$programName [$targetedProgramId] returned -1")
              -1
            }
          }

          def createProgramCollectionInsideAdmin(adminCollectionId: Int, targetedProgramId: String, programExternalId: String, ProgramCollectionName: String, programDescription: String, reportFor: String): Int = {
            val programCollectionDescription = s"Program Id: $targetedProgramId\n\nExternal Id: $programExternalId\n\nCollection For: $reportFor\n\nProgram Description: $programDescription"
            val ProgramCollectionId = Utils.createCollection(ProgramCollectionName, programCollectionDescription, metabaseUtil, Some(adminCollectionId))
            val ProgramMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", ProgramCollectionId).put("collectionName", ProgramCollectionName).put("Collection For", reportFor))
            postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$ProgramMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedProgramId';")
            ProgramCollectionId
          }


          def createSurveyCollectionInsideProgram(programCollectionId: Int, targetedSolutionId: String, solutionExternalId: String, surveyCollectionName: String, surveyDescription: String, reportFor: String): Int = {
            val solutionCollectionDescription = s"Solution Id: $targetedSolutionId\n\nSolution External Id: $solutionExternalId\n\nCollection For: $reportFor\n\nSolution Description: $surveyDescription"
            val surveyCollectionId = Utils.createCollection(surveyCollectionName, solutionCollectionDescription, metabaseUtil, Some(programCollectionId))
            val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", surveyCollectionId).put("collectionName", surveyCollectionName).put("Collection For", reportFor))
            postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
            surveyCollectionId
          }

      }
    }

    /**
     * Logic for Survey Question Dashboard
     */
    def createSurveyQuestionDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyQuestionTable: String, evidenceBaseUrl: String, reportFor: String): Int = {
      try {
        val dashboardName: String = s"Question Report"
        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
        if (parentCollectionId != -1) {
          val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
          val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
          if (databaseId != -1) {
            metabaseUtil.syncDatabaseAndRescanValues(databaseId)
            val stateNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "state_name", postgresUtil, createDashboardQuery)
            val districtNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "district_name", postgresUtil, createDashboardQuery)
            val blockNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "block_name", postgresUtil, createDashboardQuery)
            val clusterNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "cluster_name", postgresUtil, createDashboardQuery)
            metabaseUtil.updateColumnCategory(stateNameId, "State")
            metabaseUtil.updateColumnCategory(districtNameId, "City")
            val schoolNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "school_name", postgresUtil, createDashboardQuery)
            val orgNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyQuestionTable, "organisation_name", postgresUtil, createDashboardQuery)
            val questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(parentCollectionId, databaseId, dashboardId, tabId, stateNameId, districtNameId, blockNameId: Int, clusterNameId, schoolNameId, orgNameId, surveyQuestionTable, metabaseUtil, postgresUtil, reportConfig, evidenceBaseUrl)
            val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
            val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Survey' AND question_type = 'Question-Parameter'"
            UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
            val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("Collection For", reportFor))
            postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
            parentCollectionId
          } else {
            println("Database Id returned -1")
            -1
          }
        } else {
          println("Solution CollectionId returned -1")
          -1
        }
      }
      catch {
        case e: Exception =>
          postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
          println(s"An error occurred: ${e.getMessage}")
          e.printStackTrace()
          -1
      }
    }

    def createSurveyCsvDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyQuestionTable: String, surveyStatusTable: String, evidenceBaseUrl: String, reportFor: String): Unit = {
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

        def commonStepsToCreateCsvDashboard(reportConfigQuery: String, dashboardName: String, dashboardId: Int, tabIdMap: Map[String, Int], parentCollectionId: Int, parametersQuery: String, surveyTable: String, replacements: Map[String, String]): Unit = {
          val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
          val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
          if (databaseId != -1) {
            metabaseUtil.syncDatabaseAndRescanValues(databaseId)
            val stateNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyTable, "state_name", postgresUtil, createDashboardQuery)
            val districtNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyTable, "district_name", postgresUtil, createDashboardQuery)
            val blockNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyTable, "block_name", postgresUtil, createDashboardQuery)
            val clusterNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyTable, "cluster_name", postgresUtil, createDashboardQuery)
            metabaseUtil.updateColumnCategory(stateNameId, "State")
            metabaseUtil.updateColumnCategory(districtNameId, "City")
            val schoolNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyTable, "school_name", postgresUtil, createDashboardQuery)
            val orgNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyTable, "organisation_name", postgresUtil, createDashboardQuery)
            val questionCardIdList = UpdateCsvDownloadJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, stateNameId, districtNameId, blockNameId, clusterNameId, schoolNameId, orgNameId, replacements, metabaseUtil, postgresUtil)
            val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
            val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("Collection For", reportFor))
            postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
          }
        }

        println(s"==========> Started processing CSV dashboard for Question report <===========")
        commonStepsToCreateCsvDashboard(questionReportConfigQuery, questionDashboardName, dashboardId, tabIdMap, parentCollectionId, questionParametersQuery, surveyQuestionTable, questionReplacements)
        println(s"==========> Completed processing CSV dashboard for Question report <===========")
        println(s"==========> Started processing CSV dashboard for Status report <===========")
        commonStepsToCreateCsvDashboard(statusReportConfigQuery, statusDashboardName, dashboardId, tabIdMap, parentCollectionId, statusParametersQuery, surveyStatusTable, statusReplacements)
        println(s"==========> Completed processing CSV dashboard for Status report <===========")
      } catch {
        case e: Exception =>
          postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
          println(s"An error occurred: ${e.getMessage}")
          e.printStackTrace()
          -1
      }
    }

    def createSurveyStatusDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyStatusTable: String, reportFor: String): Unit = {
      try {
        val dashboardName: String = s"Status Report"
        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
        val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId != -1) {
          metabaseUtil.syncDatabaseAndRescanValues(databaseId)
          val statenNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyStatusTable, "state_name", postgresUtil, createDashboardQuery)
          val districtNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyStatusTable, "district_name", postgresUtil, createDashboardQuery)
          val blockNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyStatusTable, "block_name", postgresUtil, createDashboardQuery)
          val clusterNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyStatusTable, "cluster_name", postgresUtil, createDashboardQuery)
          val schoolNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyStatusTable, "school_name", postgresUtil, createDashboardQuery)
          val orgNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, surveyStatusTable, "organisation_name", postgresUtil, createDashboardQuery)
          metabaseUtil.updateColumnCategory(statenNameId, "State")
          metabaseUtil.updateColumnCategory(districtNameId, "City")
          val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Status-Report' AND question_type IN ('big-number', 'table');"
          val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, statenNameId, districtNameId, blockNameId, clusterNameId, schoolNameId, orgNameId, surveyStatusTable, metabaseUtil, postgresUtil)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("Collection For", reportFor))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
        }
      }
      catch {
        case e: Exception =>
          postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
          println(s"An error occurred: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    def validateCollection(collectionName: String, reportFor: String, reportId: Option[String] = None): (Boolean, Int) = {
      val mapper = new ObjectMapper()
      println(s">>> Checking Metabase API for collection: $collectionName")
      try {
        val collections = mapper.readTree(metabaseUtil.listCollections())
        val result = collections match {
          case arr: ArrayNode =>
            arr.asScala.find { c =>
                val name = Option(c.get("name")).map(_.asText).getOrElse("")
                val desc = Option(c.get("description")).map(_.asText).getOrElse("")
                val matchesName = name == collectionName
                val matchesReportFor = desc.contains(s"Collection For: $reportFor")
                val matchesReportId = reportId.forall(id =>
                  desc.contains(s"Program Id: $id") || desc.contains(s"Solution Id: $id")
                )

                val isMatch = if (reportId.isEmpty) matchesName && matchesReportFor else matchesName && matchesReportFor && matchesReportId
                isMatch
              }.map(c => (true, Option(c.get("id")).map(_.asInt).getOrElse(0)))
              .getOrElse((false, 0))
          case _ => (false, 0)
        }
        println(s">>> API result: $result")
        result
      } catch {
        case e: Exception =>
          println(s"[ERROR] API or JSON failure: ${e.getMessage}")
          (false, 0)
      }
    }
  }

}