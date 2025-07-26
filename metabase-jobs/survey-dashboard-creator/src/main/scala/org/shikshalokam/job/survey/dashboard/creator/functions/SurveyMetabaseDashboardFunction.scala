package org.shikshalokam.job.survey.dashboard.creator.functions

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

    val programDescriptionAdd = programDescription + "\n" + s"[ExternalId : $externalId]" + "\n" + s"[ProgramId : $targetedProgramId]"

    val solutionDescriptionQuery = s"SELECT description from $solutions where solution_id = '$targetedSolutionId'"
    val solutionDescription = postgresUtil.fetchData(solutionDescriptionQuery) match {
      case List(map: Map[_, _]) => Option(map.get("description")).flatten.map(_.toString).getOrElse("")
      case _ => ""
    }

    val solutionMetadataQuery = s"SELECT main_metadata FROM $metaDataTable WHERE entity_id = '$targetedSolutionId';"
    val solutionMetadataOutput = postgresUtil.fetchData(solutionMetadataQuery).collectFirst {
      case map: Map[_, _] =>
        map.get("main_metadata") match {
          case Some(value) if value != null && value.toString.trim.nonEmpty && value.toString != "null" =>
            Some(value.toString)
          case _ => None
        }
    }.flatten

    val objectMapper = new ObjectMapper()

    def dashboardExists(solutionMetadataOutput: String, dashboardName: String, category: String): String = {
      if (solutionMetadataOutput == null || solutionMetadataOutput.trim.isEmpty) return "No"
      val node: JsonNode = objectMapper.readTree(solutionMetadataOutput)
      if (node.elements().asScala.exists { arrElem: JsonNode =>
        arrElem.has("dashboardName") && arrElem.has("category") &&
          arrElem.get("dashboardName").asText() == dashboardName &&
          arrElem.get("category").asText() == category
      }) "Yes" else "No"
    }

    val adminSurveyQuestionDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Question Report", "Admin")).getOrElse("No")
    val adminSurveyStatusDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Status Report", "Admin")).getOrElse("No")

    val programSurveyQuestionDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Question Report", "Program")).getOrElse("No")
    val programSurveyStatusDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Status Report", "Program")).getOrElse("No")
    val tabList: List[String] = List("Status Report", "Question Report", "Status Raw Data for CSV Downloads", "Question Raw Data for CSV Downloads")


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
        if (solutionName != null && solutionName.nonEmpty) {
          val adminCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Programs"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '1';"""
          val adminCollectionPresent = postgresUtil.fetchData(adminCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
          val adminCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Programs' AND entity_id = '1';"
          val adminCollectionId = postgresUtil.executeQuery[Int](adminCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
          if (adminCollectionPresent == "Yes") {
            val programCollectionName = s"$programName [org : $orgName]"
            val programCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"$programCollectionName"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '1';"""
            val programCollectionPresent = postgresUtil.fetchData(programCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
            val programCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = '$programCollectionName' AND entity_id = '1';"
            val programCollectionId = postgresUtil.executeQuery[Int](programCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
            if (programCollectionPresent == "Yes") {
              val surveyCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"$solutionName [Survey]"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '$targetedSolutionId';"""
              val surveyCollectionPresent = postgresUtil.fetchData(surveyCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
              val surveyCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = '$solutionName [Survey]' AND entity_id = '$targetedSolutionId';"
              val surveyCollectionId = postgresUtil.executeQuery[Int](surveyCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
              if (surveyCollectionPresent == "Yes") {
                println("=====> Admin and Survey collection present creating Solution collection & dashboard")
                val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
                val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                createAdminDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Admin")
                createSurveyCsvDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Admin")
              } else {
                println(s"")
                val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId, s"$solutionName [Survey]", solutionDescription)
                val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
                val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                createAdminDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Admin")
                createSurveyCsvDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Admin")
              }
            } else {
              val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId, s"$programName [org : $orgName]", programDescriptionAdd)
              val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId, s"$solutionName [Survey]", solutionDescription)
              val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
              val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
              createAdminDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Admin")
              createSurveyCsvDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Admin")
            }
          } else {
            val adminCollectionId = createAdminCollection
            val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId, s"$programName [org : $orgName]", programDescriptionAdd)
            val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId, s"$solutionName [Survey]", solutionDescription)
            val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
            val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
            createAdminDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Admin")
            createSurveyCsvDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Admin")
          }
        }

        def createAdminDashboard(solutionCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], solutionCollectionName: String, category: String): Unit = {
          if (adminSurveyQuestionDashboardPresent == "Yes") {
            println(s"Admin Survey Question Dashboard already present for $solutionCollectionName")
          } else {
            val parentCollectionId = createSurveyQuestionDashboard(solutionCollectionId, dashboardId, tabIdMap, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, evidenceBaseUrl, category)
            if (adminSurveyStatusDashboardPresent == "Yes") {
              println(s"Admin Survey Status Dashboard already present for $solutionCollectionName")
            } else {
              createSurveyStatusDashboard(parentCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable, category)
            }
          }
        }

        def createProgramDashboard(solutionCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], solutionCollectionName: String, category: String): Unit = {
          if (programSurveyQuestionDashboardPresent == "Yes") {
            println(s"Program Survey Question Dashboard already present for $solutionCollectionName")
          } else {
            val parentCollectionId = createSurveyQuestionDashboard(solutionCollectionId, dashboardId, tabIdMap, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, evidenceBaseUrl, category)
            if (programSurveyStatusDashboardPresent == "Yes") {
              println(s"Program Survey Status Dashboard already present for $solutionCollectionName")
            } else {
              createSurveyStatusDashboard(parentCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyStatusTable, category)
            }
          }
        }

        def createAdminCollection: Int = {
          val (adminCollectionName, adminCollectionDescription) = ("Programs", "Program Collection which contains all the sub-collections and questions")
          val groupName: String = s"Report_Admin"
          val adminCollectionId: Int = Utils.createCollection(adminCollectionName, adminCollectionDescription, metabaseUtil)
          val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", adminCollectionId).put("collectionName", adminCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb WHERE entity_id = '1';")
          CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, adminCollectionId)
          adminCollectionId
        }

        def createProgramCollection(programCollectionName: String, programDescription: String): Int = {
          val groupName: String = s"Program_Manager[$programName - $targetedProgramId]"
          val programCollectionId: Int = Utils.createCollection(programCollectionName, programDescription, metabaseUtil)
          if (programCollectionId != -1) {
            val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName))
            postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJson' ::jsonb WHERE entity_id = '$targetedProgramId';")
            CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, programCollectionId)
            programCollectionId
          } else {
            println(s"$programName [$targetedProgramId] returned -1")
            -1
          }
        }

        def createProgramCollectionInsideAdmin(adminCollectionId: Int, ProgramCollectionName: String, ProgramCollectionDescription: String): Int = {
          val ProgramCollectionId = Utils.createCollection(ProgramCollectionName, ProgramCollectionDescription, metabaseUtil, Some(adminCollectionId))
          val ProgramMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", ProgramCollectionId).put("collectionName", ProgramCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$ProgramMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '1';")
          ProgramCollectionId
        }


        def createSurveyCollectionInsideProgram(programCollectionId: Int, surveyCollectionName: String, surveyCollectionDescription: String): Int = {
          val surveyCollectionId = Utils.createCollection(surveyCollectionName, surveyCollectionDescription, metabaseUtil, Some(programCollectionId))
          val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", surveyCollectionId).put("collectionName", surveyCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$surveyMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedProgramId';")
          surveyCollectionId
        }

        /**
         * Logic to process and create Program Dashboard for Survey
         */
        if (programName != null && programName.nonEmpty && solutionName != null && solutionName.nonEmpty) {
          println("~~~~~~~~ Start Survey Program Dashboard Processing~~~~~~~~")
          val programCollectionCheckQuery = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"$programName [org : $orgName]"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId'"""
          val programCollectionPresent = postgresUtil.fetchData(programCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
          val programCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = '$programName [org : $orgName]' AND entity_id = '$targetedProgramId';"
          val programCollectionId = postgresUtil.executeQuery[Int](programCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
          if (programCollectionPresent == "Yes") {
            val surveyCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"$solutionName [Survey]"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId';"""
            val surveyCollectionPresent = postgresUtil.fetchData(surveyCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
            val surveyCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = '$solutionName [Survey]' AND entity_id = '$targetedProgramId';"
            val surveyCollectionId = postgresUtil.executeQuery[Int](surveyCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
            if (surveyCollectionPresent == "Yes") {
              println("=====> Program and Survey collection present creating Solution collection & dashboard")
              val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
              val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
              createSurveyCsvDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Program")
              createProgramDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Program")
            } else {
              println("=====> Only Program collection is present creating Survey Collection then Solution collection & dashboard")
              val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId, s"$solutionName [Survey]", solutionDescription)
              val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
              val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
              createSurveyCsvDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Program")
              createProgramDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Program")
            }
          } else {
            println("=====> Program collection is not present creating Program, Survey Collection then Solution collection & dashboard")
            val programCollectionId = createProgramCollection(s"$programName [org : $orgName]", programDescriptionAdd)
            val surveyCollectionId = createSurveyCollectionInsideProgram(programCollectionId, s"$solutionName [Survey]", solutionDescription)
            val dashboardId: Int = Utils.createDashboard(surveyCollectionId, s"Survey Dashboard", dashboardDescription, metabaseUtil)
            val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
            createSurveyCsvDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, surveyQuestionTable, surveyStatusTable, evidenceBaseUrl, "Program")
            createProgramDashboard(surveyCollectionId, dashboardId, tabIdMap, s"$solutionName [Survey]", "Program")
          }
          println("~~~~~~~~ End Survey Program Dashboard Processing~~~~~~~~")
        } else {
          println("Program name or Solution name is null or empty, skipping the processing.")
        }
        println(s"***************** Processing Completed for Survey Metabase Dashboard Event with Id = ${event._id}*****************\n\n")
    }
  }

  /**
   * Logic for Survey Question Dashboard
   */
  def createSurveyQuestionDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyQuestionTable: String, evidenceBaseUrl: String, category: String): Int = {
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
          val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("category", category))
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

  def createSurveyCsvDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyQuestionTable: String, surveyStatusTable: String, evidenceBaseUrl: String, category: String): Unit = {
    try {
      val questionDashboardName: String = s"Question Raw Data for CSV Downloads"
      val questionReportConfigQuery: String = s"SELECT * FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Question-Report' AND question_type = 'table';"
      val questionParametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Question-Report' AND question_type = 'Question-Parameter'"
      val questionReplacements: Map[String, String] = Map(
        "${questionTable}" -> s""""$surveyQuestionTable"""",
        "${evidenceBaseUrl}" -> s"""'$evidenceBaseUrl'"""
      )
      val statusDashboardName: String = s"Status Raw Data for CSV Downloads"
      val statusReportConfigQuery: String = s"SELECT * FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Status-Report' AND question_type = 'table';"
      val statusParametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Survey' AND report_name = 'Status-Report' AND question_type = 'Status-Parameter';"
      val statusReplacements: Map[String, String] = Map(
        "${statusTable}" -> s""""${surveyStatusTable}"""",
      )
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      
      def commonStepsToCreateCsvDashboard(reportConfigQuery: String, dashboardName: String, dashboardId: Int, tabIdMap: Map[String, Int], parentCollectionId: Int, parametersQuery: String, surveyTable: String, replacements: Map[String, String] ): Unit = {
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
          val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("category", category))
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

  def createSurveyStatusDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, surveyStatusTable: String, category: String): Unit = {
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
        val surveyMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("category", category))
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

}