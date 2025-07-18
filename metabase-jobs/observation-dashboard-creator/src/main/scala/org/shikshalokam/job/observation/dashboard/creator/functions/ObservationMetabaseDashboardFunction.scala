package org.shikshalokam.job.observation.dashboard.creator.functions

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.observation.dashboard.creator.domain.Event
import org.shikshalokam.job.observation.dashboard.creator.task.ObservationMetabaseDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable.{ListMap, _}
class ObservationMetabaseDashboardFunction(config: ObservationMetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObservationMetabaseDashboardFunction])

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


    val evidenceBaseUrl: String = config.evidenceBaseUrl
    val admin = event.admin
    val solutions: String = config.solutions
    val targetedSolutionId = event.targetedSolution
    val isRubric = event.isRubric
    val entityType = event.entityType
    val metaDataTable = config.dashboard_metadata
    val reportConfig: String = config.report_config
    val metabaseDatabase: String = config.metabaseDatabase
    val solutionTable: String = config.solutions
    val ObservationDomainTable = s"${targetedSolutionId}_domain"
    val ObservationQuestionTable = s"${targetedSolutionId}_questions"
    val ObservationStatusTable = s"${targetedSolutionId}_status"
    val dashboardDescription: String = s"Analytical overview of the data for solutionId $targetedSolutionId"
    val solutionName = postgresUtil.fetchData(s"""SELECT entity_name FROM $metaDataTable WHERE entity_id = '$targetedSolutionId'""").collectFirst { case map: Map[_, _] => map.getOrElse("entity_name", "").toString }.getOrElse("")
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
    val programName = postgresUtil.fetchData(s"""SELECT entity_name from $metaDataTable where entity_id = '$targetedProgramId'""").collectFirst { case map: Map[_, _] => map.get("entity_name").map(_.toString).getOrElse("") }.getOrElse("").take(80)
    val orgName = postgresUtil.fetchData(s"""SELECT org_name from "$ObservationStatusTable" where solution_id = '$targetedSolutionId' group by org_name limit 1""") match {
      case List(map: Map[_, _]) => map.get("org_name").map(_.toString).getOrElse("")
      case _ => ""
    }
    val programDescription = postgresUtil.fetchData(s"SELECT program_description from $solutionTable where solution_id = '$targetedSolutionId'") match {
      case List(map: Map[_, _]) => map.get("program_description").map(_.toString).getOrElse("").take(230)
      case _ => ""
    }
    val externalId = postgresUtil.fetchData(s"SELECT external_id from $solutionTable where solution_id = '$targetedSolutionId'") match {
      case List(map: Map[_, _]) => map.get("external_id").map(_.toString).getOrElse("")
      case _ => ""
    }
    val solutionDescription = postgresUtil.fetchData(s"SELECT description from $solutionTable where solution_id = '$targetedSolutionId'") match {
      case List(map: Map[_, _]) => map.get("description").map(_.toString).getOrElse("").take(230)
      case _ => ""
    }
    val programDescriptionAdd = s"$programDescription\n\n[ExternalId : $externalId]\n\n[ProgramId : $targetedProgramId]"
    val solutionMetadataOutput = postgresUtil.fetchData(s"SELECT main_metadata FROM $metaDataTable WHERE entity_id = '$targetedSolutionId';").collectFirst {
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

    val adminObservationQuestionDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Domain And Question Report", "Admin")).getOrElse("No")
    val adminObservationWithoutRubricDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Question Report", "Admin")).getOrElse("No")
    val adminObservationStatusDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Status Report", "Admin")).getOrElse("No")
    val adminObservationDomainDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Domain Report", "Admin")).getOrElse("No")

    val programObservationQuestionDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Domain And Question Report", "Program")).getOrElse("No")
    val programObservationWithoutRubricDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Question Report", "Program")).getOrElse("No")
    val programObservationStatusDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Status Report", "Program")).getOrElse("No")
    val programObservationDomainDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Domain Report", "Program")).getOrElse("No")
    val tabList: List[String] = List("Status Report", "Domain Report", "Question Report", "Status Raw Data for CSV Downloads", "Domain and Criteria Raw Data for CSV Downloads", "Question Raw Data for CSV Downloads")

    println(s"domainTable: $ObservationDomainTable")
    println(s"questionTable: $ObservationQuestionTable")
    println(s"isRubric: ${event.isRubric}")
    println(s"targetedSolutionId: $targetedSolutionId")
    println(s"targetedProgramId: $targetedProgramId")
    println(s"admin: $admin")
    println(s"programName: $programName")
    println(s"solutionsName: $solutionName")


    event.reportType match {
      case "Observation" =>
        if (solutionName != null && solutionName.nonEmpty) {
          println(s"===========================================================================================================================================")
          println(s"****************************************** Started Processing Metabase Admin Dashboard For Observation ************************************")
          println(s"===========================================================================================================================================")
          val adminCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Programs"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '1';"""
          val adminCollectionPresent = postgresUtil.fetchData(adminCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
          val adminCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Programs' AND entity_id = '1';"
          val adminCollectionId = postgresUtil.executeQuery[Int](adminCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
          println("=====> checking Admin collection presence ......")
          if (adminCollectionPresent == "Yes") {
            println("=====> Admin collection is present, checking Program collection presence ......")
            val programCollectionName = s"$programName [org : $orgName]"
            val programCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"$programCollectionName"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '1';"""
            val programCollectionPresent = postgresUtil.fetchData(programCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
            val programCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = '$programCollectionName' AND entity_id = '1';"
            val programCollectionId = postgresUtil.executeQuery[Int](programCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
            if (programCollectionPresent == "Yes") {
              println(s"=====> Admin and Program collection present, checking solution collection presence for $solutionName ......")
              val solutionCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"$solutionName [Observation]"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '$targetedSolutionId';"""
              val solutionCollectionPresent = postgresUtil.fetchData(solutionCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
              val solutionCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = '$solutionName [Observation]' AND entity_id = '$targetedSolutionId';"
              val solutionCollectionId = postgresUtil.executeQuery[Int](solutionCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
              if (solutionCollectionPresent == "Yes") {
                println(s"=====> Solution collection is present, checking if Dashboard is already present for $solutionName [Observation] ......")
                val dashboardId: Int = Utils.createDashboard(solutionCollectionId, s"Observation Dashboard", dashboardDescription, metabaseUtil)
                val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                println(s"tabIdMap: $tabIdMap")
                createAdminDashboards(solutionCollectionId, dashboardId, tabIdMap, s"$solutionName [Observation]", "Admin")
                createObservationTableDashboard(solutionCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, ObservationStatusTable, ObservationDomainTable, ObservationQuestionTable, entityType, "Admin", isRubric)
              } else {
                println(s"=====> Solution collection is not present creating Solution collection & dashboard for $solutionName [Observation]")
                val solutionCollectionId: Int = createSolutionCollectionInsideProgram(programCollectionId, s"$solutionName [Observation]", solutionDescription)
                val dashboardId: Int = Utils.createDashboard(solutionCollectionId, s"Observation Dashboard", dashboardDescription, metabaseUtil)
                val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
                println(s"tabIdMap: $tabIdMap")
                createAdminDashboards(solutionCollectionId, dashboardId, tabIdMap, s"$solutionName [Observation]", "Admin")
                createObservationTableDashboard(solutionCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, ObservationStatusTable, ObservationDomainTable, ObservationQuestionTable, entityType, "Admin", isRubric)
              }
            } else {
              println(s"=====> Only Admin and Program collection is present creating [$programName] Program Collection then Solution collection & dashboard")
              val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId, programDescriptionAdd, programCollectionName)
              val solutionCollectionId: Int = createSolutionCollectionInsideProgram(programCollectionId, s"$solutionName [Observation]", solutionDescription)
              val dashboardId: Int = Utils.createDashboard(solutionCollectionId, s"Observation Dashboard", dashboardDescription, metabaseUtil)
              val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
              println(s"tabIdMap: $tabIdMap")
              createAdminDashboards(solutionCollectionId, dashboardId, tabIdMap, s"$solutionName [Observation]", "Admin")
              createObservationTableDashboard(solutionCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, ObservationStatusTable, ObservationDomainTable, ObservationQuestionTable, entityType, "Admin", isRubric)
            }
          } else {
            println("=====> Admin collection is not present creating Admin, Observation Collection then Solution collection & dashboard")
            val adminCollectionId = createAdminCollection
            val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId, programDescriptionAdd, s"$programName [org : $orgName]")
            val solutionCollectionId = createSolutionCollectionInsideProgram(programCollectionId, s"$solutionName [Observation]", solutionDescription)
            val dashboardId: Int = Utils.createDashboard(solutionCollectionId, s"Observation Dashboard", dashboardDescription, metabaseUtil)
            val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
            println(s"tabIdMap: $tabIdMap")
            createAdminDashboards(solutionCollectionId, dashboardId, tabIdMap, s"$solutionName [Observation]", "Admin")
            createObservationTableDashboard(solutionCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, ObservationStatusTable, ObservationDomainTable, ObservationQuestionTable, entityType, "Admin", isRubric)
          }
        }


        def createAdminDashboards(exactProgramCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], exactProgramCollectionName: String, category: String): Unit = {
          if (adminObservationStatusDashboardPresent == "Yes") {
            println(s"=====> Observation Status Dashboard is already present for $exactProgramCollectionName")
          } else {
            println(s"=====> Creating Observation Status Dashboard for $exactProgramCollectionName")
            createObservationStatusDashboard(exactProgramCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationStatusTable, entityType, category)
          }
          if (isRubric == "true") {
            if (adminObservationDomainDashboardPresent == "Yes") {
              println(s"=====> Observation Domain Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Domain Dashboard for $exactProgramCollectionName")
              createObservationDomainDashboard(exactProgramCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationDomainTable, entityType, category)
            }
          }
          if (isRubric == "true") {
            if (adminObservationQuestionDashboardPresent == "Yes") {
              println(s"=====> Observation Question Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Question Dashboard for $exactProgramCollectionName")
              createObservationQuestionDashboard(exactProgramCollectionId, dashboardId, tabIdMap, exactProgramCollectionName, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable, entityType, category)
            }
          } else {
            if (adminObservationWithoutRubricDashboardPresent == "Yes") {
              println(s"=====> Observation Without Rubric Question Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Without Rubric Question Dashboard for $exactProgramCollectionName")
              createObservationWithoutRubricQuestionDashboard(exactProgramCollectionId, dashboardId, tabIdMap, exactProgramCollectionName, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, ObservationQuestionTable, entityType, category)
            }
          }
        }

        def createProgramDashboards(exactProgramCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], exactProgramCollectionName: String, category: String): Unit = {
          if (programObservationStatusDashboardPresent == "Yes") {
            println(s"=====> Observation Status Dashboard is already present for $exactProgramCollectionName")
          } else {
            println(s"=====> Creating Observation Status Dashboard for $exactProgramCollectionName")
            createObservationStatusDashboard(exactProgramCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationStatusTable, entityType, category)
          }
          if (isRubric == "true") {
            if (programObservationDomainDashboardPresent == "Yes") {
              println(s"=====> Observation Domain Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Domain Dashboard for $exactProgramCollectionName")
              createObservationDomainDashboard(exactProgramCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationDomainTable, entityType, category)
            }
          }
          if (isRubric == "true") {
            if (programObservationQuestionDashboardPresent == "Yes") {
              println(s"=====> Observation Question Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Question Dashboard for $exactProgramCollectionName")
              createObservationQuestionDashboard(exactProgramCollectionId, dashboardId, tabIdMap, exactProgramCollectionName, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable, entityType, category)
            }
          } else {
            if (programObservationWithoutRubricDashboardPresent == "Yes") {
              println(s"=====> Observation Without Rubric Question Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Without Rubric Question Dashboard for $exactProgramCollectionName")
              createObservationWithoutRubricQuestionDashboard(exactProgramCollectionId, dashboardId, tabIdMap, exactProgramCollectionName, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, ObservationQuestionTable, entityType, category)
            }
          }
        }

        def createAdminCollection: Int = {
          val (adminCollectionName, adminCollectionDescription) = ("Programs", "Program Collection which contains all the sub-collections and questions")
          val groupName: String = s"Report_Admin"
          val adminCollectionId: Int = Utils.createCollection(adminCollectionName, adminCollectionDescription, metabaseUtil)
          if (adminCollectionId != -1) {
            val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", adminCollectionId).put("collectionName", adminCollectionName))
            postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb WHERE entity_id = '1';")
            CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, adminCollectionId)
            adminCollectionId
          } else {
            println(s"Admin Collection returned -1")
            -1
          }
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

        def createSolutionCollectionInsideProgram(programCollectionId: Int, solutionCollectionName: String, solutionCollectionDescription: String): Int = {
          val solutionCollectionId = Utils.createCollection(solutionCollectionName, solutionCollectionDescription, metabaseUtil, Some(programCollectionId))
          val solutionMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", solutionCollectionId).put("collectionName", solutionCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$solutionMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '1';")
          solutionCollectionId
        }

        def createProgramCollectionInsideAdmin(adminCollectionId: Int, ProgramCollectionDescription: String, ProgramCollectionName: String): Int = {
          val ProgramCollectionId = Utils.createCollection(ProgramCollectionName, ProgramCollectionDescription, metabaseUtil, Some(adminCollectionId))
          val ProgramMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", ProgramCollectionId).put("collectionName", ProgramCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$ProgramMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '1';")
          ProgramCollectionId
        }

        /**
         * Logic to process and create Program Dashboard
         */
        if (programName != null && programName.nonEmpty && solutionName != null && solutionName.nonEmpty) {
          println(s"===========================================================================================================================================")
          println("********************************************** Start Observation Program Dashboard Processing **********************************************")
          println(s"===========================================================================================================================================")
          val programCollectionName = s"$programName [org : $orgName]"
          val programCollectionCheckQuery = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"$programCollectionName"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId'"""
          val programCollectionPresent = postgresUtil.fetchData(programCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
          val programCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = '$programCollectionName' AND entity_id = '$targetedProgramId';"
          val programCollectionId = postgresUtil.executeQuery[Int](programCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
          if (programCollectionPresent == "Yes") {
            val solutionCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"$solutionName [Observation]"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId';"""
            val solutionCollectionPresent = postgresUtil.fetchData(solutionCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
            val solutionCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = '$solutionName [Observation]' AND entity_id = '$targetedProgramId';"
            val solutionCollectionId = postgresUtil.executeQuery[Int](solutionCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
            if (solutionCollectionPresent == "Yes") {
              println(s"=====> Program and Solution collection is present, checking if Dashboard is already present for $solutionName [Observation] ......")
              val dashboardId: Int = Utils.createDashboard(solutionCollectionId, s"Observation Dashboard", dashboardDescription, metabaseUtil)
              val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
              createProgramDashboards(solutionCollectionId, dashboardId, tabIdMap, programCollectionName, "Program")
              createObservationTableDashboard(solutionCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, ObservationStatusTable, ObservationDomainTable, ObservationQuestionTable, entityType, "Program", isRubric)
            } else {
              println(s"=====> Program collection is present but Solution collection is not present creating Solution collection & dashboard for $solutionName [Observation]")
              val solutionCollectionId: Int = createSolutionCollectionInsideProgram(programCollectionId, s"$solutionName [Observation]", solutionDescription)
              val dashboardId: Int = Utils.createDashboard(solutionCollectionId, s"Observation Dashboard", dashboardDescription, metabaseUtil)
              val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
              createProgramDashboards(solutionCollectionId, dashboardId, tabIdMap, programCollectionName, "Program")
              createObservationTableDashboard(solutionCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, ObservationStatusTable, ObservationDomainTable, ObservationQuestionTable, entityType, "Program", isRubric)
            }
          } else {
            println("=====> Program collection is not present creating Program, Observation Collection then Solution collection & dashboard")
            val programCollectionName = s"$programName [org : $orgName]"
            val programCollectionId = createProgramCollection(programCollectionName, programDescriptionAdd)
            val solutionCollectionId: Int = createSolutionCollectionInsideProgram(programCollectionId, s"$solutionName [Observation]", solutionDescription)
            val dashboardId: Int = Utils.createDashboard(solutionCollectionId,s"Observation Dashboard", dashboardDescription, metabaseUtil)
            val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
            createProgramDashboards(solutionCollectionId, dashboardId, tabIdMap, programCollectionName, "Program")
            createObservationTableDashboard(solutionCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, ObservationStatusTable, ObservationDomainTable, ObservationQuestionTable, entityType, "Program", isRubric)
          }
        } else {
          println(s"=====> Program name or Solution name is null, skipping the processing")
        }
        println("****************************** End Observation Program Dashboard Processing ********************************")
    }
  }

  /**
   * Logic for Observation Question Dashboard
   */
  def createObservationQuestionDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, evidenceBaseUrl: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String, observationDomainTable: String, entityType: String, category: String): Int = {
    try {
      val dashboardName: String = s"Question Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      if (parentCollectionId != -1) {
        val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId != -1) {
          metabaseUtil.syncDatabaseAndRescanValues(databaseId)
          val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "parent_one_name", postgresUtil, createDashboardQuery)
          val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "parent_two_name", postgresUtil, createDashboardQuery)
          val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Parameter'"
          val (params, diffLevelDict, entityColumnName) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationQuestionTable, postgresUtil, createDashboardQuery)
          val entityColumnId = if (entityColumnName.endsWith("_name")) {
            entityColumnName.replaceAll("_name$", "_id")
          } else {
            entityColumnName
          }
          metabaseUtil.updateColumnCategory(statenameId, "State")
          metabaseUtil.updateColumnCategory(districtnameId, "City")
          val domainReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Question-Domain';"
          UpdateQuestionDomainJsonFiles.ProcessAndUpdateJsonFiles(domainReportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, observationDomainTable, observationQuestionTable, metabaseUtil, postgresUtil, params, diffLevelDict, entityColumnName, entityColumnId, entityType)
          val questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(parentCollectionId, databaseId, dashboardId, tabId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig, params, diffLevelDict, evidenceBaseUrl)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("category", category))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
        } else {
          println("Database Id returned -1")
          -1
        }
      } else {
        println("Solution Collection Id returned -1")
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

  def createObservationWithoutRubricQuestionDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, evidenceBaseUrl: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String, entityType: String, category: String): Int = {
    try {
      val dashboardName: String = s"Question Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      if (parentCollectionId != -1) {
        val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
        println(s"Tab ID: $tabId")
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId != -1) {
          metabaseUtil.syncDatabaseAndRescanValues(databaseId)
          val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Without-Rubric-Parameter'"
          val (params, diffLevelDict, entityColumnName) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationQuestionTable, postgresUtil, createDashboardQuery)
          val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "parent_one_name", postgresUtil, createDashboardQuery)
          val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "parent_two_name", postgresUtil, createDashboardQuery)
          metabaseUtil.updateColumnCategory(statenameId, "State")
          metabaseUtil.updateColumnCategory(districtnameId, "City")
          val questionCardIdList = UpdateWithoutRubricQuestionJsonFiles.ProcessAndUpdateJsonFiles(parentCollectionId, databaseId, dashboardId, tabId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig, params, diffLevelDict, evidenceBaseUrl)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("category", category))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
        } else {
          println("Database Id returned -1")
          -1
        }
      } else {
        println("Solution Collection Id returned -1")
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

  def createObservationStatusDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], metaDataTable: String, reportConfig: String, metabaseDatabase: String, solutionName: String, targetedProgramId: String, targetedSolutionId: String, observationStatusTable: String, entityType: String, category: String): Unit = {
    try {
      val dashboardName: String = s"Status Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
      println(s"Tab ID: $tabId")
      val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
      if (databaseId != -1) {
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Status-Parameter'"
        val (params, diffLevelDict, entityColumnName) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationStatusTable, postgresUtil, createDashboardQuery)
        val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "parent_one_name", postgresUtil, createDashboardQuery)
        val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "parent_two_name", postgresUtil, createDashboardQuery)
        metabaseUtil.updateColumnCategory(statenameId, "State")
        metabaseUtil.updateColumnCategory(districtnameId, "City")
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Status' AND report_name = 'Status-Report';"
        val replacements: Map[String, String] = Map(
          "${statusTable}" -> s""""${observationStatusTable}"""",
        )
        val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, metabaseUtil, postgresUtil, params, replacements, diffLevelDict, entityType)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("category", category))
        postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
      }
    }
    catch {
      case e: Exception =>
        postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  def createObservationDomainDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], metaDataTable: String, reportConfig: String, metabaseDatabase: String, solutionName: String, targetedProgramId: String, targetedSolutionId: String, observationDomainTable: String, entityType: String, category: String): Unit = {
    try {
      val dashboardName: String = s"Domain Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
      val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
      if (databaseId != -1) {
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Domain-Parameter'"
        val (params, diffLevelDict, entityColumnName) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationDomainTable, postgresUtil, createDashboardQuery)
        val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "parent_one_name", postgresUtil, createDashboardQuery)
        val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "parent_two_name", postgresUtil, createDashboardQuery)
        val entityColumnId = if (entityColumnName.endsWith("_name")) {
          entityColumnName.replaceAll("_name$", "_id")
        } else {
          entityColumnName
        }
        metabaseUtil.updateColumnCategory(statenameId, "State")
        metabaseUtil.updateColumnCategory(districtnameId, "City")
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Domain' AND report_name = 'Domain-Report';"
        val replacements: Map[String, String] = Map(
          "${domainTable}" -> s""""${observationDomainTable}"""",
          "${entityColumnName}" -> s"""$entityColumnName""",
          "${entityColumnId}" -> s"""$entityColumnId""",
          "${entityType}" -> s"""$entityType"""
        )
        val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, metabaseUtil, postgresUtil, params, replacements, diffLevelDict, entityType)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString).put("category", category))
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil, diffLevelDict)
        postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
      }
    }
    catch {
      case e: Exception =>
        postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    }
  }


  def createObservationTableDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], metaDataTable: String, reportConfig: String, metabaseDatabase: String, evidenceBaseUrl: String, targetedProgramId: String, targetedSolutionId: String, observationStatusTable: String, observationDomainTable: String, observationQuestionTable: String, entityType: String, category: String, isRubric: String): Unit = {
    try {
      val statusCsvTab: String = s"Status Raw Data for CSV Downloads"
      val domainCsvTab: String = s"Domain and Criteria Raw Data for CSV Downloads"
      val questionCsvTab: String = s"Question Raw Data for CSV Downloads"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val reportConfigQuery1: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Status' AND report_name = 'Status-Report' AND question_type = 'table';"
      val reportConfigQuery2: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Domain' AND report_name = 'Domain-Report' AND question_type = 'table';"
      val reportConfigQuery3: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Question' AND report_name = 'Question-Report' AND question_type = 'table';"
      val parametersQuery1: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Status-Parameter'"
      val parametersQuery2: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Domain-Parameter'"
      val parametersQuery3: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Parameter'"
      val replacements: Map[String, String] = Map(
        "${statusTable}" -> s""""$observationStatusTable"""",
        "${domainTable}" -> s""""$observationDomainTable"""",
        "${questionTable}" -> s""""$observationQuestionTable"""",
        "${evidenceBaseUrl}" -> s"""'$evidenceBaseUrl'"""
      )
      println(s"==========> Started creating Observation Status Table Dashboard <==========")
      TableBasedDashboardCreationCommonSteps(tabIdMap, statusCsvTab, dashboardId, parametersQuery1, reportConfigQuery1, replacements, observationStatusTable)
      if (isRubric == "true") {
        println(s"==========> Started creating Observation Domain Table Dashboard <==========")
        TableBasedDashboardCreationCommonSteps(tabIdMap, domainCsvTab, dashboardId, parametersQuery2, reportConfigQuery2, replacements, observationDomainTable)
      } else {
        println(s"==========> Skipping Observation Domain Table Dashboard creation as isRubric is false <==========")
      }
      println(s"==========> Started creating Observation Question Table Dashboard <==========")
      TableBasedDashboardCreationCommonSteps(tabIdMap, questionCsvTab, dashboardId, parametersQuery3, reportConfigQuery3, replacements, observationQuestionTable)

      /**
       * Common steps for creating table based dashboards
       */
      def TableBasedDashboardCreationCommonSteps(tabIdMap: Map[String, Int], dashboardName: String, dashboardId: Int, parametersQuery: String, reportConfigQuery: String, replacements: Map[String, String], observationTable: String): Unit = {
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val (params, diffLevelDict, entityColumnName) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationTable, postgresUtil, createDashboardQuery)
        val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "parent_one_name", postgresUtil, createDashboardQuery)
        val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "parent_two_name", postgresUtil, createDashboardQuery)
        metabaseUtil.updateColumnCategory(statenameId, "State")
        metabaseUtil.updateColumnCategory(districtnameId, "City")
        val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, metabaseUtil, postgresUtil, params, replacements, diffLevelDict, entityType)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("dashboardId", dashboardId).put("questionIds", questionIdsString).put("category", category))
        postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
      }
    }
    catch {
      case e: Exception =>
        postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  def extractParameterDicts(parametersQuery: String, entityType: String, databaseId: Int, metabaseUtil: MetabaseUtil, tableName: String, postgresUtil: PostgresUtil, createDashboardQuery: String): (Map[String, Int], ListMap[String, String], String) = {
    val DashboardParameter = postgresUtil.fetchData(parametersQuery) match {
      case List(map: Map[_, _]) => map.get("config").map(_.toString).getOrElse("")
      case _ => ""
    }
    val mapper = new ObjectMapper()
    val arrayNode = mapper.readTree(DashboardParameter)
    val removeParamsAfterThis: String = arrayNode.elements().asScala
      .find(node => Option(node.get("entity_type")).exists(_.asText() == entityType))
      .flatMap(node => Option(node.get("param")).map(_.asText()))
      .getOrElse("")
    val completeMapOfParamAndColumnName: ListMap[String, String] = ListMap(
      arrayNode.elements().asScala
        .map(node => node.get("param").asText() -> node.get("columnName").asText())
        .toSeq: _*
    )

    val entityColumnName: String = completeMapOfParamAndColumnName.getOrElse(removeParamsAfterThis, "")

    def getTheMapOfParamsAfterRemovingTheEntityParam(completeMapOfParamAndColumnName: ListMap[String, String], removeParamsAfterThis: String): ListMap[String, String] = {
      val keys = completeMapOfParamAndColumnName.keys.toList
      val idx = keys.indexOf(removeParamsAfterThis)
      if (idx == -1) completeMapOfParamAndColumnName
      else ListMap(keys.drop(idx).map(k => k -> completeMapOfParamAndColumnName(k)): _*)
    }

    val mapOfParamsAfterRemovingBelowEntityParams = getTheMapOfParamsAfterRemovingTheEntityParam(completeMapOfParamAndColumnName, removeParamsAfterThis)
    val params: Map[String, Int] = mapOfParamsAfterRemovingBelowEntityParams.map { case (key, columnName) =>
      key -> GetTableData.getTableMetadataId(databaseId, metabaseUtil, tableName, columnName, postgresUtil, createDashboardQuery)
    }
    val mapOfRemovedParams: ListMap[String, String] =
      if (mapOfParamsAfterRemovingBelowEntityParams.isEmpty) ListMap.empty
      else completeMapOfParamAndColumnName.filterNot { case (k, v) => mapOfParamsAfterRemovingBelowEntityParams.contains(k) && mapOfParamsAfterRemovingBelowEntityParams(k) == v }
    (params, mapOfRemovedParams, entityColumnName)
  }

  println(s"***************** End of Processing the Metabase Observation Dashboard *****************\n")
}
