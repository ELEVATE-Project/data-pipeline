package org.shikshalokam.job.observation.dashboard.creator.functions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import scala.collection.JavaConverters._
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
    val programName = postgresUtil.fetchData(s"""SELECT entity_name from $metaDataTable where entity_id = '$targetedProgramId'""").collectFirst { case map: Map[_, _] => map.get("entity_name").map(_.toString).getOrElse("") }.getOrElse("")

    val programNameQuery = s"SELECT entity_name from $metaDataTable where entity_id = '$targetedProgramId'"
    val programName = postgresUtil.fetchData(programNameQuery) match {
      case List(map: Map[_, _]) => map.get("entity_name").map(_.toString).getOrElse("")
      case _ => ""
    }

    val programDescriptionQuery = s"SELECT program_description from $solutionTable where solution_id = '$targetedSolutionId'"
    val programDescription = postgresUtil.fetchData(programDescriptionQuery) match {
      case List(map: Map[_, _]) => map.get("program_description").map(_.toString).getOrElse("")
      case _ => ""
    }

    val solutionMetadataQuery = s"SELECT main_metadata FROM $metaDataTable WHERE entity_id = '$targetedSolutionId';"
    val solutionMetadataOutput = postgresUtil.fetchData(solutionMetadataQuery).collectFirst {
      case map: Map[_, _] => map.get("main_metadata").map(_.toString)
    }.flatten

    val objectMapper = new ObjectMapper()

    def dashboardExists(solutionMetadataOutput: String, dashboardName: String, category: String): String = {
      val node: JsonNode = objectMapper.readTree(solutionMetadataOutput)
      if (node.elements().asScala.exists { arrElem: JsonNode =>
        arrElem.has("dashboardName") && arrElem.has("category") &&
          arrElem.get("dashboardName").asText() == dashboardName &&
          arrElem.get("category").asText() == category
      }) "Yes" else "No"
    }

    val adminObservationQuestionDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Domain And Question Report [$solutionName - $targetedSolutionId]", "Admin")).getOrElse("No")
    val adminObservationWithoutRubricDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Question Report [$solutionName - $targetedSolutionId]", "Admin")).getOrElse("No")
    val adminObservationStatusDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Status Report [$solutionName - $targetedSolutionId]", "Admin")).getOrElse("No")
    val adminObservationDomainDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Domain Report [$solutionName - $targetedSolutionId]", "Admin")).getOrElse("No")

    val programObservationQuestionDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Domain And Question Report [$solutionName - $targetedSolutionId]", "Program")).getOrElse("No")
    val programObservationWithoutRubricDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Question Report [$solutionName - $targetedSolutionId]", "Program")).getOrElse("No")
    val programObservationStatusDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Status Report [$solutionName - $targetedSolutionId]", "Program")).getOrElse("No")
    val programObservationDomainDashboardPresent = solutionMetadataOutput.map(dashboardExists(_, s"Domain Report [$solutionName - $targetedSolutionId]", "Program")).getOrElse("No")


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

          val adminCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Admin"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '1';"""
          val adminCollectionPresent = postgresUtil.fetchData(adminCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
          val adminCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Admin' AND entity_id = '1';"
          val adminCollectionId = postgresUtil.executeQuery[Int](adminCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
          println("=====> checking Admin collection presence ......")
          if (adminCollectionPresent == "Yes") {
            println("=====> Admin collection is present, checking Program collection presence ......")
            val programCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Program"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '1';"""
            val programCollectionPresent = postgresUtil.fetchData(programCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
            val programCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Program' AND entity_id = '1';"
            val programCollectionId = postgresUtil.executeQuery[Int](programCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
            if (programCollectionPresent == "Yes") {
              println(s"=====> Admin and Program collection present, checking main program collection presence for $programName [$targetedProgramId] ......")
              val exactProgramCollectionName = s"$programName [$targetedProgramId]"
              val exactProgramCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"$programName [$targetedProgramId]"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '1';"""
              val exactProgramCollectionPresent = postgresUtil.fetchData(exactProgramCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
              val exactProgramCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = '$programName [$targetedProgramId]' AND entity_id = '1';"
              val exactProgramCollectionId = postgresUtil.executeQuery[Int](exactProgramCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
              if (exactProgramCollectionPresent == "Yes") {
                createAdminDashboards(exactProgramCollectionId, exactProgramCollectionName, "Admin")
              } else {
                println(s"=====> Only Admin and Program collection is present creating [$programName] Program Collection then Solution collection & dashboard")
                val exactProgramCollectionName = s"$programName [$targetedProgramId]"
                val exactProgramCollectionId = createProgramCollectionInsideProgram(programCollectionId, programDescription, exactProgramCollectionName)
                if (exactProgramCollectionId != -1) {
                  createAdminDashboards(exactProgramCollectionId, exactProgramCollectionName, "Admin")
                }
              }
            } else {
              println("=====> Only Admin collection is present creating Program Collection then Solution collection & dashboard")
              val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId)
              if (programCollectionId != -1) {
                val exactProgramCollectionName = s"$programName [$targetedProgramId]"
                val exactProgramCollectionId = createProgramCollectionInsideProgram(programCollectionId, programDescription, exactProgramCollectionName)
                if (exactProgramCollectionId != -1) {
                  createAdminDashboards(exactProgramCollectionId, exactProgramCollectionName, "Admin")
                }
              }
            }
          } else {
            println("=====> Admin collection is not present creating Admin, Observation Collection then Solution collection & dashboard")
            val adminCollectionId = createAdminCollection
            if (adminCollectionId != -1) {
              val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId)
              if (programCollectionId != -1) {
                val exactProgramCollectionName = s"$programName [$targetedProgramId]"
                val exactProgramCollectionId = createProgramCollectionInsideProgram(programCollectionId, programDescription, exactProgramCollectionName)
                if (exactProgramCollectionId != -1) {
                  createAdminDashboards(exactProgramCollectionId, exactProgramCollectionName, "Admin")
                }
              }
            }
          }
        } else {
          println(s"=====> Solution name is null, skipping the processing")
        }

        def createAdminDashboards(exactProgramCollectionId: Int, exactProgramCollectionName: String, category: String): Unit = {
          if (isRubric == "true") {
            if (adminObservationQuestionDashboardPresent == "Yes") {
              println(s"=====> Observation Question Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Question Dashboard for $exactProgramCollectionName")
              createObservationQuestionDashboard(exactProgramCollectionId, exactProgramCollectionName, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable, category)
            }
          } else {
            if (adminObservationWithoutRubricDashboardPresent == "Yes") {
              println(s"=====> Observation Without Rubric Question Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Without Rubric Question Dashboard for $exactProgramCollectionName")
              createObservationWithoutRubricQuestionDashboard(exactProgramCollectionId, exactProgramCollectionName, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationQuestionTable, category)
            }
          }
          if (adminObservationStatusDashboardPresent == "Yes") {
            println(s"=====> Observation Status Dashboard is already present for $exactProgramCollectionName")
          } else {
            println(s"=====> Creating Observation Status Dashboard for $exactProgramCollectionName")
            createObservationStatusDashboard(exactProgramCollectionId, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationStatusTable, category)
          }
          if (isRubric == "true") {
            if (adminObservationDomainDashboardPresent == "Yes") {
              println(s"=====> Observation Domain Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Domain Dashboard for $exactProgramCollectionName")
              createObservationDomainDashboard(exactProgramCollectionId, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationDomainTable, category)
            }
          }
        }

        def createProgramDashboards(exactProgramCollectionId: Int, exactProgramCollectionName: String, category: String): Unit = {
          if (isRubric == "true") {
            if (programObservationQuestionDashboardPresent == "Yes") {
              println(s"=====> Observation Question Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Question Dashboard for $exactProgramCollectionName")
              createObservationQuestionDashboard(exactProgramCollectionId, exactProgramCollectionName, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable, category)
            }
          } else {
            if (programObservationWithoutRubricDashboardPresent == "Yes") {
              println(s"=====> Observation Without Rubric Question Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Without Rubric Question Dashboard for $exactProgramCollectionName")
              createObservationWithoutRubricQuestionDashboard(exactProgramCollectionId, exactProgramCollectionName, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationQuestionTable, category)
            }
          }
          if (programObservationStatusDashboardPresent == "Yes") {
            println(s"=====> Observation Status Dashboard is already present for $exactProgramCollectionName")
          } else {
            println(s"=====> Creating Observation Status Dashboard for $exactProgramCollectionName")
            createObservationStatusDashboard(exactProgramCollectionId, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationStatusTable, category)
          }
          if (isRubric == "true") {
            if (programObservationDomainDashboardPresent == "Yes") {
              println(s"=====> Observation Domain Dashboard is already present for $exactProgramCollectionName")
            } else {
              println(s"=====> Creating Observation Domain Dashboard for $exactProgramCollectionName")
              createObservationDomainDashboard(exactProgramCollectionId, metaDataTable, reportConfig, metabaseDatabase, solutionName, targetedProgramId, targetedSolutionId, ObservationDomainTable, category)
            }
          }
        }

        def createAdminCollection: Int = {
          val (adminCollectionName, adminCollectionDescription) = ("Admin", "Admin Collection which contains all the sub-collections and questions")
          val groupName: String = s"Report_Admin"
          val adminCollectionId: Int = Utils.checkAndCreateCollection(adminCollectionName, adminCollectionDescription, metabaseUtil)
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
          val groupName: String = s"Program_Manager[$programName]"
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

        def createProgramCollectionInsideAdmin(adminCollectionId: Int): Int = {
          val (programCollectionName, programCollectionDescription) = ("Program", "This collection contains sub-collection, questions and dashboards required for Observation")
          val programCollectionId = Utils.createCollection(programCollectionName, programCollectionDescription, metabaseUtil, Some(adminCollectionId))
          val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '1';")
          programCollectionId
        }

        def createProgramCollectionInsideProgram(programCollectionId: Int, exactProgramCollectionDescription: String, exactProgramCollectionName: String): Int = {
          val exactProgramCollectionId = Utils.createCollection(exactProgramCollectionName, exactProgramCollectionDescription, metabaseUtil, Some(programCollectionId))
          val exactProgramMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", exactProgramCollectionId).put("collectionName", exactProgramCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$exactProgramMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '1';")
          exactProgramCollectionId
        }

        /**
         * Logic to process and create Program Dashboard
         */
        if (programName != null && programName.nonEmpty && solutionName != null && solutionName.nonEmpty) {
          println(s"===========================================================================================================================================")
          println("********************************************** Start Observation Program Dashboard Processing **********************************************")
          println(s"===========================================================================================================================================")
          val programCollectionName = s"$programName [$targetedProgramId]"
          val programCollectionCheckQuery = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"$programCollectionName"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId'"""
          val programCollectionPresent = postgresUtil.fetchData(programCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
          val programCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = '$programCollectionName' AND entity_id = '$targetedProgramId';"
          val programCollectionId = postgresUtil.executeQuery[Int](programCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
          if (programCollectionPresent == "Yes") {
            println(s"=====> Program collection is present, checking if Program Dashboard is already present for $programCollectionName ......")
            createProgramDashboards(programCollectionId, programCollectionName, "Program")
          } else {
            println("=====> Program collection is not present creating Program, Observation Collection then Solution collection & dashboard")
            val programCollectionName = s"$programName [$targetedProgramId]"
            val programCollectionId = createProgramCollection(programCollectionName, programDescription)
            if (programCollectionId != -1) {
              createProgramDashboards(programCollectionId, programCollectionName, "Program")
            } else {
              println(s"Check if $programName [program: $targetedProgramId] is already present")
            }
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
  def createObservationQuestionDashboard(parentCollectionId: Int, collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, solutionName: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String, observationDomainTable: String, category: String): Int = {
    try {
      val dashboardName: String = s"Domain And Question Report [$solutionName - $targetedSolutionId]"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      if (parentCollectionId != -1) {
        val dashboardId: Int = Utils.createDashboard(parentCollectionId, dashboardName, metabaseUtil, postgresUtil)
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId != -1) {
          metabaseUtil.syncDatabaseAndRescanValues(databaseId)
          val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "state_name", postgresUtil, createDashboardQuery)
          val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "district_name", postgresUtil, createDashboardQuery)
          val clusterId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "cluster_name", postgresUtil, createDashboardQuery)
          val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "school_name", postgresUtil, createDashboardQuery)
          val domainId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "domain_name", postgresUtil, createDashboardQuery)
          val criteriaId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "criteria_name", postgresUtil, createDashboardQuery)
          metabaseUtil.updateColumnCategory(statenameId, "State")
          metabaseUtil.updateColumnCategory(districtnameId, "City")
          val domainReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Question-Domain';"
          UpdateQuestionDomainJsonFiles.ProcessAndUpdateJsonFiles(domainReportConfigQuery, parentCollectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, domainId, criteriaId, observationDomainTable, observationQuestionTable, metabaseUtil, postgresUtil)
          val questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(parentCollectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, domainId, criteriaId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Parameter'"
          UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
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

  def createObservationWithoutRubricQuestionDashboard(parentCollectionId: Int, collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, solutionName: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String, category: String): Int = {
    try {
      val dashboardName: String = s"Question Report [$solutionName - $targetedSolutionId]"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      if (parentCollectionId != -1) {
        val dashboardId: Int = Utils.createDashboard(parentCollectionId, dashboardName, metabaseUtil, postgresUtil)
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId != -1) {
          metabaseUtil.syncDatabaseAndRescanValues(databaseId)
          val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "state_name", postgresUtil, createDashboardQuery)
          val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "district_name", postgresUtil, createDashboardQuery)
          val clusterId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "cluster_name", postgresUtil, createDashboardQuery)
          val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "school_name", postgresUtil, createDashboardQuery)
          metabaseUtil.updateColumnCategory(statenameId, "State")
          metabaseUtil.updateColumnCategory(districtnameId, "City")
          val questionCardIdList = UpdateWithoutRubricQuestionJsonFiles.ProcessAndUpdateJsonFiles(parentCollectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Without-Rubric-Parameter'"
          UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
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


  def createObservationStatusDashboard(parentCollectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, solutionName: String, targetedProgramId: String, targetedSolutionId: String, observationStatusTable: String, category: String): Unit = {
    try {
      val dashboardName: String = s"Status Report [$solutionName - $targetedSolutionId]"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val dashboardId: Int = Utils.createDashboard(parentCollectionId, dashboardName, metabaseUtil, postgresUtil)
      val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
      if (databaseId != -1) {
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Status-Parameter'"
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
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

  def createObservationDomainDashboard(parentCollectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, solutionName: String, targetedProgramId: String, targetedSolutionId: String, observationDomainTable: String, category: String): Unit = {
    try {
      val dashboardName: String = s"Domain Report [$solutionName - $targetedSolutionId]"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val dashboardId: Int = Utils.createDashboard(parentCollectionId, dashboardName, metabaseUtil, postgresUtil)
      val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
      if (databaseId != -1) {
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Domain-Parameter'"
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
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
