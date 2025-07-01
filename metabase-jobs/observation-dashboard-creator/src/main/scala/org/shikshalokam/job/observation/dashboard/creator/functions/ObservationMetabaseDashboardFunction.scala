package org.shikshalokam.job.observation.dashboard.creator.functions

import com.fasterxml.jackson.databind.ObjectMapper
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

    println(s"entityType: $entityType")
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

          val adminCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Admin Collection"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '1';"""
          val adminCollectionPresent = postgresUtil.fetchData(adminCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
          val adminCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Admin Collection' AND entity_id = '1';"
          val adminCollectionId = postgresUtil.executeQuery[Int](adminCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
          if (adminCollectionPresent == "Yes") {
            val observationCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Observation Collection"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '1';"""
            val observationCollectionPresent = postgresUtil.fetchData(observationCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
            val observationCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Observation Collection' AND entity_id = '1';"
            val observationCollectionId = postgresUtil.executeQuery[Int](observationCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
            if (observationCollectionPresent == "Yes") {
              println("=====> Admin and Observation collection present creating Solution collection & dashboard")
              val solutionCollectionName: String = s"$solutionName [Admin]"
              val parentCollectionId: Int = if (isRubric == "true") {
                createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable, entityType)
              } else {
                createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, entityType)
              }
              createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable, entityType)
              if (isRubric == "true") {
                createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable, entityType)
              }
            } else {
              println("=====> Only Admin collection is present creating Observation Collection then Solution collection & dashboard")
              val observationCollectionId = createObservationCollectionInsideAdmin(adminCollectionId)
              if (observationCollectionId != -1) {
                val solutionCollectionName: String = s"$solutionName [Admin]"
                val parentCollectionId: Int = if (isRubric == "true") {
                  createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable, entityType)
                } else {
                  createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, entityType)
                }
                createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable, entityType)
                if (isRubric == "true") {
                  createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable, entityType)
                }
              }
            }
          } else {
            println("=====> Admin collection is not present creating Admin, Observation Collection then Solution collection & dashboard")
            val adminCollectionId = createAdminCollection
            if (adminCollectionId != -1) {
              val observationCollectionId = createObservationCollectionInsideAdmin(adminCollectionId)
              if (observationCollectionId != -1) {
                val solutionCollectionName: String = s"$solutionName [Admin]"
                val parentCollectionId: Int = if (isRubric == "true") {
                  createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable, entityType)
                } else {
                  createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, entityType)
                }
                createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable, entityType)
                if (isRubric == "true") {
                  createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable, entityType)
                }
              }
            }
          }
        } else {
          println(s"=====> Solution name is null, skipping the processing")
        }

        def createAdminCollection: Int = {
          val (adminCollectionName, adminCollectionDescription) = ("Admin Collection", "Admin Collection which contains all the sub-collections and questions")
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

        def createProgramCollection(programName: String): Int = {
          val (programCollectionName, programCollectionDescription) = (s"Program Collection [$programName]", "Program Collection which contains all the sub-collections and questions")
          val groupName: String = s"Program_Manager[$programName]"
          val programCollectionId: Int = Utils.checkAndCreateCollection(programCollectionName, programCollectionDescription, metabaseUtil)
          if (programCollectionId != -1) {
            val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName))
            postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJson' ::jsonb WHERE entity_id = '$targetedProgramId';")
            CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, programCollectionId)
            programCollectionId
          } else {
            println(s"Program Collection [$programName] returned -1")
            -1
          }
        }

        def createObservationCollectionInsideAdmin(adminCollectionId: Int): Int = {
          val (observationCollectionName, observationCollectionDescription) = ("Observation Collection", "This collection contains sub-collection, questions and dashboards required for Observation")
          val observationCollectionId = Utils.createCollection(observationCollectionName, observationCollectionDescription, metabaseUtil, Some(adminCollectionId))
          val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", observationCollectionId).put("collectionName", observationCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '1';")
          observationCollectionId
        }

        def createObservationCollectionInsideProgram(adminCollectionId: Int): Int = {
          val (observationCollectionName, observationCollectionDescription) = ("Observation Collection", "This collection contains sub-collection, questions and dashboards required for Observation")
          val observationCollectionId = Utils.createCollection(observationCollectionName, observationCollectionDescription, metabaseUtil, Some(adminCollectionId))
          val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", observationCollectionId).put("collectionName", observationCollectionName))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedProgramId';")
          observationCollectionId
        }

        /**
         * Logic to process and create Program Dashboard
         */
        if (programName != null && programName.nonEmpty && solutionName != null && solutionName.nonEmpty) {
          println(s"===========================================================================================================================================")
          println("********************************************** Start Observation Program Dashboard Processing **********************************************")
          println(s"===========================================================================================================================================")
          val programCollectionCheckQuery = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Program Collection [$programName]"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId'"""
          val programCollectionPresent = postgresUtil.fetchData(programCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
          val programCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Program Collection [$programName]' AND entity_id = '$targetedProgramId';"
          val programCollectionId = postgresUtil.executeQuery[Int](programCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
          if (programCollectionPresent == "Yes") {
            val observationCollectionCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Observation Collection"}]'::jsonb THEN 'Yes' ELSE 'No' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId';"""
            val observationCollectionPresent = postgresUtil.fetchData(observationCollectionCheckQuery).collectFirst { case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("") }.getOrElse("")
            val observationCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Observation Collection' AND entity_id = '$targetedProgramId';"
            val observationCollectionId = postgresUtil.executeQuery[Int](observationCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
            if (observationCollectionPresent == "Yes") {
              println("=====> Program and Observation collection present creating Solution collection & dashboard")
              val solutionCollectionName: String = s"$solutionName [Program]"
              val parentCollectionId: Int = if (isRubric == "true") {
                createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable, entityType)
              } else {
                createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, entityType)
              }
              createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable, entityType)
              if (isRubric == "true") {
                createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable, entityType)
              }
            } else {
              println("=====> Only Program collection is present creating Observation Collection then Solution collection & dashboard")
              val observationCollectionId = createObservationCollectionInsideProgram(programCollectionId)
              if (observationCollectionId != -1) {
                val solutionCollectionName: String = s"$solutionName [Program]"
                val parentCollectionId: Int = if (isRubric == "true") {
                  createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable, entityType)
                } else {
                  createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, entityType)
                }
                createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable, entityType)
                if (isRubric == "true") {
                  createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable, entityType)
                }
              }
            }
          } else {
            println("=====> Program collection is not present creating Program, Observation Collection then Solution collection & dashboard")
            val programCollectionId = createProgramCollection(programName)
            if (programCollectionId != -1) {
              val observationCollectionId = createObservationCollectionInsideAdmin(programCollectionId)
              if (observationCollectionId != -1) {
                val solutionCollectionName: String = s"$solutionName [Program]"
                val parentCollectionId: Int = if (isRubric == "true") {
                  createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable, entityType)
                } else {
                  createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, entityType)
                }
                createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable, entityType)
                if (isRubric == "true") {
                  createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable, entityType)
                }
              }
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
  def createObservationQuestionDashboard(parentCollectionId: Int, collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String, observationDomainTable: String, entityType: String): Int = {
    try {
      println(s"==========> started creating Observation Question Dashboard with Rubric")
      val dashboardName: String = s"Observation Question Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val collectionId: Int = Utils.checkAndCreateCollection(collectionName, s"Solution Collection which contains all the dashboards and questions", metabaseUtil, Some(parentCollectionId))
      if (collectionId != -1) {
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil)
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId != -1) {
          metabaseUtil.syncDatabaseAndRescanValues(databaseId)
          val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Parameter'"
          val (params, diffLevelDict, entityColumnName) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationQuestionTable, postgresUtil, createDashboardQuery)
          val entityColumnId = if (entityColumnName.endsWith("_name")) {
            entityColumnName.replaceAll("_name$", "_id")
          } else {
            entityColumnName
          }
          val stateNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "parent_one_name", postgresUtil, createDashboardQuery)
          val districtNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "parent_two_name", postgresUtil, createDashboardQuery)
          metabaseUtil.updateColumnCategory(stateNameId, "State")
          metabaseUtil.updateColumnCategory(districtNameId, "City")
          val domainReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Question-Domain';"
          UpdateQuestionDomainJsonFiles.ProcessAndUpdateJsonFiles(domainReportConfigQuery, collectionId, databaseId, dashboardId, observationDomainTable, observationQuestionTable, metabaseUtil, postgresUtil, params, diffLevelDict, entityColumnName, entityColumnId, entityType)
          val questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(collectionId, databaseId, dashboardId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig, params, diffLevelDict)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil, diffLevelDict)
          val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
          collectionId
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

  def createObservationWithoutRubricQuestionDashboard(parentCollectionId: Int, collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String, entityType: String): Int = {
    try {
      println(s"==========> started creating Observation Question Dashboard without Rubric")
      val dashboardName: String = s"Observation Question Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val collectionId: Int = Utils.checkAndCreateCollection(collectionName, s"Solution Collection which contains all the dashboards and questions", metabaseUtil, Some(parentCollectionId))
      if (collectionId != -1) {
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil)
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId != -1) {
          metabaseUtil.syncDatabaseAndRescanValues(databaseId)
          val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Without-Rubric-Parameter'"
          val (params, diffLevelDict, entityColumnName) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationQuestionTable, postgresUtil, createDashboardQuery)
          val stateNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "parent_one_name", postgresUtil, createDashboardQuery)
          val districtNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "parent_two_name", postgresUtil, createDashboardQuery)
          metabaseUtil.updateColumnCategory(stateNameId, "State")
          metabaseUtil.updateColumnCategory(districtNameId, "City")
          val questionCardIdList = UpdateWithoutRubricQuestionJsonFiles.ProcessAndUpdateJsonFiles(collectionId, databaseId, dashboardId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig, params, diffLevelDict)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil, diffLevelDict)
          val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
          collectionId
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


  def createObservationStatusDashboard(parentCollectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationStatusTable: String, entityType: String): Unit = {
    try {
      println(s"======> started creating Observation Status Dashboard")
      val dashboardName: String = s"Observation Status Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val dashboardId: Int = Utils.createDashboard(parentCollectionId, dashboardName, metabaseUtil, postgresUtil)
      val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
      if (databaseId != -1) {
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Status-Parameter'"
        val (params, diffLevelDict, entityColumnName) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationStatusTable, postgresUtil, createDashboardQuery)
        val stateNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "parent_one_name", postgresUtil, createDashboardQuery)
        val districtNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "parent_two_name", postgresUtil, createDashboardQuery)
        metabaseUtil.updateColumnCategory(stateNameId, "State")
        metabaseUtil.updateColumnCategory(districtNameId, "City")
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Status' AND report_name = 'Status-Report';"
        val replacements: Map[String, String] = Map(
          "${statusTable}" -> s""""$observationStatusTable""""
        )
        val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, metabaseUtil, postgresUtil, params, replacements, diffLevelDict, entityType)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil, diffLevelDict)
        val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString))
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


  def createObservationDomainDashboard(parentCollectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationDomainTable: String, entityType: String): Unit = {
    try {
      println(s"======> started creating Observation Domain Dashboard")
      val dashboardName: String = s"Observation Domain Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val dashboardId: Int = Utils.createDashboard(parentCollectionId, dashboardName, metabaseUtil, postgresUtil)
      val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
      if (databaseId != -1) {
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Domain-Parameter'"
        val (params, diffLevelDict, entityColumnName) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationDomainTable, postgresUtil, createDashboardQuery)
        val entityColumnId = if (entityColumnName.endsWith("_name")) {
          entityColumnName.replaceAll("_name$", "_id")
        } else {
          entityColumnName
        }
        val stateNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "parent_one_name", postgresUtil, createDashboardQuery)
        val districtNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "parent_two_name", postgresUtil, createDashboardQuery)
        metabaseUtil.updateColumnCategory(stateNameId, "State")
        metabaseUtil.updateColumnCategory(districtNameId, "City")
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Domain' AND report_name = 'Domain-Report';"
        val replacements: Map[String, String] = Map(
          "${domainTable}" -> s""""$observationDomainTable"""",
          "${entityColumnName}" -> s"""$entityColumnName""",
          "${entityColumnId}" -> s"""$entityColumnId""",
          "${entityType}" -> s"""$entityType"""
        )
        val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, metabaseUtil, postgresUtil, params, replacements, diffLevelDict, entityType)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil, diffLevelDict)
        val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString))
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
