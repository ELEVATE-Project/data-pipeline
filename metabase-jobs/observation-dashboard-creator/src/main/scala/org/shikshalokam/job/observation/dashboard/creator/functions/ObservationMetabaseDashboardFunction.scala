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

import scala.collection.immutable._

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
    val targetedProgramId = event.targetedProgram
    val targetedSolutionId = event.targetedSolution
    val isRubric = event.isRubric
    val metaDataTable = config.dashboard_metadata
    val reportConfig: String = config.report_config
    val metabaseDatabase: String = config.metabaseDatabase
    val ObservationDomainTable = s"${targetedSolutionId}_domain"
    val ObservationQuestionTable = s"${targetedSolutionId}_questions"
    val ObservationStatusTable = s"${targetedSolutionId}_status"

    val solutionNameQuery = s"SELECT entity_name from $metaDataTable where entity_id = '$targetedSolutionId'"
    val solutionName = postgresUtil.fetchData(solutionNameQuery) match {
      case List(map: Map[_, _]) => map.get("entity_name").map(_.toString).getOrElse("")
      case _ => ""
    }

    val programNameQuery = s"SELECT entity_name from $metaDataTable where entity_id = '$targetedProgramId'"
    val programName = postgresUtil.fetchData(programNameQuery) match {
      case List(map: Map[_, _]) => map.get("entity_name").map(_.toString).getOrElse("")
      case _ => ""
    }
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
                createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable)
              } else {
                createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
              }
              createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
              if (isRubric == "true") {
                createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
              }
            } else {
              println("=====> Only Admin collection is present creating Observation Collection then Solution collection & dashboard")
              val observationCollectionId = createObservationCollectionInsideAdmin(adminCollectionId)
              if (observationCollectionId != -1) {
                val solutionCollectionName: String = s"$solutionName [Admin]"
                val parentCollectionId: Int = if (isRubric == "true") {
                  createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable)
                } else {
                  createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                }
                createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
                if (isRubric == "true") {
                  createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
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
                  createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable)
                } else {
                  createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                }
                createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
                if (isRubric == "true") {
                  createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
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
                createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable)
              } else {
                createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
              }
              createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
              if (isRubric == "true") {
                createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
              }
            } else {
              println("=====> Only Program collection is present creating Observation Collection then Solution collection & dashboard")
              val observationCollectionId = createObservationCollectionInsideProgram(programCollectionId)
              if (observationCollectionId != -1) {
                val solutionCollectionName: String = s"$solutionName [Program]"
                val parentCollectionId: Int = if (isRubric == "true") {
                  createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable)
                } else {
                  createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                }
                createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
                if (isRubric == "true") {
                  createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
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
                  createObservationQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable, ObservationDomainTable)
                } else {
                  createObservationWithoutRubricQuestionDashboard(observationCollectionId, solutionCollectionName, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                }
                createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
                if (isRubric == "true") {
                  createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
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
  def createObservationQuestionDashboard(parentCollectionId: Int, collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String, observationDomainTable: String): Int = {
    try {
      val dashboardName: String = s"Observation Question Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val collectionId: Int = Utils.checkAndCreateCollection(collectionName, s"Solution Collection which contains all the dashboards and questions", metabaseUtil, Some(parentCollectionId))
      if (collectionId != -1) {
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil)
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
          UpdateQuestionDomainJsonFiles.ProcessAndUpdateJsonFiles(domainReportConfigQuery, collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, domainId, criteriaId, observationDomainTable, observationQuestionTable, metabaseUtil, postgresUtil)
          val questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, domainId, criteriaId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Parameter'"
          UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
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

  def createObservationWithoutRubricQuestionDashboard(parentCollectionId: Int, collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String): Int = {
    try {
      val dashboardName: String = s"Observation Question Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val collectionId: Int = Utils.checkAndCreateCollection(collectionName, s"Solution Collection which contains all the dashboards and questions", metabaseUtil, Some(parentCollectionId))
      if (collectionId != -1) {
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil)
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId != -1) {
          metabaseUtil.syncDatabaseAndRescanValues(databaseId)
          val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "state_name", postgresUtil, createDashboardQuery)
          val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "district_name", postgresUtil, createDashboardQuery)
          val clusterId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "cluster_name", postgresUtil, createDashboardQuery)
          val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "school_name", postgresUtil, createDashboardQuery)
          metabaseUtil.updateColumnCategory(statenameId, "State")
          metabaseUtil.updateColumnCategory(districtnameId, "City")
          val questionCardIdList = UpdateWithoutRubricQuestionJsonFiles.ProcessAndUpdateJsonFiles(collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig)
          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
          val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Without-Rubric-Parameter'"
          UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
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


  def createObservationStatusDashboard(parentCollectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationStatusTable: String): Unit = {
    try {
      val dashboardName: String = s"Observation Status Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val dashboardId: Int = Utils.createDashboard(parentCollectionId, dashboardName, metabaseUtil, postgresUtil)
      val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
      if (databaseId != -1) {
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "state_name", postgresUtil, createDashboardQuery)
        val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "district_name", postgresUtil, createDashboardQuery)
        metabaseUtil.updateColumnCategory(statenameId, "State")
        metabaseUtil.updateColumnCategory(districtnameId, "City")
        val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "school_name", postgresUtil, createDashboardQuery)
        val clusterId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "cluster_name", postgresUtil, createDashboardQuery)
        val blockId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "block_name", postgresUtil, createDashboardQuery)
        val orgId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "org_name", postgresUtil, createDashboardQuery)
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Status' AND report_name = 'Status-Report';"
        val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, blockId, orgId, observationStatusTable, metabaseUtil, postgresUtil, reportConfig)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Status-Parameter'"
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
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

  def createObservationDomainDashboard(parentCollectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationDomainTable: String): Unit = {
    try {
      val dashboardName: String = s"Observation Domain Report"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
      val dashboardId: Int = Utils.createDashboard(parentCollectionId, dashboardName, metabaseUtil, postgresUtil)
      val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
      if (databaseId != -1) {
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "state_name", postgresUtil, createDashboardQuery)
        val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "district_name", postgresUtil, createDashboardQuery)
        metabaseUtil.updateColumnCategory(statenameId, "State")
        metabaseUtil.updateColumnCategory(districtnameId, "City")
        val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "school_name", postgresUtil, createDashboardQuery)
        val clusterId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "cluster_name", postgresUtil, createDashboardQuery)
        val domainId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "domain", postgresUtil, createDashboardQuery)
        val criteriaId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "criteria", postgresUtil, createDashboardQuery)
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Domain' AND report_name = 'Domain-Report';"
        val questionCardIdList = UpdateDomainJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, domainId, criteriaId, observationDomainTable, metabaseUtil, postgresUtil, reportConfig)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Domain-Parameter'"
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
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

  println(s"***************** End of Processing the Metabase Observation Dashboard *****************\n")
}
