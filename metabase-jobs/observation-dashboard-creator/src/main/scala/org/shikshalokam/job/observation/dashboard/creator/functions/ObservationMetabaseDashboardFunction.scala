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
    val solutions: String = config.solutions
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
    println(s"solutions: $solutions")

    event.reportType match {
      case "Observation" =>
        println(s">>>>>>>>>>> Started Processing Metabase Project Dashboards >>>>>>>>>>>>")
        if (admin.nonEmpty) {
          println(s"********** Started Processing Metabase Admin Dashboard For Observation ***********")
          val adminObservationSolutionIdCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Admin Collection"}]'::jsonb AND main_metadata::jsonb @> '[{"collectionName":"Observation Collection"}]'::jsonb AND main_metadata::jsonb @> '[{"collectionName":"Observation [$solutionName]"}]'::jsonb THEN 'Success' ELSE 'Failed' END AS result FROM $metaDataTable WHERE entity_id = '$admin';"""
          val adminObservationSolutionIdStatus = postgresUtil.fetchData(adminObservationSolutionIdCheckQuery) match {
            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
            case _ => ""
          }
          if (adminObservationSolutionIdStatus == "Failed") {
            val adminObservationIdCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Admin Collection"}]'::jsonb AND main_metadata::jsonb @> '[{"collectionName":"Observation Collection"}]'::jsonb THEN 'Success' ELSE 'Failed' END AS result FROM $metaDataTable WHERE entity_id = '$admin';"""
            val adminObservationIdStatus = postgresUtil.fetchData(adminObservationIdCheckQuery) match {
              case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
              case _ => ""
            }
            if (adminObservationIdStatus == "Failed") {
              val adminIdCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Admin Collection"}]'::jsonb THEN 'Success' ELSE 'Failed' END AS result FROM $metaDataTable WHERE entity_id = '$admin';"""
              val adminIdStatus = postgresUtil.fetchData(adminIdCheckQuery) match {
                case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
                case _ => ""
              }
              if (adminIdStatus == "Failed") {
                println(s"********** Admin Collection is not present hence creating the collection and dashboard ***********")
                try {
                  val (adminCollectionName, adminCollectionDescription) = ("Admin Collection", "Admin Report")
                  val groupName: String = s"Report_Admin"
                  val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$admin';"
                  val adminCollectionId: Int = CreateDashboard.checkAndCreateCollection(adminCollectionName, adminCollectionDescription, metabaseUtil, postgresUtil, createDashboardQuery)
                  val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", adminCollectionId).put("collectionName", adminCollectionName))
                  postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb WHERE entity_id = '$admin';")
                  val (observationCollectionName, observationCollectionDescription) = ("Observation Collection", "This collection contains sub-collection, questions and dashboards required for Observation")
                  val observationCollectionId = Utils.createCollection(observationCollectionName, observationCollectionDescription, metabaseUtil, Some(adminCollectionId))
                  val parentCollectionId: Int = if (isRubric == "true") {
                    createObservationQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                  } else {
                    createObservationWithoutRubricQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                  }
                  createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
                  if (isRubric == "true") {
                    createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
                  }
                  val observationMetadataJson = new ObjectMapper().createArrayNode()
                    .add(new ObjectMapper().createObjectNode()
                      .put("collectionId", adminCollectionId)
                      .put("collectionName", adminCollectionName))
                    .add(new ObjectMapper().createObjectNode()
                      .put("collectionId", observationCollectionId)
                      .put("collectionName", observationCollectionName))
                    .add(new ObjectMapper().createObjectNode()
                      .put("collectionId", parentCollectionId)
                      .put("collectionName", s"Observation[$solutionName]"))
                  postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$admin';")
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
                  val (observationCollectionName, observationCollectionDescription) = ("Observation Collection", "This collection contains sub-collection, questions and dashboards required for Observation")
                  val observationCollectionId = Utils.createCollection(observationCollectionName, observationCollectionDescription, metabaseUtil, Some(adminCollectionId))
                  val parentCollectionId: Int = if (isRubric == "true") {
                    createObservationQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                  } else {
                    createObservationWithoutRubricQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                  }
                  createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
                  if (isRubric == "true") {
                    createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
                  }
                  val observationMetadataJson = new ObjectMapper().createArrayNode()
                    .add(new ObjectMapper().createObjectNode()
                      .put("collectionId", observationCollectionId)
                      .put("collectionName", observationCollectionName))
                    .add(new ObjectMapper().createObjectNode()
                      .put("collectionId", parentCollectionId)
                      .put("collectionName", s"Observation[$solutionName]"))
                  postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$admin';")

                }
                catch {
                  case e: Exception =>
                    postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$admin';")
                    println(s"An error occurred: ${e.getMessage}")
                    e.printStackTrace()
                }
              }
            } else {
              try {
                val observationCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Observation Collection' AND entity_name = 'Admin';"
                val observationCollectionId = postgresUtil.executeQuery[Int](observationCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
                val parentCollectionId: Int = if (isRubric == "true") {
                  createObservationQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                } else {
                  createObservationWithoutRubricQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                }
                createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
                if (isRubric == "true") {
                  createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
                }
                val observationMetadataJson = new ObjectMapper().createArrayNode()
                  .add(new ObjectMapper().createObjectNode()
                    .put("collectionId", parentCollectionId)
                    .put("collectionName", s"Observation[$solutionName]"))
                postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$admin';")
              }
              catch {
                case e: Exception =>
                  postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$admin';")
                  println(s"An error occurred: ${e.getMessage}")
                  e.printStackTrace()
              }
            }
          } else {
            println(s"Admin, Observation & Observation [$solutionName] Collections has already been created hence skipping the process !!!!!!!!!!!!")
          }
        } else {
          println(s"Admin key is not present or is empty")
        }


        /**
         * Logic to process and create Program Dashboard
         */
        if (targetedProgramId.nonEmpty) {
          println(s"********** Started Processing Metabase Program Dashboard For Observation ***********")
          val programObservationSolutionIdCheckQuery =
            s"""SELECT CASE WHEN
                    EXISTS (SELECT 1 FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS e WHERE entity_id = '$targetedProgramId' AND e ->> 'collectionName' = ('Program Collection [' || entity_name || ']'))
                    AND
                    EXISTS (SELECT 1 FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS e WHERE entity_id = '$targetedProgramId' AND e ->> 'collectionName' = 'Observation Collection')
                    AND
                    EXISTS (SELECT 1 FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS e WHERE entity_id = '$targetedProgramId' AND e ->> 'collectionName' = 'Observation [$solutionName]')
                    THEN 'Success' ELSE 'Failed' END AS result""".stripMargin.replaceAll("\n", " ")
          val programObservationSolutionIdStatus = postgresUtil.fetchData(programObservationSolutionIdCheckQuery) match {
            case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
            case _ => ""
          }
          if (programObservationSolutionIdStatus == "Failed") {
            val programObservationIdCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Program Collection [$programName]"}]'::jsonb AND main_metadata::jsonb @> '[{"collectionName":"Observation Collection"}]'::jsonb THEN 'Success' ELSE 'Failed' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId';"""
            val programObservationIdStatus = postgresUtil.fetchData(programObservationIdCheckQuery) match {
              case List(map: Map[_, _]) => map.get("result").map(_.toString).getOrElse("")
              case _ => ""
            }
            if (programObservationIdStatus == "Failed") {
              val programObservationIdCheckQuery: String = s"""SELECT CASE WHEN main_metadata::jsonb @> '[{"collectionName":"Program Collection [$programName]"}]'::jsonb THEN 'Success' ELSE 'Failed' END AS result FROM $metaDataTable WHERE entity_id = '$targetedProgramId';"""
              val programIdStatus = postgresUtil.fetchData(programObservationIdCheckQuery) match {
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
                  val (observationCollectionName, observationCollectionDescription) = ("Observation Collection", "This collection contains sub-collection, questions and dashboards required for Observation")
                  val observationCollectionId = Utils.createCollection(observationCollectionName, observationCollectionDescription, metabaseUtil, Some(programCollectionId))
                  val parentCollectionId: Int = if (isRubric == "true") {
                    createObservationQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                  } else {
                    createObservationWithoutRubricQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                  }
                  createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
                  if (isRubric == "true") {
                    createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
                  }
                  val observationMetadataJson = new ObjectMapper().createArrayNode()
                    .add(new ObjectMapper().createObjectNode()
                      .put("collectionId", programCollectionId)
                      .put("collectionName", programCollectionName))
                    .add(new ObjectMapper().createObjectNode()
                      .put("collectionId", observationCollectionId)
                      .put("collectionName", observationCollectionName))
                    .add(new ObjectMapper().createObjectNode()
                      .put("collectionId", parentCollectionId)
                      .put("collectionName", s"Observation[$solutionName]"))
                  postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedProgramId';")
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
                  val (observationCollectionName, observationCollectionDescription) = ("Observation Collection", "This collection contains sub-collection, questions and dashboards required for Observation")
                  val observationCollectionId = Utils.createCollection(observationCollectionName, observationCollectionDescription, metabaseUtil, Some(programCollectionId))
                  val parentCollectionId: Int = if (isRubric == "true") {
                    createObservationQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                  } else {
                    createObservationWithoutRubricQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                  }
                  createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
                  if (isRubric == "true") {
                    createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
                  }
                  val observationMetadataJson = new ObjectMapper().createArrayNode()
                    .add(new ObjectMapper().createObjectNode()
                      .put("collectionId", observationCollectionId)
                      .put("collectionName", observationCollectionName))
                    .add(new ObjectMapper().createObjectNode()
                      .put("collectionId", parentCollectionId)
                      .put("collectionName", s"Observation[$solutionName]"))
                  postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedProgramId';")
                }
                catch {
                  case e: Exception =>
                    postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedProgramId';")
                    println(s"An error occurred: ${e.getMessage}")
                    e.printStackTrace()
                }
              }
            } else {
              try {
                val observationCollectionIdQuery = s"SELECT jsonb_extract_path(element, 'collectionId') AS collection_id FROM $metaDataTable, LATERAL jsonb_array_elements(main_metadata::jsonb) AS element WHERE element ->> 'collectionName' = 'Observation Collection' AND entity_id = '$targetedProgramId';"
                val observationCollectionId = postgresUtil.executeQuery[Int](observationCollectionIdQuery)(resultSet => if (resultSet.next()) resultSet.getInt("collection_id") else 0)
                val parentCollectionId: Int = if (isRubric == "true") {
                  createObservationQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                } else {
                  createObservationWithoutRubricQuestionDashboard(observationCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationQuestionTable)
                }
                createObservationStatusDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationStatusTable)
                if (isRubric == "true") {
                  createObservationDomainDashboard(parentCollectionId, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, ObservationDomainTable)
                }
                val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", s"Observation [$solutionName]"))
                postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb WHERE entity_id = '$targetedProgramId';")
              }
              catch {
                case e: Exception =>
                  postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedProgramId';")
                  println(s"An error occurred: ${e.getMessage}")
                  e.printStackTrace()
              }
            }
          } else {
            println(s"Program & Observation Collections has already been created hence skipping the process !!!!!!!!!!!!")
          }
        } else {
          println(s"program key is not present or is empty")
        }
    }
  }
    /**
     * Logic for Observation Question Dashboard
     */
    def createObservationQuestionDashboard(parentCollectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String): Int = {
      try {
        val solutionNameQuery = s"""SELECT entity_name from $metaDataTable where entity_id = '$targetedSolutionId' """
        val solutionName = postgresUtil.fetchData(solutionNameQuery) match {
          case List(map: Map[_, _]) => map.get("entity_name").map(_.toString).getOrElse("")
          case _ => ""
        }
        val collectionName: String = s"Observation [$solutionName]"
        val dashboardName: String = s"Observation Question Report"
        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
        val collectionId: Int = Utils.createCollection(collectionName, s"${solutionName} - Question Report", metabaseUtil, Some(parentCollectionId))
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil)
        val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "state_name", postgresUtil, createDashboardQuery)
        val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "district_name", postgresUtil, createDashboardQuery)
        val clusterId : Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "cluster_name", postgresUtil, createDashboardQuery)
        val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "school_name", postgresUtil, createDashboardQuery)
        val domainId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "domain_name", postgresUtil, createDashboardQuery)
        val criteriaId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "criteria_name", postgresUtil, createDashboardQuery)
        metabaseUtil.updateColumnCategory(statenameId, "State")
        metabaseUtil.updateColumnCategory(districtnameId, "City")
        val questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, domainId, criteriaId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Parameter'"
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
        val observationMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString))
        postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$observationMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
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

    def createObservationWithoutRubricQuestionDashboard(parentCollectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String): Int = {
      try {
        val solutionNameQuery = s"""SELECT entity_name from $metaDataTable where entity_id = '$targetedSolutionId' """
        val solutionName = postgresUtil.fetchData(solutionNameQuery) match {
          case List(map: Map[_, _]) => map.get("entity_name").map(_.toString).getOrElse("")
          case _ => ""
        }
        val collectionName: String = s"Observation [$solutionName]"
        val dashboardName: String = s"Observation Question Report"
        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
        val collectionId: Int = Utils.createCollection(collectionName, s"${solutionName} - Question Report", metabaseUtil, Some(parentCollectionId))
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil)
        val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "state_name", postgresUtil, createDashboardQuery)
        val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "district_name", postgresUtil, createDashboardQuery)
        val clusterId : Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationQuestionTable, "cluster_name", postgresUtil, createDashboardQuery)
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
        val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "state_name", postgresUtil, createDashboardQuery)
        val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationStatusTable, "district_name", postgresUtil, createDashboardQuery)
        metabaseUtil.updateColumnCategory(statenameId,"State")
        metabaseUtil.updateColumnCategory(districtnameId,"City")
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
      val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
      metabaseUtil.syncDatabaseAndRescanValues(databaseId)
      val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "state_name", postgresUtil, createDashboardQuery)
      val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, observationDomainTable, "district_name", postgresUtil, createDashboardQuery)
      metabaseUtil.updateColumnCategory(statenameId,"State")
      metabaseUtil.updateColumnCategory(districtnameId,"City")
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
    catch {
      case e: Exception =>
        postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    }
  }



//    event.reportType match {
//      case "Observation" =>
//        println(s">>>>>>>>>>> Started Processing Metabase Observation Dashboards >>>>>>>>>>>>")
//          if (event.isRubric == "true") {
//            println("Executing domain part...")
//            try {
//              val solutionName: String = event.solutionName
//              val collectionName: String = s"Observation Domain Collection[$solutionName]"
//              val dashboardName: String = s"Observation Domain Report[$solutionName]"
//              val groupName: String = s"${solutionName}_Domain_Program_Manager"
//              val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$solution_id';"
//              val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"${solution_id}_domain Report", metabaseUtil, postgresUtil, createDashboardQuery)
//              val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
//              val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
//              metabaseUtil.syncDatabaseAndRescanValues(databaseId)
//              val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "state_name", postgresUtil, createDashboardQuery)
//              val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "district_name", postgresUtil, createDashboardQuery)
//              metabaseUtil.updateColumnCategory(statenameId,"State")
//              metabaseUtil.updateColumnCategory(districtnameId,"City")
//              val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "school_name", postgresUtil, createDashboardQuery)
//              val clusterId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "cluster_name", postgresUtil, createDashboardQuery)
//              val domainId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "domain", postgresUtil, createDashboardQuery)
//              val criteriaId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "criteria", postgresUtil, createDashboardQuery)
//              val reportConfigQuery: String = s"SELECT question_type, config FROM $report_config WHERE dashboard_name = 'Observation-Domain' AND report_name = 'Domain-Report';"
//              val questionCardIdList = UpdateDomainJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, domainId, criteriaId, domainTable, metabaseUtil, postgresUtil, report_config)
//              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
//              val parametersQuery: String = s"SELECT config FROM $report_config WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Domain-Parameter'"
//              UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
////                val updateTableQuery = s"UPDATE $metaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', quest ion_ids = '$questionIdsString', status = 'Success', error_message = '' WHERE entity_id = '$solution_id';"
////                postgresUtil.insertData(updateTableQuery)
////                CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
//            } catch {
//              case e: Exception =>
////                  val updateTableQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE entity_id = '$solution_id';"
////                  postgresUtil.insertData(updateTableQuery)
//                println(s"An error occurred: ${e.getMessage}")
//                e.printStackTrace()
//            }
//          }

//          case ("question") =>
//            if (event.isRubric == "true") {
//              println("Executing with rubric question part...")
//              try {
//                val solutionName: String = event.solutionName
//                val collectionName: String = s"Observation Question Collection[$solutionName]"
//                val dashboardName: String = s"Observation Question Report[$solutionName]"
//                val groupName: String = s"${solutionName}_Question_Program_Manager"
//                val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$solution_id';"
//                val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"${solution_id}_question Report", metabaseUtil, postgresUtil, createDashboardQuery)
//                val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
//                val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
//                metabaseUtil.syncDatabaseAndRescanValues(databaseId)
//                val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "state_name", postgresUtil, createDashboardQuery)
//                val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "district_name", postgresUtil, createDashboardQuery)
//                val clusterId : Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "cluster_name", postgresUtil, createDashboardQuery)
//                val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "school_name", postgresUtil, createDashboardQuery)
//                val domainId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "domain_name", postgresUtil, createDashboardQuery)
//                val criteriaId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "criteria_name", postgresUtil, createDashboardQuery)
//                metabaseUtil.updateColumnCategory(statenameId,"State")
//                metabaseUtil.updateColumnCategory(districtnameId,"City")
//                val questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, domainId, criteriaId, questionTable, metabaseUtil, postgresUtil, report_config)
//                val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
//                val parametersQuery: String = s"SELECT config FROM $report_config WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Parameter'"
//                UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
//                //              val updateTableQuery = s"UPDATE $metaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success', error_message = '' WHERE entity_id = '$solution_id';"
//                //              postgresUtil.insertData(updateTableQuery)
//                //              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
//              } catch {
//                case e: Exception =>
//                  //                val updateTableQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE entity_id = '$solution_id';"
//                  //                postgresUtil.insertData(updateTableQuery)
//                  println(s"An error occurred: ${e.getMessage}")
//                  e.printStackTrace()
//              }
//            } else {
//              println("Executing without rubric question part...")
//              try {
//                val solutionName: String = event.solutionName
//                val collectionName: String = s"Observation Question Collection[$solutionName]"
//                val dashboardName: String = s"Observation Question Report[$solutionName]"
//                val groupName: String = s"${solutionName}_Question_Program_Manager"
//                val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$solution_id';"
//                val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"${solution_id}_question Report", metabaseUtil, postgresUtil, createDashboardQuery)
//                val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
//                val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
//                metabaseUtil.syncDatabaseAndRescanValues(databaseId)
//                val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "state_name", postgresUtil, createDashboardQuery)
//                val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "district_name", postgresUtil, createDashboardQuery)
//                val clusterId : Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "cluster_name", postgresUtil, createDashboardQuery)
//                val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "school_name", postgresUtil, createDashboardQuery)
//                metabaseUtil.updateColumnCategory(statenameId,"State")
//                metabaseUtil.updateColumnCategory(districtnameId,"City")
//                val questionCardIdList = UpdateWithoutRubricQuestionJsonFiles.ProcessAndUpdateJsonFiles(collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, questionTable, metabaseUtil, postgresUtil, report_config)
//                val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
//                val parametersQuery: String = s"SELECT config FROM $report_config WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Without-Rubric-Parameter'"
//                UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
//                //              val updateTableQuery = s"UPDATE $metaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success', error_message = '' WHERE entity_id = '$solution_id';"
//                //              postgresUtil.insertData(updateTableQuery)
//                //              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
//              } catch {
//                case e: Exception =>
//                  //                val updateTableQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE entity_id = '$solution_id';"
//                  //                postgresUtil.insertData(updateTableQuery)
//                  println(s"An error occurred: ${e.getMessage}")
//                  e.printStackTrace()
//              }
//            }

//        println("Executing Observation Status part...")
//        try {
//          val solutionName: String = event.solutionName
//          val collectionName: String = s"Observation Status Collection[$solutionName]"
//          val dashboardName: String = s"Observation Status Report[$solutionName]"
//          val groupName: String = s"${solutionName}_Status_Program_Manager"
//          val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$solution_id';"
//          val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"${solution_id}_status Report", metabaseUtil, postgresUtil, createDashboardQuery)
//          val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
//          val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
//          metabaseUtil.syncDatabaseAndRescanValues(databaseId)
//          val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "state_name", postgresUtil, createDashboardQuery)
//          val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "district_name", postgresUtil, createDashboardQuery)
//          metabaseUtil.updateColumnCategory(statenameId,"State")
//          metabaseUtil.updateColumnCategory(districtnameId,"City")
//          val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "school_name", postgresUtil, createDashboardQuery)
//          val clusterId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "cluster_name", postgresUtil, createDashboardQuery)
//          val blockId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "block_name", postgresUtil, createDashboardQuery)
//          val orgId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "org_name", postgresUtil, createDashboardQuery)
//          val reportConfigQuery: String = s"SELECT question_type, config FROM $report_config WHERE dashboard_name = 'Observation-Status' AND report_name = 'Status-Report';"
//          val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, blockId, orgId, statusTable, metabaseUtil, postgresUtil, report_config)
//          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
//          val parametersQuery: String = s"SELECT config FROM $report_config WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Status-Parameter'"
//          UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
//          //                val updateTableQuery = s"UPDATE $metaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', quest ion_ids = '$questionIdsString', status = 'Success', error_message = '' WHERE entity_id = '$solution_id';"
//          //                postgresUtil.insertData(updateTableQuery)
//          //                CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
//        } catch {
//          case e: Exception =>
//            //                  val updateTableQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE entity_id = '$solution_id';"
//            //                  postgresUtil.insertData(updateTableQuery)
//            println(s"An error occurred: ${e.getMessage}")
//            e.printStackTrace()
//        }

        println(s"***************** End of Processing the Metabase Observation Dashboard *****************\n")
//  }
}
