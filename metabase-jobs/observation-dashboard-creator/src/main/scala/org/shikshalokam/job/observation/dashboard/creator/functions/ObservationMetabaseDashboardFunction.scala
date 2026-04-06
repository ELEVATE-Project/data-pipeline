package org.shikshalokam.job.observation.dashboard.creator.functions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.observation.dashboard.creator.domain.Event
import org.shikshalokam.job.observation.dashboard.creator.task.ObservationMetabaseDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory
import scala.collection.concurrent.TrieMap

import scala.collection.JavaConverters._
import scala.collection.immutable.{ListMap, _}
import scala.collection.mutable.ListBuffer

class ObservationMetabaseDashboardFunction(config: ObservationMetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabasePostgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
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
    val metabasePgDb: String = config.metabasePgDatabase
    val connectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$pgDataBase"
    val metabaseConnectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$metabasePgDb"
    postgresUtil = new PostgresUtil(connectionUrl, pgUsername, pgPassword)
    metabasePostgresUtil = new PostgresUtil(metabaseConnectionUrl, pgUsername, pgPassword)
    metabaseUtil = new MetabaseUtil(metabaseUrl, metabaseUsername, metabasePassword)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {

    println(s"***************** Start of Processing the Metabase Dashboard Event with Id = ${event._id}*****************")

    val startTime = System.currentTimeMillis()
    val evidenceBaseUrl: String = config.evidenceBaseUrl
    val admin = event.admin
    val solutions: String = config.solutions
    val targetedSolutionId = event.targetedSolution
    val isRubric = event.isRubric
    val entityType = event.entityType
    val filterSync = event.filterSync
    val filterTable = event.filterTable
    val metaDataTable = config.dashboard_metadata
    val reportConfig: String = config.report_config
    val metabaseApiKey: String = config.metabaseKey
    val metabaseDatabase: String = config.metabaseDatabase
    val solutionTable: String = config.solutions
    if (targetedSolutionId.nonEmpty) {
      val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
      val observationDomainTable = s"${targetedSolutionId}_domain"
      val observationQuestionTable = s"${targetedSolutionId}_questions"
      val observationStatusTable = s"${targetedSolutionId}_status"
      val dashboardDescription: String = s"Analytical overview of the data for solutionId $targetedSolutionId"
      val solutionName = postgresUtil.fetchData(s"""SELECT entity_name FROM $metaDataTable WHERE entity_id = '$targetedSolutionId'""").collectFirst { case map: Map[_, _] => map.getOrElse("entity_name", "").toString }.getOrElse("")
      val targetedProgramId: String = {
        val id = Option(event.targetedProgram).map(_.trim).getOrElse("")
        if (id.nonEmpty) id
        else {
          val query = s"SELECT program_id FROM $solutions WHERE solution_id = '$targetedSolutionId'"
          postgresUtil.fetchData(query) match {
            case List(map: Map[_, _]) => map.get("program_id").map(_.toString).getOrElse("")
            case _ => ""
          }
        }
      }
      val storedTableIds = TrieMap.empty[(Int, String), Int]
      val storedColumnIds = TrieMap.empty[(Int, String), Int]
      val programName = postgresUtil.fetchData(s"""SELECT entity_name from $metaDataTable where entity_id = '$targetedProgramId'""").collectFirst { case map: Map[_, _] => map.get("entity_name").map(_.toString).getOrElse("") }.getOrElse("").take(80)
      val orgId = postgresUtil.fetchData(s"""SELECT org_id FROM $solutions WHERE program_id = '$targetedProgramId' AND org_id IS NOT NULL AND TRIM(org_id) <> '' LIMIT 1 """).collectFirst { case map: Map[_, _] => map.getOrElse("org_id", "").toString }.getOrElse("")
      val query =
        s"SELECT program_description, external_id, program_external_id, description FROM $solutionTable WHERE solution_id = '$targetedSolutionId'"
      val resultMap = postgresUtil.fetchData(query).collectFirst { case map: Map[_, _] => map }.getOrElse(Map.empty)

      val programDescription = resultMap.get("program_description").map(_.toString).getOrElse("")
      val solutionExternalId = resultMap.get("external_id").map(_.toString).getOrElse("")
      val programExternalId = resultMap.get("program_external_id").map(_.toString).getOrElse("")
      val solutionDescription = resultMap.get("description").map(_.toString).getOrElse("")

      val programCollectionName = s"$programName [org : $orgId]"
      val solutionCollectionName = s"$solutionName [Observation]"

      var tabList: List[String] = List()
      if (isRubric == "true") {
        tabList = List("Status Report", "Domain Report", "Question Report", "Status CSV", "Domain and Criteria CSV", "Question CSV")
      } else {
        tabList = List("Status Report", "Question Report", "Status CSV", "Question CSV")
      }


      println(s"domainTable: $observationDomainTable")
      println(s"questionTable: $observationQuestionTable")
      println(s"isRubric: ${event.isRubric}")
      println(s"targetedSolutionId: $targetedSolutionId")
      println(s"targetedProgramId: $targetedProgramId")
      println(s"admin: $admin")
      println(s"programName: $programName")
      println(s"solutionsName: $solutionName")

      println(s">>>>>>>>>>> Started Processing Metabase Observation Dashboards >>>>>>>>>>>>\n")

      /**
       * Logic to process and create Programs Collection and observation solution Dashboard for Admin
       */
      if (solutionName.nonEmpty && targetedProgramId.nonEmpty && programName.nonEmpty) {
        event.reportType match {
          case "Observation" =>
            println(s"===========> Started Processing Metabase Admin Dashboard For Observation \n")
            val (adminCollectionPresent, adminCollectionId) = validateCollection(s"Programs", "Admin")
            if (adminCollectionPresent && adminCollectionId != 0) {
              val (programCollectionPresent, programCollectionId) = validateCollection(programCollectionName.take(100), "Admin", Some(targetedProgramId))
              if (programCollectionPresent && programCollectionId != 0) {
                val (solutionCollectionPresent, solutionCollectionId) = validateCollection(solutionCollectionName.take(100), "Admin", Some(targetedSolutionId))
                if (solutionCollectionPresent && solutionCollectionId != 0) {
                  println(s"=====> $solutionCollectionName collection is present, hence skipping the process ......")
                } else {
                  createDashboard(programCollectionId, targetedSolutionId, solutionExternalId, solutionCollectionName, solutionDescription, dashboardDescription, tabList, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, observationStatusTable, observationDomainTable, observationQuestionTable, entityType, isRubric, "Admin")
                }
              } else {
                println(s"=====> $programCollectionName collection is not present, creating $programCollectionName collection for Admin ......")
                val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId, targetedProgramId, programExternalId, programCollectionName, programDescription, "Admin")
                createDashboard(programCollectionId, targetedSolutionId, solutionExternalId, solutionCollectionName, solutionDescription, dashboardDescription, tabList, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, observationStatusTable, observationDomainTable, observationQuestionTable, entityType, isRubric, "Admin")
              }
            } else {
              println(s"=====> Programs Collection is not present, creating Programs Collection ......")
              val adminCollectionId = createAdminCollection
              val programCollectionId = createProgramCollectionInsideAdmin(adminCollectionId, targetedProgramId, programExternalId, programCollectionName, programDescription, "Admin")
              createDashboard(programCollectionId, targetedSolutionId, solutionExternalId, solutionCollectionName, solutionDescription, dashboardDescription, tabList, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, observationStatusTable, observationDomainTable, observationQuestionTable, entityType, isRubric, "Admin")
            }

            println("********************************************** Start Observation Program Dashboard Processing **********************************************")
            val (programCollectionPresent, programCollectionId) = validateCollection(programCollectionName.take(100), "Program Manager", Some(targetedProgramId))
            if (programCollectionPresent && programCollectionId != 0) {
              println(s"=====> $programCollectionName collection is present hence skipping the process ......")
              val (solutionCollectionPresent, solutionCollectionId) = validateCollection(solutionCollectionName.take(100), "Program Manager", Some(targetedSolutionId))
              if (solutionCollectionPresent && solutionCollectionId != 0) {
                println(s"=====> $solutionCollectionName collection is present, hence skipping the process ......")
              } else {
                println(s"=====> $solutionCollectionName collection is not present, creating $solutionCollectionName collection for Program Manager ......")
                createDashboard(programCollectionId, targetedSolutionId, solutionExternalId, solutionCollectionName, solutionDescription, dashboardDescription, tabList, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, observationStatusTable, observationDomainTable, observationQuestionTable, entityType, isRubric, "Program Manager")
              }
            } else {
              println(s"=====> $programCollectionName collection is not present, creating $programCollectionName collection for Program Manager ......")
              val programCollectionId = createProgramCollection(programCollectionName, targetedProgramId, programExternalId, programDescription, "Program Manager")
              createDashboard(programCollectionId, targetedSolutionId, solutionExternalId, solutionCollectionName, solutionDescription, dashboardDescription, tabList, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, observationStatusTable, observationDomainTable, observationQuestionTable, entityType, isRubric, "Program Manager")
            }
        }
      } else {
        println(s"=====> Program name or Solution name is null, skipping the processing")
      }

      if (filterSync.nonEmpty){
        println(s"Started syncing the $filterTable table ")
        val searchTableResponse = metabaseUtil.searchTable(filterTable, databaseId)
        val filterTableId: Int = extractTableId(searchTableResponse)
        if (filterTableId != -1) {
          metabaseUtil.discardValues(filterTableId)
          metabaseUtil.rescanValues(filterTableId)
        } else {
          println(s"Table does not exits in the metabase DB")
        }
        println(s"Finished syncing the $filterTable table ")
      }

      def extractTableId(response: String): Int = {
        val json = ujson.read(response)
        val dataArr = json("data").arr
        if (dataArr.nonEmpty && dataArr(0).obj.contains("table_id")) {
          dataArr(0)("table_id").num.toInt
        } else {
          -1
        }
      }

      def createDashboard(programCollectionId: Int, targetedSolutionId: String, solutionExternalId: String, solutionCollectionName: String, solutionDescription: String, dashboardDescription: String, tabList: List[String], metaDataTable: String, reportConfig: String, metabaseDatabase: String, evidenceBaseUrl: String, targetedProgramId: String, observationStatusTable: String, observationDomainTable: String, observationQuestionTable: String, entityType: String, isRubric: String, reportFor: String): Unit = {
        val solutionCollectionId = createSolutionCollectionInsideProgram(programCollectionId, targetedSolutionId, solutionExternalId, solutionCollectionName, solutionDescription, reportFor)
        val dashboardId: Int = Utils.createDashboard(solutionCollectionId, s"Observation Dashboard", dashboardDescription, metabaseUtil)
        val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
        val solutionDashboardIds = combinedFunctionForDashboards(solutionCollectionId, dashboardId, tabIdMap, solutionCollectionName, reportFor)
        val csvDashboardIds = createObservationTableDashboard(solutionCollectionId, databaseId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, observationStatusTable, observationDomainTable, observationQuestionTable, entityType, reportFor, isRubric)
        val mainQuestionIdsString = "[" + (solutionDashboardIds ++ csvDashboardIds).mkString(",") + "]"
        val solutionMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", solutionCollectionId).put("collectionName", solutionCollectionName).put("Collection For", reportFor).put("dashboardId", dashboardId).put("dashboardName", s"Observation Dashboard").put("questionIds", mainQuestionIdsString))
        postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$solutionMetadataJson' ::jsonb, status = 'Success' WHERE entity_id = '$targetedSolutionId';")
      }

      def combinedFunctionForDashboards(exactProgramCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], exactProgramCollectionName: String, reportFor: String): ListBuffer[Int] = {
        val (domainIds, questionIds, withoutRubricIds) =
          if (isRubric == "true") {
            println(s"=====> Creating Observation Domain Dashboard for $exactProgramCollectionName")
            val observationDomainDashboardIds = createObservationDomainDashboard(exactProgramCollectionId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, observationDomainTable, entityType)
            println(s"=====> Creating Observation Question Dashboard for $exactProgramCollectionName")
            val observationQuestionDashboardIds = createObservationQuestionDashboard(exactProgramCollectionId, dashboardId, tabIdMap, exactProgramCollectionName, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, observationQuestionTable, observationDomainTable, entityType, reportFor)
            (observationDomainDashboardIds, observationQuestionDashboardIds, ListBuffer.empty[Int])
          } else {
            println(s"=====> Creating Observation Without Rubric Question Dashboard for $exactProgramCollectionName")
            val observationWithoutRubricQuestionDashboardIds = createObservationWithoutRubricQuestionDashboard(exactProgramCollectionId, databaseId, dashboardId, tabIdMap, exactProgramCollectionName, metaDataTable, reportConfig, metabaseDatabase, evidenceBaseUrl, targetedProgramId, targetedSolutionId, observationQuestionTable, entityType, reportFor)
            (ListBuffer.empty[Int], ListBuffer.empty[Int], observationWithoutRubricQuestionDashboardIds)
          }

        println(s"=====> Creating Observation Status Dashboard for $exactProgramCollectionName")
        val observationStatusDashboardId: ListBuffer[Int] = createObservationStatusDashboard(exactProgramCollectionId, databaseId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, targetedProgramId, targetedSolutionId, observationStatusTable, entityType, reportFor)
        val allDashboardIds = domainIds ++ questionIds ++ withoutRubricIds ++ observationStatusDashboardId
        allDashboardIds
      }

      def createAdminCollection: Int = {
        val (adminCollectionName, adminCollectionDescription) = ("Programs", s"All programs made available on the platform are stored in this collection.\n\nCollection For: Admin")
        val adminCollectionId: Int = Utils.createCollection(adminCollectionName, adminCollectionDescription, metabaseUtil)
        if (adminCollectionId != -1) {
          val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", adminCollectionId).put("collectionName", adminCollectionName).put("Collection For", "Admin"))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb, status = 'Success' WHERE entity_id = '1';")
          Utils.createGroupForCollection(metabaseUtil, "Report_Admin_Programs", adminCollectionId)
          adminCollectionId
        } else {
          println(s"Admin Collection returned -1")
          -1
        }
      }

      def createProgramCollection(programCollectionName: String, targetedProgramId: String, programExternalId: String, programDescription: String, reportFor: String): Int = {
        val programCollectionDescription = s"Program Id: $targetedProgramId\n\nProgram External Id: $programExternalId\n\nCollection For: $reportFor\n\nProgram Description: $programDescription"
        val programCollectionId: Int = Utils.createCollection(programCollectionName, programCollectionDescription, metabaseUtil)
        if (programCollectionId != -1) {
          val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName).put("Collection For", reportFor))
          val programMetadataJsonString = programMetadataJson.toString.replace("'", "''")
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJsonString' ::jsonb, status = 'Success' WHERE entity_id = '$targetedProgramId';")
          Utils.createGroupForCollection(metabaseUtil, s"Program_Manager_$targetedProgramId".take(255), programCollectionId)
          programCollectionId
        } else {
          println(s"$programName [$targetedProgramId] returned -1")
          -1
        }
      }

      def createSolutionCollectionInsideProgram(programCollectionId: Int, targetedSolutionId: String, solutionExternalId: String, solutionCollectionName: String, solutionCollectionDescription: String, reportFor: String): Int = {
        val solutionDescription = s"Solution Id: $targetedSolutionId\n\nSolution External Id: $solutionExternalId\n\nCollection For: $reportFor\n\nSolution Description: $solutionCollectionDescription"
        val solutionCollectionId = Utils.createCollection(solutionCollectionName.take(100), solutionDescription.take(255), metabaseUtil, Some(programCollectionId))
        val solutionMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", solutionCollectionId).put("collectionName", solutionCollectionName).put("Collection For", reportFor))
        val solutionMetadataJsonString = solutionMetadataJson.toString.replace("'", "''")
        postgresUtil.insertData(s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$solutionMetadataJsonString'::jsonb, status = 'Success' WHERE entity_id = '$targetedSolutionId';")
        solutionCollectionId
      }

      def createProgramCollectionInsideAdmin(adminCollectionId: Int, targetedProgramId: String, externalId: String, programCollectionName: String, programCollectionDescription: String, reportFor: String): Int = {
        val programDescription = s"Program Id: $targetedProgramId\n\nExternal Id: $externalId\n\nCollection For: $reportFor\n\nProgram Description: $programCollectionDescription"
        val programCollectionId = Utils.createCollection(programCollectionName.take(100), programDescription.take(255), metabaseUtil, Some(adminCollectionId))
        val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName).put("Collection For", reportFor))
        val programMetadataJsonString = programMetadataJson.toString.replace("'", "''")
        postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJsonString' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedProgramId';")
        programCollectionId
      }

      def createObservationQuestionDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, evidenceBaseUrl: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String, observationDomainTable: String, entityType: String, reportFor: String): ListBuffer[Int] = {
        try {
          val dashboardName: String = s"Question Report"
          val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
          if (parentCollectionId != -1) {
            val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
            val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Parameter'"
            val (params, diffLevelDict, entityColumnName, isEntityTypeMatched) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationQuestionTable, postgresUtil, createDashboardQuery, metabaseApiKey)
            var questionCardIdList: ListBuffer[Int] = ListBuffer.empty[Int]
            if (!isEntityTypeMatched) {
              val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Customised-Filter-Question-Parameter'"
              val (params, diffLevelDict, entityColumnName, isEntityTypeMatched) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationQuestionTable, postgresUtil, createDashboardQuery, metabaseApiKey)
              questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(parentCollectionId, databaseId, dashboardId, tabId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig, params, diffLevelDict, evidenceBaseUrl, isEntityTypeMatched)
            } else {
              questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(parentCollectionId, databaseId, dashboardId, tabId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig, params, diffLevelDict, evidenceBaseUrl, isEntityTypeMatched)
            }
            questionCardIdList
          } else {
            println("Solution Collection Id returned -1")
            ListBuffer.empty[Int]
          }
        }
        catch {
          case e: Exception =>
            postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
            println(s"An error occurred: ${e.getMessage}")
            e.printStackTrace()
            ListBuffer.empty[Int]
        }
      }

      def createObservationWithoutRubricQuestionDashboard(parentCollectionId: Int, databaseId: Int, dashboardId: Int, tabIdMap: Map[String, Int], collectionName: String, metaDataTable: String, reportConfig: String, metabaseDatabase: String, evidenceBaseUrl: String, targetedProgramId: String, targetedSolutionId: String, observationQuestionTable: String, entityType: String, reportFor: String): ListBuffer[Int] = {
        try {
          val dashboardName: String = s"Question Report"
          val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
          if (parentCollectionId != -1) {
            val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
            val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Without-Rubric-Parameter'"
            val (params, diffLevelDict, _, isEntityTypeMatched) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationQuestionTable, postgresUtil, createDashboardQuery, metabaseApiKey)
            var questionCardIdList: ListBuffer[Int] = ListBuffer.empty[Int]
            if (!isEntityTypeMatched) {
              val parametersQuery1: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Customised-Filter-Question-Without-Rubric-Parameter'"
              val (params, diffLevelDict, _, isEntityTypeMatched) = extractParameterDicts(parametersQuery1, entityType, databaseId, metabaseUtil, observationQuestionTable, postgresUtil, createDashboardQuery, metabaseApiKey)
              questionCardIdList = UpdateWithoutRubricQuestionJsonFiles.ProcessAndUpdateJsonFiles(parentCollectionId, databaseId, dashboardId, tabId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig, params, diffLevelDict, evidenceBaseUrl, isEntityTypeMatched)
              UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery1, dashboardId, postgresUtil, diffLevelDict, entityType, isEntityTypeMatched)
            } else {
              questionCardIdList = UpdateWithoutRubricQuestionJsonFiles.ProcessAndUpdateJsonFiles(parentCollectionId, databaseId, dashboardId, tabId, observationQuestionTable, metabaseUtil, postgresUtil, reportConfig, params, diffLevelDict, evidenceBaseUrl, isEntityTypeMatched)
              UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil, diffLevelDict, entityType, isEntityTypeMatched)
            }
            questionCardIdList
          } else {
            println("Solution Collection Id returned -1")
            ListBuffer.empty[Int]
          }
        }
        catch {
          case e: Exception =>
            postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
            println(s"An error occurred: ${e.getMessage}")
            e.printStackTrace()
            ListBuffer.empty[Int]
        }
      }

      def createObservationStatusDashboard(parentCollectionId: Int, databaseId: Int, dashboardId: Int, tabIdMap: Map[String, Int], metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationStatusTable: String, entityType: String, reportFor: String): ListBuffer[Int] = {
        try {
          val dashboardName: String = s"Status Report"
          val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
          val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
          val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Status-Parameter'"
          val (params, diffLevelDict, _, isEntityTypeMatched) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationStatusTable, postgresUtil, createDashboardQuery, metabaseApiKey)
          var questionCardIdList: ListBuffer[Int] = ListBuffer.empty[Int]
          if (!isEntityTypeMatched) {
            val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Customised-Filter-Status-Parameter'"
            val (params, diffLevelDict, _, _) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationStatusTable, postgresUtil, createDashboardQuery, metabaseApiKey)
            val reportConfigQuery: String = s"SELECT * FROM $reportConfig WHERE (dashboard_name = 'Observation-Status' AND report_name = 'Status-Report' AND question_type != 'table') OR (dashboard_name = 'Observation-Customised-Filter-Csv-Table' AND report_name = 'Status-Report' AND question_type = 'table'); "
            val replacements: Map[String, String] = Map(
              "${statusTable}" -> s""""${observationStatusTable}"""",
            )
            questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, metabaseUtil, postgresUtil, params, replacements, diffLevelDict, entityType)
          } else {
            val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Status' AND report_name = 'Status-Report';"
            val replacements: Map[String, String] = Map(
              "${statusTable}" -> s""""${observationStatusTable}"""",
            )
            questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, metabaseUtil, postgresUtil, params, replacements, diffLevelDict, entityType)
          }
          questionCardIdList
        }
        catch {
          case e: Exception =>
            postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
            println(s"An error occurred: ${e.getMessage}")
            e.printStackTrace()
            ListBuffer.empty[Int]
        }
      }

      def createObservationDomainDashboard(parentCollectionId: Int, dashboardId: Int, tabIdMap: Map[String, Int], metaDataTable: String, reportConfig: String, metabaseDatabase: String, targetedProgramId: String, targetedSolutionId: String, observationDomainTable: String, entityType: String): ListBuffer[Int] = {
        try {
          val dashboardName: String = s"Domain Report"
          val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
          val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
          val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Domain-Parameter'"
          val (params, diffLevelDict, entityColumnName, isEntityTypeMatched) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationDomainTable, postgresUtil, createDashboardQuery, metabaseApiKey)
          var questionCardIdList: ListBuffer[Int] = ListBuffer.empty[Int]
          if (!isEntityTypeMatched) {
            val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Customised-Filter-Domain-Parameter'"
            val (params, diffLevelDict, entityColumnName, isEntityTypeMatched) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationDomainTable, postgresUtil, createDashboardQuery, metabaseApiKey)
            val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE (dashboard_name = 'Observation-Domain' AND report_name = 'Domain-Report' AND question_type != 'table') OR (dashboard_name = 'Observation-Customised-Filter-Csv-Table' AND report_name = 'Domain-Report' AND question_type = 'table');"
            val replacements: Map[String, String] = Map(
              "${domainTable}" -> s""""${observationDomainTable}"""",
              "${entityColumnName}" -> s"""$entityColumnName""",
              "${entityColumnId}" -> s"""$entityColumnName""",
              "${entityType}" -> s"""$entityType"""
            )
            questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, metabaseUtil, postgresUtil, params, replacements, diffLevelDict, entityType)
            UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil, diffLevelDict, entityType, isEntityTypeMatched)
          } else {
            val entityColumnId = if (entityColumnName.endsWith("_name")) {
              entityColumnName.replaceAll("_name$", "_id")
            } else {
              entityColumnName
            }
            val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Domain' AND report_name = 'Domain-Report';"
            val replacements: Map[String, String] = Map(
              "${domainTable}" -> s""""${observationDomainTable}"""",
              "${entityColumnName}" -> s"""$entityColumnName""",
              "${entityColumnId}" -> s"""$entityColumnId""",
              "${entityType}" -> s"""$entityType"""
            )
            questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, metabaseUtil, postgresUtil, params, replacements, diffLevelDict, entityType)
            UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil, diffLevelDict, entityType, isEntityTypeMatched)
          }
          questionCardIdList
        }
        catch {
          case e: Exception =>
            postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
            println(s"An error occurred: ${e.getMessage}")
            e.printStackTrace()
            ListBuffer.empty[Int]
        }
      }

      def createObservationTableDashboard(parentCollectionId: Int, databaseId: Int, dashboardId: Int, tabIdMap: Map[String, Int], metaDataTable: String, reportConfig: String, metabaseDatabase: String, evidenceBaseUrl: String, targetedProgramId: String, targetedSolutionId: String, observationStatusTable: String, observationDomainTable: String, observationQuestionTable: String, entityType: String, reportFor: String, isRubric: String): ListBuffer[Int] = {
        try {
          val statusCsvTab: String = s"Status CSV"
          val domainCsvTab: String = s"Domain and Criteria CSV"
          val questionCsvTab: String = s"Question CSV"
          val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedProgramId';"
          val statusCsvConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Status' AND report_name = 'Status-Report' AND question_type = 'table';"
          val statusCsvCustomFilterConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Customised-Filter-Csv-Table' AND report_name = 'Status-Report' AND question_type = 'table';"
          val domainCsvConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Domain' AND report_name = 'Domain-Report' AND question_type = 'table';"
          val domainCsvCustomFilterConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Customised-Filter-Csv-Table' AND report_name = 'Domain-Report' AND question_type = 'table';"
          val questionCsvConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Question' AND report_name = 'Question-Report' AND question_type = 'table';"
          val questionCsvCustomFilterConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Observation-Customised-Filter-Csv-Table' AND report_name = 'Question-Report' AND question_type = 'table';"
          val statusCsvTabParametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Status-Parameter'"
          val statusCsvTabCustomParametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Customised-Filter-Status-Parameter'"
          val domainCsvTabParametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Domain-Parameter'"
          val domainCsvTabCustomParametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Customised-Filter-Domain-Parameter'"
          val questionCsvTabParametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Question-Parameter'"
          val questionCsvTabCustomParametersQuery: String = s"SELECT config FROM $reportConfig WHERE dashboard_name = 'Observation' AND question_type = 'Observation-Customised-Filter-Question-Parameter'"
          val replacements: Map[String, String] = Map(
            "${statusTable}" -> s""""$observationStatusTable"""",
            "${domainTable}" -> s""""$observationDomainTable"""",
            "${questionTable}" -> s""""$observationQuestionTable"""",
            "${evidenceBaseUrl}" -> s"""'$evidenceBaseUrl'"""
          )

          val (params, diffLevelDict, _, isEntityTypeMatched) = extractParameterDicts(statusCsvTabParametersQuery, entityType, databaseId, metabaseUtil, observationStatusTable, postgresUtil, createDashboardQuery, metabaseApiKey)
          var questionCardIdList: ListBuffer[Int] = ListBuffer.empty[Int]
          if(!isEntityTypeMatched){
            val statusQuestionIds = TableBasedDashboardCreationCommonSteps(tabIdMap, databaseId, statusCsvTab, dashboardId, statusCsvTabCustomParametersQuery, statusCsvCustomFilterConfigQuery, replacements, observationStatusTable)
            val questionQuestionIds = TableBasedDashboardCreationCommonSteps(tabIdMap, databaseId, questionCsvTab, dashboardId, questionCsvTabCustomParametersQuery, questionCsvCustomFilterConfigQuery, replacements, observationQuestionTable)
            val domainQuestionIds: ListBuffer[Int] =
              if (isRubric == "true") TableBasedDashboardCreationCommonSteps(tabIdMap, databaseId, domainCsvTab, dashboardId, domainCsvTabCustomParametersQuery, domainCsvCustomFilterConfigQuery, replacements, observationDomainTable)
              else ListBuffer.empty[Int]
            questionCardIdList = statusQuestionIds ++ domainQuestionIds ++ questionQuestionIds
          } else {
            val statusQuestionIds = TableBasedDashboardCreationCommonSteps(tabIdMap, databaseId, statusCsvTab, dashboardId, statusCsvTabParametersQuery, statusCsvConfigQuery, replacements, observationStatusTable)
            val questionQuestionIds = TableBasedDashboardCreationCommonSteps(tabIdMap, databaseId, questionCsvTab, dashboardId, questionCsvTabParametersQuery, questionCsvConfigQuery, replacements, observationQuestionTable)
            val domainQuestionIds: ListBuffer[Int] =
              if (isRubric == "true") TableBasedDashboardCreationCommonSteps(tabIdMap, databaseId, domainCsvTab, dashboardId, domainCsvTabParametersQuery, domainCsvConfigQuery, replacements, observationDomainTable)
              else ListBuffer.empty[Int]
            questionCardIdList = statusQuestionIds ++ domainQuestionIds ++ questionQuestionIds
          }

          def TableBasedDashboardCreationCommonSteps(tabIdMap: Map[String, Int], databaseId: Int, dashboardName: String, dashboardId: Int, parametersQuery: String, reportConfigQuery: String, replacements: Map[String, String], observationTable: String): ListBuffer[Int] = {
            val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
            val (params, diffLevelDict, _, _) = extractParameterDicts(parametersQuery, entityType, databaseId, metabaseUtil, observationTable, postgresUtil, createDashboardQuery, metabaseApiKey)
            val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, tabId, metabaseUtil, postgresUtil, params, replacements, diffLevelDict, entityType)
            questionCardIdList
          }
          questionCardIdList
        }
        catch {
          case e: Exception =>
            postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$targetedSolutionId';")
            println(s"An error occurred: ${e.getMessage}")
            e.printStackTrace()
            ListBuffer.empty[Int]
        }
      }

      def getTheTableId(databaseId: Int, tableName: String, metabaseUtil: MetabaseUtil, metabasePostgresUtil: PostgresUtil, metabaseApiKey: String): Int = {
        storedTableIds.get((databaseId, tableName)) match {
          case Some(tableId) =>
            tableId

          case None =>
            val tableQuery = s"SELECT id FROM metabase_table WHERE name = '$tableName';"
            val tableIdOpt = metabasePostgresUtil.fetchData(tableQuery) match {
              case List(map: Map[_, _]) =>
                map.get("id").flatMap(id => scala.util.Try(id.toString.toInt).toOption)
              case _ => None
            }

            val tableId = tableIdOpt.getOrElse {
              val tableJson = metabaseUtil.syncNewTable(databaseId, tableName, metabaseApiKey)
              tableJson("id").num.toInt
            }

            storedTableIds.put((databaseId, tableName), tableId)
            println(s"tableId = $tableId")
            tableId
        }
      }

      def getTheColumnId(databaseId: Int, tableName: String, columnName: String, metabaseUtil: MetabaseUtil, metabasePostgresUtil: PostgresUtil, metabaseApiKey: String, metaTableQuery: String): Int = {
        try {
          val tableId = getTheTableId(databaseId, tableName, metabaseUtil, metabasePostgresUtil, metabaseApiKey)

          storedColumnIds.get((tableId, columnName)) match {
            case Some(columnId) =>
              columnId

            case None =>
              val columnQuery = s"SELECT id FROM metabase_field WHERE table_id = '$tableId' AND name = '$columnName';"

              val columnIdOpt = metabasePostgresUtil.fetchData(columnQuery) match {
                case List(map: Map[_, _]) =>
                  map.get("id").flatMap(id => scala.util.Try(id.toString.toInt).toOption)
                case _ => None
              }

              val columnId = columnIdOpt.getOrElse(-1)

              if (columnId != -1) {
                storedColumnIds.put((tableId, columnName), columnId)
                columnId
              } else {
                val errorMessage = s"Column '$columnName' not found in table '$tableName' (tableId: $tableId)"
                val escapedError = errorMessage.replace("'", "''")
                val updateTableQuery = metaTableQuery.replace("'errorMessage'", s"'$escapedError'")
                postgresUtil.insertData(updateTableQuery)
                println(s"[WARN] $errorMessage")
                -1
              }
          }
        } catch {
          case e: Exception =>
            val escapedError = e.getMessage.replace("'", "''")
            val updateTableQuery = metaTableQuery.replace("'errorMessage'", s"'$escapedError'")
            postgresUtil.insertData(updateTableQuery)
            println(s"[ERROR] Failed to get column ID: ${e.getMessage}")
            -1
        }
      }

      def extractParameterDicts(parametersQuery: String, entityType: String, databaseId: Int, metabaseUtil: MetabaseUtil, tableName: String, postgresUtil: PostgresUtil, createDashboardQuery: String, metabaseApiKey: String): (Map[String, Int], ListMap[String, String], String, Boolean) = {
        val DashboardParameter = postgresUtil.fetchData(parametersQuery) match {
          case List(map: Map[_, _]) => map.get("config").map(_.toString).getOrElse("")
          case _ => ""
        }
        val mapper = new ObjectMapper()
        val json = if (Option(DashboardParameter).exists(_.trim.nonEmpty)) DashboardParameter else "[]"
        val arrayNode = mapper.readTree(json)
        val matchedNodeOpt = arrayNode.elements().asScala.find(
          node => Option(node.get("entity_type")).exists(_.asText() == entityType)
        )
        val completeMapOfParamAndColumnName: ListMap[String, String] = ListMap(
          arrayNode.elements().asScala
            .map(node => node.get("param").asText() -> node.get("columnName").asText())
            .toSeq: _*
        )
        val (mapOfParamsAfterRemovingBelowEntityParams, _, entityColumnName, isEntityTypeMatched) =
          matchedNodeOpt match {
            // Case 1: entityType matches
            case Some(node) =>
              val removeParam = node.get("param").asText()
              val entityCol = completeMapOfParamAndColumnName.getOrElse(removeParam, "")
              val keys = completeMapOfParamAndColumnName.keys.toList
              val idx = keys.indexOf(removeParam)
              val filtered =
                if (idx == -1) completeMapOfParamAndColumnName
                else ListMap(keys.drop(idx).map(k => k -> completeMapOfParamAndColumnName(k)): _*)
              val filteredWithoutEntity = filtered.filterNot { case (k, _) => k == "entity_param" }
              (filteredWithoutEntity, removeParam, entityCol, true)

            // Case 2: entityType not found
            case None =>
              println(s"No entityType match, keeping all params")
              (completeMapOfParamAndColumnName, "", "entity_name", false)
          }

        val params: Map[String, Int] =
          mapOfParamsAfterRemovingBelowEntityParams.map { case (key, columnName) =>
            key -> getTheColumnId(databaseId, tableName, columnName, metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
          }
        val mapOfRemovedParams: ListMap[String, String] =
          completeMapOfParamAndColumnName.filterNot {
            case (k, v) =>
              mapOfParamsAfterRemovingBelowEntityParams.contains(k) &&
                mapOfParamsAfterRemovingBelowEntityParams(k) == v
          }
        (params, mapOfRemovedParams, entityColumnName, isEntityTypeMatched)
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

      val endTime = System.currentTimeMillis()
      val totalTimeSeconds = (endTime - startTime) / 1000
      println(s"Total time taken: $totalTimeSeconds seconds")
      println(s"***************** End of Processing the Metabase Observation Dashboard *****************\n")
    } else {
      println(s"targetedSolutionId is empty or null, skipping dashboard creation.")
    }
  }
}
