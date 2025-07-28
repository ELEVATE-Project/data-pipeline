package org.shikshalokam.job.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.dashboard.creator.domain.Event
import org.shikshalokam.job.dashboard.creator.miDashboard.{ComparePage, DistrictPage, HomePage, StatePage, Utils}
import org.shikshalokam.job.dashboard.creator.task.ProjectMetabaseDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.shikshalokam.job.util.JSONUtil.mapper

import scala.collection.JavaConverters._
import scala.collection.{Seq, mutable}
import org.slf4j.LoggerFactory

import scala.collection.immutable._

class ProjectMetabaseDashboardFunction(config: ProjectMetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProjectMetabaseDashboardFunction])

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

    println(s"***************** Start of Processing the Metabase Dashboard Event with Id = ${event._id} *****************")

    val admin = 1
    val targetedStateId = event.targetedState
    val targetedDistrictId = event.targetedDistrict
    val targetedSolutionId = event.targetedSolution
    val solutions: String = config.solutions
    val projects: String = config.projects
    val tasks: String = config.tasks
    val metaDataTable = config.dashboard_metadata
    val reportConfig: String = config.report_config
    val metabaseDatabase: String = config.metabaseDatabase
    val domainName: String = config.metabaseDomainName
    val solutionName = postgresUtil.fetchData(s"""SELECT name FROM $solutions WHERE solution_id = '$targetedSolutionId'""").collectFirst { case map: Map[_, _] => map.getOrElse("name", "").toString }.getOrElse("")
    val targetedProgramId: String = Option(event.targetedProgram).map(_.trim).filter(_.nonEmpty).getOrElse(postgresUtil.fetchData(s"SELECT program_id FROM $solutions WHERE solution_id = '$targetedSolutionId'").collectFirst { case map: Map[_, _] => map.get("program_id").map(_.toString).getOrElse("") }.getOrElse(""))
    val programName = postgresUtil.fetchData(s"""SELECT program_name FROM $solutions WHERE solution_id = '$targetedSolutionId'""").collectFirst { case map: Map[_, _] => map.getOrElse("program_name", "").toString }.getOrElse("")
    val orgName = postgresUtil.fetchData(s"""SELECT org_name FROM $projects WHERE solution_id = '$targetedSolutionId' LIMIT 1 """).collectFirst { case map: Map[_, _] => map.getOrElse("org_name", "").toString }.getOrElse("")
    val combinedQuery = s"SELECT program_description, program_external_id, external_id, description FROM $solutions WHERE solution_id = '$targetedSolutionId' LIMIT 1"
    val resultMap = postgresUtil.fetchData(combinedQuery).collectFirst { case map: Map[_, _] => map }.getOrElse(Map.empty[String, Any])
    val programExternalId = resultMap.get("program_external_id").map(_.toString).getOrElse("")
    val programDescription = resultMap.get("program_description").map(_.toString).getOrElse("")
    val solutionExternalId = resultMap.get("external_id").map(_.toString).getOrElse("")
    val solutionDescription = resultMap.get("description").map(_.toString).getOrElse("")

    // Printing the targetedState ID
    println(s"admin: $admin")
    println(s"Targeted State ID: $targetedStateId")
    println(s"Targeted District ID: $targetedDistrictId")
    println(s"Targeted Program ID: $targetedProgramId")
    println(s"Targeted Program Name: $programName")
    println(s"Targeted Solution ID: $targetedSolutionId")
    println(s"Targeted Solution Name: $solutionName")

    event.reportType match {
      case "Project" =>
        println(s">>>>>>>>>>> Started Processing Metabase Project Dashboards >>>>>>>>>>>>\n")

        /**
         * Logic to process and create the Micro Improvements Dashboard
         */
        val (mipCollectionPresent, mipCollectionId) = validateCollection("Micro Improvements", "Admin")
        if (mipCollectionPresent && mipCollectionId != 0) {
          println(s"=====> Micro Improvements collection present with id: $mipCollectionId, Skipping this step.")
        } else {
          println("=====> Creating Micro Improvements Collection And Dashboard")
          createMicroImprovementsCollectionAndDashboard(metaDataTable, reportConfig, metabaseDatabase)
        }
        println("\n")

        /**
         * Logic to process and create National Overview Dashboard
         */
        val (nationalCollectionPresent, nationalCollectionId) = validateCollection("National Overview", "Admin")
        if (nationalCollectionPresent && nationalCollectionId != 0) {
          println(s"=====> National Overview collection present with id: $nationalCollectionId, Skipping this step.")
        } else {
          println("=====> Creating National Overview Collection And Dashboard")
          createNationalOverviewCollectionAndDashboard(metaDataTable, reportConfig, metabaseDatabase)
        }
        println("\n")

        //TODO: @Prashant put state and district logic here

        /**
         * Logic to process and create Programs Collection and project solution Dashboard for Admin
         */
        if (targetedSolutionId.nonEmpty && solutionName.nonEmpty && targetedProgramId.nonEmpty && programName.nonEmpty) {
          val programCollectionName = s"$programName [org : $orgName]"
          val programCollectionDescription = s"Program Id: $targetedProgramId\n\nProgram External Id: $programExternalId\n\nCollection For: Admin\n\nProgram Description: $programDescription"
          val solutionCollectionName = s"$solutionName [Project]"
          val solutionCollectionDescription = s"Solution Id: $targetedSolutionId\n\nSolution External Id: $solutionExternalId\n\nCollection For: Admin\n\nSolution Description: $solutionDescription"

          val (mainCollectionPresent, mainCollectionId) = validateCollection("Programs", "Admin")
          if (mainCollectionPresent && mainCollectionId != 0) {
            val (programCollectionPresent, programCollectionId) = validateCollection(programCollectionName, "Admin", Some(targetedProgramId))
            if (programCollectionPresent && programCollectionId != 0) {
              val (solutionCollectionPresent, solutionCollectionId) = validateCollection(solutionCollectionName, "Admin", Some(targetedSolutionId))
              if (solutionCollectionPresent && solutionCollectionId != 0) {
                println(s"=====> Collection & Dashboard for solution: $solutionCollectionName is already present, Skipping this step.")
              } else {
                println(s"=====> Main and Program collection is present creating Solution collection & Dashboard.")
                createSolutionCollectionAndDashboard(programCollectionId, solutionCollectionName, solutionCollectionDescription, "Admin")
              }
            } else {
              println(s"=====> Main collection is present creating Program, Solution collection & Dashboard.")
              val programCollectionId = createProgramCollectionInsideMain(mainCollectionId, programCollectionName, programCollectionDescription, "Admin")
              createSolutionCollectionAndDashboard(programCollectionId, solutionCollectionName, solutionCollectionDescription, "Admin")
            }
          } else {
            println("=====> Main Program collection is not present creating Programs, Solution collection & Dashboard")
            val mainProgramCollectionId = createMainProgramsCollection
            val programCollectionId = createProgramCollectionInsideMain(mainProgramCollectionId, programCollectionName, programCollectionDescription, "Admin")
            createSolutionCollectionAndDashboard(programCollectionId, solutionCollectionName, solutionCollectionDescription, "Admin")
          }
        } else println("Either Solution or Program name is null")
        println("\n")

        /**
         * Logic to process and create Programs Collection and project solution Dashboard for Program Manager
         */
        if (targetedSolutionId.nonEmpty && solutionName.nonEmpty && targetedProgramId.nonEmpty && programName.nonEmpty) {
          val programCollectionName = s"$programName [org : $orgName]"
          val programCollectionDescription = s"Program Id: $targetedProgramId\n\nProgram External Id: $programExternalId\n\nCollection For: Program Manager\n\nProgram Description: $programDescription"
          val solutionCollectionName = s"$solutionName [Project]"
          val solutionCollectionDescription = s"Solution Id: $targetedSolutionId\n\nSolution External Id: $solutionExternalId\n\nCollection For: Program Manager\n\nSolution Description: $solutionDescription"

          val (programCollectionPresent, programCollectionId) = validateCollection(programCollectionName, "Program Manager", Some(targetedProgramId))
          if (programCollectionPresent && programCollectionId != 0) {
            val (solutionCollectionPresent, solutionCollectionId) = validateCollection(solutionCollectionName, "Program Manager", Some(targetedSolutionId))
            if (solutionCollectionPresent && solutionCollectionId != 0) {
              println(s"=====> Collection & Dashboard for solution: $solutionCollectionName is already present, Skipping this step.")
            } else {
              println(s"=====> Main and Program collection is present creating Solution collection & Dashboard.")
              createSolutionCollectionAndDashboard(programCollectionId, solutionCollectionName, solutionCollectionDescription, "Program Manager")
            }
          } else {
            println(s"=====> Main collection is present creating Program, Solution collection & Dashboard.")
            val programCollectionId = createProgramCollectionOutSideMain(targetedProgramId, programCollectionName, programCollectionDescription, "Program Manager")
            createSolutionCollectionAndDashboard(programCollectionId, solutionCollectionName, solutionCollectionDescription, "Program Manager")
          }
        } else println("Either Solution or Program name is null")
        println("\n")

        println(s">>>>>>>>>>> Completed Processing Metabase Project Dashboards >>>>>>>>>>>>")
    }


    def createMicroImprovementsCollectionAndDashboard(metaDataTable: String, reportConfig: String, metabaseDatabase: String): Unit = {
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$admin';"
      val (mipCollectionName, mipCollectionDescription) = (s"Micro Improvements", s"This collection contains dashboards that track the progress, participation, and effectiveness of Micro Improvement Projects across various programs.\n\nCollection For: Admin")
      val mipCollectionId = Utils.checkAndCreateCollection(mipCollectionName, mipCollectionDescription, metabaseUtil)
      if (mipCollectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, s"Report_Admin_Micro_Improvement", mipCollectionId)
        val (mipDashboardName, mipDashboardDescription) = (s"Overview - Across States and Programs", s"A consolidated view of project progress and user participation.")
        val (mipDashboardId, projectTabId, userTabId, csvTabId) = Utils.createMicroImprovementsDashboardAndTabs(mipCollectionId, mipDashboardName, mipDashboardDescription, metabaseUtil)
        if (projectTabId != -1 && userTabId != -1 && csvTabId != -1) {
          val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
          if (databaseId != -1) {
            val stateNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "state_name", postgresUtil, createDashboardQuery)
            val districtNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "district_name", postgresUtil, createDashboardQuery)
            val programNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "program_name", postgresUtil, createDashboardQuery)
            val blockNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "block_name", postgresUtil, createDashboardQuery)
            val clusterNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "cluster_name", postgresUtil, createDashboardQuery)
            val orgNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "org_name", postgresUtil, createDashboardQuery)
            val projectDetailsConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Admin' AND report_name = 'Project-Details';"
            val projectQuestionCardIdList = ProcessAdminConstructor.ProcessAndUpdateJsonFiles(projectDetailsConfigQuery, mipCollectionId, databaseId, mipDashboardId, projectTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, projects, solutions, metabaseUtil, postgresUtil)
            val userDetailsConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Admin' AND report_name = 'User-Details';"
            val userQuestionCardIdList = ProcessAdminConstructor.ProcessAndUpdateJsonFiles(userDetailsConfigQuery, mipCollectionId, databaseId, mipDashboardId, userTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, projects, solutions, metabaseUtil, postgresUtil)
            val submissionDetailsConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Admin' AND report_name = 'Submission-Details-CSV';"
            val submissionQuestionCardIdList = ProcessAdminConstructor.ProcessAndUpdateJsonFiles(submissionDetailsConfigQuery, mipCollectionId, databaseId, mipDashboardId, csvTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, projects, solutions, metabaseUtil, postgresUtil)
            val mainQuestionIdsString = "[" + (projectQuestionCardIdList ++ userQuestionCardIdList ++ submissionQuestionCardIdList).mkString(",") + "]"
            val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Project-Parameter' AND question_type = 'admin-parameter'"
            UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, mipDashboardId, postgresUtil)
            val projectMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", mipCollectionId).put("collectionName", mipCollectionName).put("dashboardId", mipDashboardId).put("dashboardName", mipDashboardName).put("questionIds", mainQuestionIdsString))
            postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$projectMetadataJson' ::jsonb WHERE entity_id = '$admin';")
          } else {
            println("Database Id returned -1")
          }
        } else {
          println(s"Creating tabs failed please check: $projectTabId, $userTabId, $csvTabId")
        }
      }
    }

    def createNationalOverviewCollectionAndDashboard(metaDataTable: String, reportConfig: String, metabaseDatabase: String): Unit = {
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$admin';"
      val (mainCollectionName, mainCollectionDescription) = (s"National Overview", s"A collection of dashboards that highlight progress and comparisons of Micro Improvement projects across states and districts.\n\nCollection For: Admin")
      val mainCollectionId = Utils.checkAndCreateCollection(mainCollectionName, mainCollectionDescription, metabaseUtil)
      if (mainCollectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, s"Report_Admin_National_Overview", mainCollectionId)
        val (homeDashboardName, homeDashboardDescription) = (s"National Dashboard", s"Centralized view of regional performance data for Micro Improvement programs.")
        val homeDashboardId: Int = Utils.createDashboard(mainCollectionId, homeDashboardName, homeDashboardDescription, metabaseUtil)
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId != -1) {
          val homeReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Mi-Dashboard' AND report_name = 'Home-Details-Report';"
          val homeQuestionCardIdList = HomePage.ProcessAndUpdateJsonFiles(homeReportConfigQuery, mainCollectionId, databaseId, homeDashboardId, projects, solutions, reportConfig, metaDataTable, metabaseUtil, postgresUtil)
          val homeQuestionIdsString = "[" + homeQuestionCardIdList.mkString(",") + "]"
          val filterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Mi-Dashboard-Filters' AND question_type = 'home-dashboard-filter'"
          val filterResults: List[Map[String, Any]] = postgresUtil.fetchData(filterQuery)
          val objectMapper = new ObjectMapper()
          val slugNameToStateIdFilterMap = mutable.Map[String, Int]()
          for (result <- filterResults) {
            val configString = result.get("config").map(_.toString).getOrElse("")
            val configJson = objectMapper.readTree(configString)
            val slugName = configJson.findValue("name").asText()
            val stateIdFilter: Int = HomePage.updateAndAddFilter(metabaseUtil, configJson: JsonNode, mainCollectionId, databaseId, projects, solutions)
            slugNameToStateIdFilterMap(slugName) = stateIdFilter
          }
          val parameterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Mi-Dashboard-Parameters' AND question_type = 'home-dashboard-parameter'"
          val immutableSlugNameToStateIdFilterMap: Map[String, Int] = slugNameToStateIdFilterMap.toMap
          HomePage.updateParameterFunction(metabaseUtil, postgresUtil, parameterQuery, immutableSlugNameToStateIdFilterMap, homeDashboardId)
          val homeMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", mainCollectionId).put("collectionName", mainCollectionName).put("dashboardId", homeDashboardId).put("dashboardName", homeDashboardName).put("questionIds", homeQuestionIdsString))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  mi_metadata = '$homeMetadataJson' WHERE entity_id = '$admin';")

          val (compareDashboardName, compareDashboardDescription) = (s"Region Comparison Dashboard", s"Compare Micro Improvement progress across states and districts using key metrics.")
          val compareDashboardId: Int = Utils.createDashboard(mainCollectionId, compareDashboardName, compareDashboardDescription, metabaseUtil)
          val compareReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Mi-Dashboard' AND report_name = 'Compare-Details-Report';"
          val stateNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "state_name", postgresUtil, createDashboardQuery)
          val districtNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "district_name", postgresUtil, createDashboardQuery)
          val compareReportQuestionIdList = ComparePage.ProcessAndUpdateJsonFiles(compareReportConfigQuery, mainCollectionId, databaseId, compareDashboardId, stateNameId, districtNameId, projects, solutions, metabaseUtil, postgresUtil)
          val compareQuestionIdsString = "[" + compareReportQuestionIdList.mkString(",") + "]"
          val compareParametersQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Mi-Dashboard-Parameters' AND question_type = 'compare-dashboard-parameter'"
          ComparePage.UpdateParameterFunction(metabaseUtil, compareParametersQuery, compareDashboardId, postgresUtil)
          val compareMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", mainCollectionId).put("collectionName", mainCollectionName).put("dashboardId", compareDashboardId).put("dashboardName", compareDashboardName).put("questionIds", compareQuestionIdsString))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  comparison_metadata = '$compareMetadataJson' WHERE entity_id = '$admin';")
          postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Success', error_message = '' WHERE entity_id = '$admin';")
        }
      }
    }

    //TODO: @Prashant put state and district function logic here

    def createMainProgramsCollection: Int = {
      val (mainCollectionName, mainCollectionDescription) = ("Programs", s"All programs made available on the platform are stored in this collection.\n\nCollection For: Admin")
      val mainCollectionId: Int = Utils.checkAndCreateCollection(mainCollectionName, mainCollectionDescription, metabaseUtil)
      if (mainCollectionId != -1) {
        CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, "Report_Admin_Programs", mainCollectionId)
        val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", mainCollectionId).put("collectionName", mainCollectionName))
        postgresUtil.insertData(s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb WHERE entity_id = '1';")
        mainCollectionId
      } else {
        -1
      }
    }

    def createProgramCollectionInsideMain(mainProgramsCollectionId: Int, programCollectionName: String, programCollectionDescription: String, reportFor: String): Int = {
      val programCollectionId = Utils.createCollection(programCollectionName.take(100), programCollectionDescription.take(255), metabaseUtil, Some(mainProgramsCollectionId))
      val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName).put("Collection For", reportFor))
      postgresUtil.insertData(s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJson' ::jsonb WHERE entity_id = '$targetedProgramId';")
      programCollectionId
    }

    def createProgramCollectionOutSideMain(programId: String, programCollectionName: String, programCollectionDescription: String, reportFor: String): Int = {
      val programCollectionId = Utils.createCollection(programCollectionName.take(100), programCollectionDescription.take(255), metabaseUtil)
      Utils.createGroupForCollection(metabaseUtil, s"Program_Manager_$programId".take(255), programCollectionId)
      val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName).put("Collection For", reportFor))
      postgresUtil.insertData(s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJson' ::jsonb WHERE entity_id = '$targetedProgramId';")
      programCollectionId
    }

    def createSolutionCollectionAndDashboard(programCollectionId: Int, solutionCollectionName: String, solutionCollectionDescription: String, reportFor: String): Unit = {
      val solutionCollectionId = Utils.createCollection(solutionCollectionName.take(100), solutionCollectionDescription.take(255), metabaseUtil, Some(programCollectionId))
      println(s">>> Created Solution collection with Id: $solutionCollectionId inside the parent collection Id: $programCollectionId")
      val (solutionDashboardName, solutionDashboardDescription) = ("Dashboard", "A consolidated view of project progress, usage patterns, and unique user participation in Micro Improvement.")
      val (solutionDashboardId, projectTabId, submissionCsvTabId, taskReportCsvTabId, statusReportCsvTabId) = Utils.createSolutionDashboardAndTabs(solutionCollectionId, solutionDashboardName, solutionDashboardDescription, metabaseUtil)
      if (projectTabId != -1 && submissionCsvTabId != -1 && taskReportCsvTabId != -1 && statusReportCsvTabId != -1) {
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId != -1) {
          val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedSolutionId';"
          val stateNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "state_name", postgresUtil, createDashboardQuery)
          val districtNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "district_name", postgresUtil, createDashboardQuery)
          val programNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, solutions, "program_name", postgresUtil, createDashboardQuery)
          val blockNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "block_name", postgresUtil, createDashboardQuery)
          val clusterNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "cluster_name", postgresUtil, createDashboardQuery)
          val orgNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, projects, "org_name", postgresUtil, createDashboardQuery)
          val projectDetailsConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Program' AND report_name = 'Project-Details';"
          val projectQuestionCardIdList = ProcessProgramConstructor.ProcessAndUpdateJsonFiles(projectDetailsConfigQuery, solutionCollectionId, databaseId, solutionDashboardId, projectTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, projects, solutions, tasks, metabaseUtil, postgresUtil, targetedProgramId, targetedSolutionId, config.evidenceBaseUrl)
          val submissionReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Program' AND report_name = 'Submission-Details-CSV';"
          val submissionQuestionCardIdList = ProcessProgramConstructor.ProcessAndUpdateJsonFiles(submissionReportConfigQuery, solutionCollectionId, databaseId, solutionDashboardId, submissionCsvTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, projects, solutions, tasks, metabaseUtil, postgresUtil, targetedProgramId, targetedSolutionId, config.evidenceBaseUrl)
          val taskReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Program' AND report_name = 'Task-Details-CSV';"
          val taskQuestionCardIdList = ProcessProgramConstructor.ProcessAndUpdateJsonFiles(taskReportConfigQuery, solutionCollectionId, databaseId, solutionDashboardId, taskReportCsvTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, projects, solutions, tasks, metabaseUtil, postgresUtil, targetedProgramId, targetedSolutionId, config.evidenceBaseUrl)
          val statusReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Program' AND report_name = 'Status-Details-CSV';"
          val statusQuestionCardIdList = ProcessProgramConstructor.ProcessAndUpdateJsonFiles(statusReportConfigQuery, solutionCollectionId, databaseId, solutionDashboardId, statusReportCsvTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, projects, solutions, tasks, metabaseUtil, postgresUtil, targetedProgramId, targetedSolutionId, config.evidenceBaseUrl)
          val mainQuestionIdsString = "[" + (projectQuestionCardIdList ++ submissionQuestionCardIdList ++ taskQuestionCardIdList ++ statusQuestionCardIdList).mkString(",") + "]"
          val filterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Project-Filters' AND question_type = 'program-filter'"
          val filterResults: List[Map[String, Any]] = postgresUtil.fetchData(filterQuery)
          val objectMapper = new ObjectMapper()
          val slugNameToProgramIdFilterMap = mutable.Map[String, Int]()
          for (result <- filterResults) {
            val configString = result.get("config").map(_.toString).getOrElse("")
            val configJson = objectMapper.readTree(configString)
            val slugName = configJson.findValue("name").asText()
            val programIdFilter: Int = UpdateAndAddProgramFilter.updateAndAddFilter(metabaseUtil, configJson, targetedProgramId, solutionCollectionId, databaseId, projects, solutions)
            slugNameToProgramIdFilterMap(slugName) = programIdFilter
          }
          val immutableSlugNameToProgramIdFilterMap: Map[String, Int] = slugNameToProgramIdFilterMap.toMap
          val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Project-Parameter' AND question_type = 'program-parameter'"
          UpdateParameters.updateParameterFunction(metabaseUtil, postgresUtil, parametersQuery, immutableSlugNameToProgramIdFilterMap, solutionDashboardId)
          val solutionMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", solutionCollectionId).put("collectionName", solutionCollectionName).put("Collection For", reportFor).put("dashboardId", solutionDashboardId).put("dashboardName", solutionDashboardName).put("questionIds", mainQuestionIdsString))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$solutionMetadataJson' ::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$targetedSolutionId';")
        }
        else {
          println("Database Id returned -1")
        }
      } else {
        println(s"Creating tabs failed please check: $projectTabId, $submissionCsvTabId, $taskReportCsvTabId", statusReportCsvTabId)
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