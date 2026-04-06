package org.shikshalokam.job.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.dashboard.creator.domain.Event
import org.shikshalokam.job.dashboard.creator.miDashboard.{ComparePage, DistrictPage, HomePage, StatePage}
import org.shikshalokam.job.dashboard.creator.task.ProjectMetabaseDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.immutable._
import scala.collection.mutable

class ProjectMetabaseDashboardFunction(config: ProjectMetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabasePostgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
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

    println(s"***************** Start of Processing the Metabase Project Dashboard Event with Id = ${event._id} *****************")

    val startTime = System.currentTimeMillis()
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
    val metabaseApiKey: String = config.metabaseKey
    val filterSync: String = event.filterSync
    val filterTable: String = event.filterTable
    val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
    val solutionName = postgresUtil.fetchData(s"""SELECT name FROM $solutions WHERE solution_id = '$targetedSolutionId'""").collectFirst { case map: Map[_, _] => map.getOrElse("name", "").toString }.getOrElse("")
    val targetedProgramId: String = Option(event.targetedProgram).map(_.trim).filter(_.nonEmpty).getOrElse(postgresUtil.fetchData(s"SELECT program_id FROM $solutions WHERE solution_id = '$targetedSolutionId'").collectFirst { case map: Map[_, _] => map.get("program_id").map(_.toString).getOrElse("") }.getOrElse(""))
    val programName = postgresUtil.fetchData(s"""SELECT program_name FROM $solutions WHERE solution_id = '$targetedSolutionId'""").collectFirst { case map: Map[_, _] => map.getOrElse("program_name", "").toString }.getOrElse("")
    val orgId = postgresUtil.fetchData(s"""SELECT org_id FROM $solutions WHERE program_id = '$targetedProgramId' AND org_id IS NOT NULL AND TRIM(org_id) <> '' LIMIT 1 """).collectFirst { case map: Map[_, _] => map.getOrElse("org_id", "").toString }.getOrElse("")
    val stateName = postgresUtil.fetchData(s"""SELECT entity_name FROM $metaDataTable WHERE entity_id = '$targetedStateId'""").collectFirst { case map: Map[_, _] => map.getOrElse("entity_name", "").toString }.getOrElse("")
    val districtName = postgresUtil.fetchData(s"""SELECT entity_name FROM $metaDataTable WHERE entity_id = '$targetedDistrictId'""").collectFirst { case map: Map[_, _] => map.getOrElse("entity_name", "").toString }.getOrElse("")
    val tenantId = postgresUtil.fetchData(s"""SELECT tenant_id FROM $projects WHERE solution_id = '$targetedSolutionId' AND tenant_id IS NOT NULL AND TRIM(tenant_id) <> '' LIMIT 1 """).collectFirst { case map: Map[_, _] => map.getOrElse("tenant_id", "").toString }.getOrElse("")
    val combinedQuery = s"SELECT program_description, program_external_id, external_id, description FROM $solutions WHERE solution_id = '$targetedSolutionId' LIMIT 1"
    val resultMap = postgresUtil.fetchData(combinedQuery).collectFirst { case map: Map[_, _] => map }.getOrElse(Map.empty[String, Any])
    val programExternalId = resultMap.get("program_external_id").map(_.toString).getOrElse("")
    val programDescription = resultMap.get("program_description").map(_.toString).getOrElse("")
    val solutionExternalId = resultMap.get("external_id").map(_.toString).getOrElse("")
    val solutionDescription = resultMap.get("description").map(_.toString).getOrElse("")
    val stateIdForDistrictId: String = postgresUtil.fetchData(s"""SELECT state_id FROM $projects WHERE district_id = '$targetedDistrictId' LIMIT 1 """).collectFirst { case map: Map[_, _] => map.getOrElse("state_id", "").toString }.getOrElse("")
    val stateNameForDistrictId: String = postgresUtil.fetchData(s"""SELECT entity_name FROM $metaDataTable WHERE entity_id = '$stateIdForDistrictId'""").collectFirst { case map: Map[_, _] => map.getOrElse("entity_name", "").toString }.getOrElse("")
    val tenantIdForDistrictId: String = postgresUtil.fetchData(s"""SELECT tenant_id FROM $projects WHERE state_id = '$stateIdForDistrictId' AND tenant_id IS NOT NULL AND TRIM(tenant_id) <> '' LIMIT 1 """).collectFirst { case map: Map[_, _] => map.getOrElse("tenant_id", "").toString }.getOrElse("")
    val storedTableIds = TrieMap.empty[(Int, String), Int]
    val storedColumnIds = TrieMap.empty[(Int, String), Int]

    println(s"admin: $admin")
    println(s"Targeted State ID: $targetedStateId")
    println(s"Targeted District ID: $targetedDistrictId")
    println(s"Targeted Program ID: $targetedProgramId")
    println(s"Targeted Program Name: $programName")
    println(s"Targeted Solution ID: $targetedSolutionId")
    println(s"Targeted Solution Name: $solutionName")

    event.reportType match {
      case "Project" =>
        println(s">>>>>>>>>>> Started Processing Metabase Project Dashboards >>>>>>>>>>>>")

        /**
         * Logic to process and create the Micro Improvements Dashboard
         */
        println("\n-->> Process Admin Micro Improvements Dashboard")
        val (mipCollectionPresent, mipCollectionId) = validateCollection("Micro Improvements", "Admin")
        if (mipCollectionPresent && mipCollectionId != 0) {
          println(s"=====> Micro Improvements collection present with id: $mipCollectionId, Skipping this step.")
        } else {
          println("=====> Creating Micro Improvements Collection And Dashboard")
          createMicroImprovementsCollectionAndDashboard(metaDataTable, reportConfig, databaseId)
        }

        /**
         * Logic to process and create National Overview Dashboard
         */
        println("\n-->> Process Admin Micro National Overview Dashboard")
        val (nationalCollectionPresent, nationalCollectionId) = validateCollection("National Overview", "Admin")
        if (nationalCollectionPresent && nationalCollectionId != 0) {
          println(s"=====> National Overview collection present with id: $nationalCollectionId, Skipping this step.")
        } else {
          println("=====> Creating National Overview Collection And Dashboard")
          createNationalOverviewCollectionAndDashboard(metaDataTable, reportConfig, metabaseDatabase)
        }

        /**
         * Logic to process and create State Micro Improvements & Overview Dashboard
         */
        println("\n-->> Process State Micro Improvements & Overview Dashboard")
        val stateReportConfigQueryForAdmin: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Mi-Dashboard' AND report_name IN ('State-Details-Report', 'State-Details-Table-For-Admin');"
        val stateReportConfigQueryForStateManager: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Mi-Dashboard' AND report_name IN ('State-Details-Report', 'State-Details-Table-For-State-Manager');"
        if (targetedStateId.nonEmpty && stateName.nonEmpty) {
          val (stateCollectionPresent, stateCollectionId) = validateCollection(s"$stateName State [Tenant : $tenantId]", "State Manager", Some(targetedStateId))
          if (stateCollectionPresent && stateCollectionId != 0) {
            println(s"=====> $stateName State collection present with id: $stateCollectionId, Skipping this step.")
          } else {
            val stateCollectionId = processStateDashboard(tenantId, stateName, targetedStateId, reportConfig, metaDataTable, databaseId, projects, solutions, metabaseUtil, postgresUtil, "State Manager")
            val (stateDashboardName, stateDashboardDescription) = (s"$stateName - State overview", s"This dashboard contains important metrics for $stateName state\n\nDashboard For: State Manager\n\nState Id: $targetedStateId")
            createStateOverviewDashboard(stateDashboardName, stateDashboardDescription, stateCollectionId, s"$stateName State [Tenant : $tenantId]", stateName, databaseId, stateReportConfigQueryForStateManager, metabaseUtil, postgresUtil, "State", "State Manager", "Yes")
            createDistrictComparisonDashboard(stateCollectionId, s"$stateName State [Tenant : $tenantId]", stateName, databaseId, metabaseUtil, postgresUtil, "State", "State Manager", "Yes")
          }

          println(s"\n-->> Process $stateName state inside National Overview Collection")
          val (collectionPresent, collectionId) = validateCollection("National Overview", "Admin")
          if (collectionPresent && collectionId != 0) {
            val (stateDashboardPresent, stateDashboardId) = validateDashboard(s"$stateName - State overview", "Admin", Some(targetedStateId))
            if (stateDashboardPresent && stateDashboardId != 0) {
              println(s"=====> $stateName - State overview dashboard already present inside National Overview collection with id: $stateDashboardId, Skipping this step.")
            } else {
              val (stateDashboardName, stateDashboardDescription) = (s"$stateName - State overview", s"This dashboard contains important metrics for $stateName state\n\nDashboard For: Admin\n\nState Id: $targetedStateId")
              createStateOverviewDashboard(stateDashboardName, stateDashboardDescription, collectionId, "National Overview", stateName, databaseId, stateReportConfigQueryForAdmin, metabaseUtil, postgresUtil, "Admin", "Admin", "No")
            }
          } else {
            println("=====> National Overview collection is not created.")
          }
        } else {
          println("Targeted State is not present or is empty")
        }

        /**
         * Logic to process and create District Micro Improvements & Overview Dashboard
         */
        println("\n-->> Process District Micro Improvements & Overview Dashboard")
        if (targetedDistrictId.nonEmpty && districtName.nonEmpty) {
          val (districtCollectionPresent, districtCollectionId) = validateCollection(s"$districtName District [Tenant : $tenantIdForDistrictId]", "District Manager", Some(targetedDistrictId))
          if (districtCollectionPresent && districtCollectionId != 0) {
            println(s"=====> $districtName district collection present with id: $targetedDistrictId, Skipping this step.")
          } else {
            println("=====> Creating $districtName district Collection And Dashboard")
            val districtCollectionId = processDistrictDashboard(tenantIdForDistrictId, stateNameForDistrictId, stateIdForDistrictId, districtName, targetedDistrictId, reportConfig, metaDataTable, databaseId, projects, solutions, metabaseUtil, postgresUtil, "District Manager")
            val (districtDashboardName, districtDashboardDescription) = (s"$districtName - District Overview", s"This dashboard contains important metrics for $districtName district in $stateName state\n\nDashboard For: District Manager\n\nDistrict Id: $targetedDistrictId")
            createDistrictOverviewDashboard(districtDashboardName, districtDashboardDescription, districtCollectionId, s"$districtName District [Tenant : $tenantIdForDistrictId]", districtName, databaseId, metabaseUtil, postgresUtil, "District", "District Manager", "Yes")
          }

          println(s"\n-->> Process $districtName district inside $stateName Collection")
          if (stateIdForDistrictId.nonEmpty && stateNameForDistrictId.nonEmpty) {
            val (stateCollectionPresent, stateCollectionId) = validateCollection(s"$stateName State [Tenant : $tenantId]", "State Manager", Some(targetedStateId))
            if (stateCollectionPresent && stateCollectionId != 0) {
              val (districtDashboardPresent, districtDashboardId) = validateDashboard(s"$districtName District [Tenant : $tenantIdForDistrictId]", "State Manager", Some(targetedDistrictId))
              if (districtDashboardPresent && districtDashboardId != 0) {
                println(s"=====> $districtName District [Tenant : $tenantIdForDistrictId] dashboard already present inside state collection with id: $districtDashboardId, Skipping this step.")
              } else {
                val (districtDashboardName, districtDashboardDescription) = (s"$districtName District [Tenant : $tenantIdForDistrictId]", s"This dashboard contains important metrics for $districtName district in $stateName state\n\nDashboard For: State Manager\n\nDistrict Id: $targetedDistrictId")
                createDistrictOverviewDashboard(districtDashboardName, districtDashboardDescription, stateCollectionId, s"$districtName District [Tenant : $tenantIdForDistrictId]", districtName, databaseId, metabaseUtil, postgresUtil, "State", "State Manager", "No")
              }
            } else {
              println(s"=====> $stateName State [Tenant : $tenantId] collection is not created.")
            }
          } else println("Targeted State given a district is not present or is empty")

          println(s"\n-->> Process $districtName district inside National Overview Collection")
          val (collectionPresent, collectionId) = validateCollection("National Overview", "Admin")
          if (collectionPresent && collectionId != 0) {
            val (districtDashboardPresent, districtDashboardId) = validateDashboard(s"$districtName District [Tenant : $tenantIdForDistrictId]", "Admin", Some(targetedDistrictId))
            if (districtDashboardPresent && districtDashboardId != 0) {
              println(s"=====> $districtName District [Tenant : $tenantIdForDistrictId] dashboard already present inside National Overview collection, Skipping this step.")
            } else {
              val (districtDashboardName, districtDashboardDescription) = (s"$districtName District [Tenant : $tenantIdForDistrictId]", s"This dashboard contains important metrics for $districtName district in $stateName state\n\nDashboard For: Admin\n\nDistrict Id: $targetedDistrictId")
              createDistrictOverviewDashboard(districtDashboardName, districtDashboardDescription, collectionId, s"$districtName District [Tenant : $tenantIdForDistrictId]", districtName, databaseId, metabaseUtil, postgresUtil, "Admin", "Admin", "No")
            }
          } else {
            println("=====> National Overview collection is not created.")
          }
        } else {
          println("Targeted District is not present or is empty")
        }

        /**
         * Logic to process and create Programs Collection and project solution Dashboard for Admin
         */
        println("\n-->> Process Admin Programs Collection and project solution Dashboard")
        if (targetedSolutionId.nonEmpty && solutionName.nonEmpty && targetedProgramId.nonEmpty && programName.nonEmpty) {
          val programCollectionName = s"$programName [org : $orgId]"
          val programCollectionDescription = s"Program Id: $targetedProgramId\n\nProgram External Id: $programExternalId\n\nCollection For: Admin\n\nProgram Description: $programDescription"
          val solutionCollectionName = s"$solutionName [Project]"
          val solutionCollectionDescription = s"Solution Id: $targetedSolutionId\n\nSolution External Id: $solutionExternalId\n\nCollection For: Admin\n\nSolution Description: $solutionDescription"

          val (mainCollectionPresent, mainCollectionId) = validateCollection("Programs", "Admin")
          if (mainCollectionPresent && mainCollectionId != 0) {
            val (programCollectionPresent, programCollectionId) = validateCollection(programCollectionName.take(100), "Admin", Some(targetedProgramId))
            if (programCollectionPresent && programCollectionId != 0) {
              val (solutionCollectionPresent, solutionCollectionId) = validateCollection(solutionCollectionName.take(100), "Admin", Some(targetedSolutionId))
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

        /**
         * Logic to process and create Programs Collection and project solution Dashboard for Program Manager
         */
        println("\n-->> Process Program Manager Collection and project solution Dashboard")
        if (targetedSolutionId.nonEmpty && solutionName.nonEmpty && targetedProgramId.nonEmpty && programName.nonEmpty) {
          val programCollectionName = s"$programName [org : $orgId]"
          val programCollectionDescription = s"Program Id: $targetedProgramId\n\nProgram External Id: $programExternalId\n\nCollection For: Program Manager\n\nProgram Description: $programDescription"
          val solutionCollectionName = s"$solutionName [Project]"
          val solutionCollectionDescription = s"Solution Id: $targetedSolutionId\n\nSolution External Id: $solutionExternalId\n\nCollection For: Program Manager\n\nSolution Description: $solutionDescription"

          val (programCollectionPresent, programCollectionId) = validateCollection(programCollectionName.take(100), "Program Manager", Some(targetedProgramId))
          if (programCollectionPresent && programCollectionId != 0) {
            val (solutionCollectionPresent, solutionCollectionId) = validateCollection(solutionCollectionName.take(100), "Program Manager", Some(targetedSolutionId))
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

        println(s">>>>>>>>>>> Completed Processing Metabase Project Dashboards >>>>>>>>>>>>")

        if (filterSync.nonEmpty) {
          val searchTableResponse = metabaseUtil.searchTable(filterTable, databaseId)
          val filterTableId: Int = extractTableId(searchTableResponse)
          if (filterTableId != -1) {
            metabaseUtil.discardValues(filterTableId)
            metabaseUtil.rescanValues(filterTableId)
          } else {
            println(s"Table does not exits in the metabase DB")
          }
          println("Successfully updated the filters values")
        }
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

    def createMicroImprovementsCollectionAndDashboard(metaDataTable: String, reportConfig: String, databaseId: Int): Unit = {
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$admin';"
      val (mipCollectionName, mipCollectionDescription) = (s"Micro Improvements", s"This collection contains dashboards that track the progress, participation, and effectiveness of Micro Improvement Projects across various programs.\n\nCollection For: Admin")
      val mipCollectionId = Utils.checkAndCreateCollection(mipCollectionName, mipCollectionDescription, metabaseUtil)
      if (mipCollectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, s"Report_Admin_Micro_Improvement", mipCollectionId)
        val (mipDashboardName, mipDashboardDescription) = (s"Overview - Across States and Programs", s"A consolidated view of project progress and user participation.")
        val (mipDashboardId, projectTabId, userTabId, csvTabId) = Utils.createMicroImprovementsDashboardAndTabs(mipCollectionId, mipDashboardName, mipDashboardDescription, metabaseUtil)
        if (projectTabId != -1 && userTabId != -1 && csvTabId != -1) {
          val stateNameId: Int = getTheColumnId(databaseId, projects, "state_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
          val districtNameId: Int = getTheColumnId(databaseId, projects, "district_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
          val programNameId: Int = getTheColumnId(databaseId, projects, "program_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
          val blockNameId: Int = getTheColumnId(databaseId, projects, "block_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
          val clusterNameId: Int = getTheColumnId(databaseId, projects, "cluster_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
          val orgNameId: Int = getTheColumnId(databaseId, projects, "org_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
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
          println(s"Creating tabs failed please check: $projectTabId, $userTabId, $csvTabId")
        }
      }
    }

    def processStateDashboard(tenantId: String, stateName: String, targetedStateId: String, reportConfig: String, metaDataTable: String, databaseId: Int, projects: String, solutions: String, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, reportFor: String): Int = {
      val collectionName = s"$stateName State [Tenant : $tenantId]"
      val collectionDescription = s"Tenant Id: $tenantId\n\nState Id: $targetedStateId\n\nCollection For: $reportFor\n\nCollection Description: This collection contains micro improvement dashboards for $stateName state"
      val metaDataStatusUpdateQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedStateId';"
      val collectionId: Int = Utils.createCollection(collectionName, collectionDescription, metabaseUtil)
      if (collectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, s"State_Manager_$targetedStateId", collectionId)
        val (dashboardName, dashboardDescription) = (s"Micro Improvements", s"Analytical overview of micro improvements for $stateName state")
        val (dashboardId, projectTabId, userTabId, csvTabId) = Utils.createMicroImprovementsDashboardAndTabs(collectionId, dashboardName, dashboardDescription, metabaseUtil)
        val stateNameId: Int = getTheColumnId(databaseId, projects, "state_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, metaDataStatusUpdateQuery)
        val districtNameId: Int = getTheColumnId(databaseId, projects, "district_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, metaDataStatusUpdateQuery)
        val programNameId: Int = getTheColumnId(databaseId, solutions, "program_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, metaDataStatusUpdateQuery)
        val blockNameId: Int = getTheColumnId(databaseId, projects, "block_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, metaDataStatusUpdateQuery)
        val clusterNameId: Int = getTheColumnId(databaseId, projects, "cluster_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, metaDataStatusUpdateQuery)
        val orgNameId: Int = getTheColumnId(databaseId, projects, "org_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, metaDataStatusUpdateQuery)
        val projectReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'State' AND report_name = 'Project-Details';"
        val projectQuestionCardIdList = ProcessStateConstructor.ProcessAndUpdateJsonFiles(projectReportConfigQuery, collectionId, databaseId, dashboardId, projectTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, projects, solutions, metabaseUtil, postgresUtil, targetedStateId)
        val userReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'State' AND report_name = 'User-Details';"
        val userQuestionCardIdList = ProcessStateConstructor.ProcessAndUpdateJsonFiles(userReportConfigQuery, collectionId, databaseId, dashboardId, userTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, projects, solutions, metabaseUtil, postgresUtil, targetedStateId)
        val csvQuestionConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'State' AND report_name = 'Csv-Details';"
        val csvQuestionCardIdList = ProcessStateConstructor.ProcessAndUpdateJsonFiles(csvQuestionConfigQuery, collectionId, databaseId, dashboardId, csvTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, projects, solutions, metabaseUtil, postgresUtil, targetedStateId)
        val mainQuestionIdsString = "[" + (projectQuestionCardIdList ++ userQuestionCardIdList ++ csvQuestionCardIdList).mkString(",") + "]"
        val filterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Project-Filters' AND question_type = 'state-filter'"
        val filterResults: List[Map[String, Any]] = postgresUtil.fetchData(filterQuery)
        val objectMapper = new ObjectMapper()
        val slugNameToStateIdFilterMap = mutable.Map[String, Int]()
        for (result <- filterResults) {
          val configString = result.get("config").map(_.toString).getOrElse("")
          val configJson = objectMapper.readTree(configString)
          val slugName = configJson.findValue("name").asText()
          val stateIdFilter: Int = UpdateAndAddStateFilter.updateAndAddFilter(metabaseUtil, configJson: JsonNode, s"$targetedStateId", collectionId, databaseId, projects, solutions)
          slugNameToStateIdFilterMap(slugName) = stateIdFilter
        }
        val immutableSlugNameToStateIdFilterMap: Map[String, Int] = slugNameToStateIdFilterMap.toMap
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Project-Parameter' AND question_type = 'state-parameter'"
        UpdateParameters.updateParameterFunction(metabaseUtil, postgresUtil, parametersQuery, immutableSlugNameToStateIdFilterMap, dashboardId)
        val mainMetadataJson = new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName).put("Collection For", reportFor).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", mainQuestionIdsString)
        postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = '$mainMetadataJson' WHERE entity_id = '$targetedStateId';")
      }
      collectionId
    }

    def processDistrictDashboard(tenantId: String, stateName: String, targetedStateId: String, districtName: String, targetedDistrictId: String, reportConfig: String, metaDataTable: String, databaseId: Int, projects: String, solutions: String, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, reportFor: String): Int = {
      val collectionName = s"$districtName District [Tenant : $tenantId]"
      val collectionDescription = s"Tenant Id: $tenantId\n\nDistrict Id: $targetedDistrictId\n\nCollection For: $reportFor\n\nCollection Description: This collection contains micro improvement dashboards for $districtName district in $stateName state"
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedDistrictId';"
      val collectionId: Int = Utils.createCollection(collectionName, collectionDescription, metabaseUtil)
      if (collectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, s"District_Manager_$targetedDistrictId", collectionId)
        val (dashboardName, dashboardDescription) = (s"Micro Improvements", s"Analytical overview of micro improvements for $districtName district")
        val (dashboardId, projectTabId, statusTabId, csvTabId) = Utils.createMicroImprovementsDashboardAndTabs(collectionId, dashboardName, dashboardDescription, metabaseUtil)
        val stateNameId: Int = getTheColumnId(databaseId, projects, "state_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val districtNameId: Int = getTheColumnId(databaseId, projects, "district_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val programNameId: Int = getTheColumnId(databaseId, solutions, "program_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val blockNameId: Int = getTheColumnId(databaseId, projects, "block_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val clusterNameId: Int = getTheColumnId(databaseId, projects, "cluster_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val orgNameId: Int = getTheColumnId(databaseId, projects, "org_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val projectReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'District' AND report_name = 'Project-Details';"
        val projectQuestionCardIdList = ProcessDistrictConstructor.ProcessAndUpdateJsonFiles(projectReportConfigQuery, collectionId, databaseId, dashboardId, projectTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, metabaseUtil, postgresUtil, projects, solutions, targetedStateId, targetedDistrictId)
        val userReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'District' AND report_name = 'User-Details';"
        val userQuestionCardIdList = ProcessDistrictConstructor.ProcessAndUpdateJsonFiles(userReportConfigQuery, collectionId, databaseId, dashboardId, statusTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, metabaseUtil, postgresUtil, projects, solutions, targetedStateId, targetedDistrictId)
        val csvQuestionConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'District' AND report_name = 'Csv-Details';"
        val csvQuestionCardIdList = ProcessDistrictConstructor.ProcessAndUpdateJsonFiles(csvQuestionConfigQuery, collectionId, databaseId, dashboardId, csvTabId, stateNameId, districtNameId, programNameId, blockNameId, clusterNameId, orgNameId, metabaseUtil, postgresUtil, projects, solutions, targetedStateId, targetedDistrictId)
        val mainQuestionIdsString = "[" + (projectQuestionCardIdList ++ userQuestionCardIdList ++ csvQuestionCardIdList).mkString(",") + "]"
        val filterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Project-Filters' AND question_type = 'district-filter'"
        val filterResults: List[Map[String, Any]] = postgresUtil.fetchData(filterQuery)
        val objectMapper = new ObjectMapper()
        val slugNameToDistrictIdFilterMap = mutable.Map[String, Int]()
        for (result <- filterResults) {
          val configString = result.get("config").map(_.toString).getOrElse("")
          val configJson = objectMapper.readTree(configString)
          val slugName = configJson.findValue("name").asText()
          val districtIdFilter: Int = UpdateAndAddDistrictFilter.updateAndAddFilter(metabaseUtil, configJson, targetedStateId, targetedDistrictId, collectionId, databaseId, projects, solutions)
          slugNameToDistrictIdFilterMap(slugName) = districtIdFilter
        }
        val immutableSlugNameToDistrictIdFilterMap: Map[String, Int] = slugNameToDistrictIdFilterMap.toMap
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Project-Parameter' AND question_type = 'district-parameter'"
        UpdateParameters.updateParameterFunction(metabaseUtil, postgresUtil, parametersQuery, immutableSlugNameToDistrictIdFilterMap, dashboardId)

        val districtMetadataJson = new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName).put("Collection For", reportFor).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", mainQuestionIdsString)
        postgresUtil.insertData(s"UPDATE $metaDataTable SET main_metadata = '$districtMetadataJson' WHERE entity_id = '$targetedDistrictId';")
      }
      collectionId
    }

    def createNationalOverviewCollectionAndDashboard(metaDataTable: String, reportConfig: String, metabaseDatabase: String): Unit = {
      val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE id = '$admin';"
      val (mainCollectionName, mainCollectionDescription) = (s"National Overview", s"A collection of dashboards that highlight progress and comparisons of Micro Improvement projects across states and districts.\n\nCollection For: Admin")
      val mainCollectionId = Utils.checkAndCreateCollection(mainCollectionName, mainCollectionDescription, metabaseUtil)
      if (mainCollectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, s"Report_Admin_National_Overview", mainCollectionId)
        val (homeDashboardName, homeDashboardDescription) = (s"National Dashboard", s"Centralized view of regional performance data for Micro Improvement programs.")
        val homeDashboardId: Int = Utils.createDashboard(mainCollectionId, homeDashboardName, homeDashboardDescription, metabaseUtil, "Yes")
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
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
        val compareDashboardId: Int = Utils.createDashboard(mainCollectionId, compareDashboardName, compareDashboardDescription, metabaseUtil, "Yes")
        val compareReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Mi-Dashboard' AND report_name = 'Compare-Details-Report';"
        val stateNameId: Int = getTheColumnId(databaseId, projects, "state_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val districtNameId: Int = getTheColumnId(databaseId, projects, "district_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val compareReportQuestionIdList = ComparePage.ProcessAndUpdateJsonFiles(compareReportConfigQuery, mainCollectionId, databaseId, compareDashboardId, stateNameId, districtNameId, projects, solutions, metabaseUtil, postgresUtil)
        val compareQuestionIdsString = "[" + compareReportQuestionIdList.mkString(",") + "]"
        val compareParametersQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Mi-Dashboard-Parameters' AND question_type = 'compare-dashboard-parameter'"
        ComparePage.UpdateParameterFunction(metabaseUtil, compareParametersQuery, compareDashboardId, postgresUtil)
        val compareMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", mainCollectionId).put("collectionName", mainCollectionName).put("dashboardId", compareDashboardId).put("dashboardName", compareDashboardName).put("questionIds", compareQuestionIdsString))
        postgresUtil.insertData(s"UPDATE $metaDataTable SET  comparison_metadata = '$compareMetadataJson' WHERE entity_id = '$admin';")
        postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Success' WHERE entity_id = '$admin';")
      }
    }

    def createStateOverviewDashboard(stateDashboardName: String, stateDashboardDescription: String, parentCollectionId: Int, parentCollectionName: String, stateName: String, databaseId: Int, stateReportConfigQuery: String, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, processType: String, reportFor: String, pinDashboard: String): Unit = {
      println("\nProcessing State overview logic")
      if (parentCollectionId != -1) {
        val stateDashboardId: Int = Utils.createDashboard(parentCollectionId, stateDashboardName, stateDashboardDescription, metabaseUtil, pinDashboard)
        val stateQuestionCardIdList = StatePage.ProcessAndUpdateJsonFiles(stateReportConfigQuery, parentCollectionId, databaseId, stateDashboardId, projects, solutions, metaDataTable, reportConfig, metabaseUtil, postgresUtil, targetedStateId, stateName)
        val stateQuestionIdsString = "[" + stateQuestionCardIdList.mkString(",") + "]"
        val stateFilterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Mi-Dashboard-Filters' AND question_type = 'state-dashboard-filter'"
        val stateFilterResults: List[Map[String, Any]] = postgresUtil.fetchData(stateFilterQuery)
        val stateObjectMapper = new ObjectMapper()
        val slugNameToStateIdFilterMapForState = mutable.Map[String, Int]()
        for (result <- stateFilterResults) {
          val configString = result.get("config").map(_.toString).getOrElse("")
          val configJson = stateObjectMapper.readTree(configString)
          val slugName = configJson.findValue("name").asText()
          val stateIdFilter: Int = StatePage.updateAndAddFilter(metabaseUtil, configJson: JsonNode, targetedStateId, parentCollectionId, databaseId, projects, solutions)
          slugNameToStateIdFilterMapForState(slugName) = stateIdFilter
        }
        val parameterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Mi-Dashboard-Parameters' AND question_type = 'state-dashboard-parameter'"
        val stateImmutableSlugNameToStateIdFilterMap: Map[String, Int] = slugNameToStateIdFilterMapForState.toMap
        StatePage.updateParameterFunction(metabaseUtil, postgresUtil, parameterQuery, stateImmutableSlugNameToStateIdFilterMap, stateDashboardId)
        if (processType == "Admin") {
          val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", parentCollectionName).put("Collection For", reportFor)
            .put("dashboardId", stateDashboardId).put("dashboardName", stateDashboardName).put("questionIds", stateQuestionIdsString))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET mi_metadata = COALESCE(mi_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb, state_details_url_admin = '$domainName$stateDashboardId', status = 'Success' WHERE entity_id = '$targetedStateId';")
        } else {
          val stateMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", parentCollectionName).put("Collection For", reportFor)
            .put("dashboardId", stateDashboardId).put("dashboardName", stateDashboardName).put("questionIds", stateQuestionIdsString))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET mi_metadata = COALESCE(mi_metadata::jsonb, '[]'::jsonb) || '$stateMetadataJson' ::jsonb, state_details_url_state = '$domainName$stateDashboardId', status = 'Success' WHERE entity_id = '$targetedStateId';")
        }
      } else {
        println(s"Parent collection ID is -1 for $stateName state dashboard hence skipping the creation of state dashboard.")
      }
    }

    def createDistrictOverviewDashboard(districtDashboardName: String, dashboardDescription: String, parentCollectionId: Int, parentCollectionName: String, districtName: String, databaseId: Int, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, processType: String, reportFor: String, pinDashboard: String): Unit = {
      println("\nProcessing District overview logic")
      if (parentCollectionId != -1) {
        val districtDashboardId: Int = Utils.createDashboard(parentCollectionId, districtDashboardName, dashboardDescription, metabaseUtil, pinDashboard)
        val districtReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Mi-Dashboard' AND report_name = 'District-Details-Report';"
        val districtQuestionCardIdList = DistrictPage.ProcessAndUpdateJsonFiles(districtReportConfigQuery, parentCollectionId, databaseId, districtDashboardId, projects, solutions, reportConfig, metabaseUtil, postgresUtil, targetedDistrictId, districtName)
        val districtQuestionIdsString = "[" + districtQuestionCardIdList.mkString(",") + "]"
        val districtFilterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Mi-Dashboard-Filters' AND question_type = 'district-dashboard-filter'"
        val districtFilterResults: List[Map[String, Any]] = postgresUtil.fetchData(districtFilterQuery)
        val districtObjectMapper = new ObjectMapper()
        val slugNameToStateIdFilterMapForState = mutable.Map[String, Int]()
        for (result <- districtFilterResults) {
          val configString = result.get("config").map(_.toString).getOrElse("")
          val configJson = districtObjectMapper.readTree(configString)
          val slugName = configJson.findValue("name").asText()
          val stateIdFilter: Int = DistrictPage.updateAndAddFilter(metabaseUtil, configJson: JsonNode, targetedDistrictId, parentCollectionId, databaseId, projects, solutions)
          slugNameToStateIdFilterMapForState(slugName) = stateIdFilter
        }
        val parameterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Mi-Dashboard-Parameters' AND question_type = 'district-dashboard-parameter'"
        val stateImmutableSlugNameToStateIdFilterMap: Map[String, Int] = slugNameToStateIdFilterMapForState.toMap
        DistrictPage.updateParameterFunction(metabaseUtil, postgresUtil, parameterQuery, stateImmutableSlugNameToStateIdFilterMap, districtDashboardId)
        if (processType == "Admin") {
          val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", parentCollectionName).put("Collection For", reportFor)
            .put("dashboardId", districtDashboardId).put("dashboardName", districtDashboardName).put("questionIds", districtQuestionIdsString))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET mi_metadata = COALESCE(mi_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb, district_details_url_admin = '$domainName$districtDashboardId', status = 'Success' WHERE entity_id = '$targetedDistrictId';")
        } else if (processType == "State") {
          val stateMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", parentCollectionName).put("Collection For", reportFor)
            .put("dashboardId", districtDashboardId).put("dashboardName", districtDashboardName).put("questionIds", districtQuestionIdsString))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET mi_metadata = COALESCE(mi_metadata::jsonb, '[]'::jsonb) || '$stateMetadataJson' ::jsonb, district_details_url_state = '$domainName$districtDashboardId', status = 'Success' WHERE entity_id = '$targetedDistrictId';")
        }
        else {
          val districtMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", parentCollectionName).put("Collection For", reportFor)
            .put("dashboardId", districtDashboardId).put("dashboardName", districtDashboardName).put("questionIds", districtQuestionIdsString))
          postgresUtil.insertData(s"UPDATE $metaDataTable SET mi_metadata = COALESCE(mi_metadata::jsonb, '[]'::jsonb) || '$districtMetadataJson' ::jsonb, district_details_url_district = '$domainName$districtDashboardId', status = 'Success' WHERE entity_id = '$targetedDistrictId';")
        }
      } else {
        println(s"Parent collection ID is -1 for $parentCollectionName hence skipping dashboard creation.")
      }
    }

    def createDistrictComparisonDashboard(parentCollectionId: Int, parentCollectionName: String, stateName: String, databaseId: Int, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, processType: String, reportFor: String, pinDashboard: String): Unit = {
      println("\nProcessing District Comparison logic")
      val (districtCompareDashboardName, districtCompareDashboardDescription) = (s"Compare Districts [$stateName]", s"Compare micro improvement progress across districts in $stateName state")
      val compareDistrictDashboardId: Int = Utils.createDashboard(parentCollectionId, districtCompareDashboardName, districtCompareDashboardDescription, metabaseUtil, pinDashboard)
      val compareReportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Mi-Dashboard' AND report_name = 'Compare-District-Details-Report';"
      val compareDistrictReportQuestionCardIdList = StatePage.ProcessAndUpdateJsonFiles(compareReportConfigQuery, parentCollectionId, databaseId, compareDistrictDashboardId, projects, solutions, metaDataTable, reportConfig, metabaseUtil, postgresUtil, targetedStateId, stateName)
      val compareDistrictReportQuestionIdsString = "[" + compareDistrictReportQuestionCardIdList.mkString(",") + "]"
      val compareReportFilterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Mi-Dashboard-Filters' AND question_type = 'compare-district-dashboard-filter'"
      val compareReportFilterResults: List[Map[String, Any]] = postgresUtil.fetchData(compareReportFilterQuery)
      val compareDistrictObjectMapper = new ObjectMapper()
      val slugNameToDistrictIdFilterMap = mutable.Map[String, Int]()
      for (result <- compareReportFilterResults) {
        val configString = result.get("config").map(_.toString).getOrElse("")
        val configJson = compareDistrictObjectMapper.readTree(configString)
        val slugName = configJson.findValue("name").asText()
        val districtIdFilter: Int = StatePage.updateAndAddFilter(metabaseUtil, configJson: JsonNode, targetedStateId, parentCollectionId, databaseId, projects, solutions)
        slugNameToDistrictIdFilterMap(slugName) = districtIdFilter
      }
      val compareReportParameterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Mi-Dashboard-Parameters' AND question_type = 'compare-district-dashboard-parameter'"
      val immutableSlugNameToDistrictIdFilterMap: Map[String, Int] = slugNameToDistrictIdFilterMap.toMap
      StatePage.updateParameterFunction(metabaseUtil, postgresUtil, compareReportParameterQuery, immutableSlugNameToDistrictIdFilterMap, compareDistrictDashboardId)

      val stateMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", parentCollectionId).put("collectionName", parentCollectionName).put("Collection For", reportFor)
        .put("dashboardId", compareDistrictDashboardId).put("dashboardName", districtCompareDashboardName).put("questionIds", compareDistrictReportQuestionIdsString))
      postgresUtil.insertData(s"UPDATE $metaDataTable SET comparison_metadata = COALESCE(comparison_metadata::jsonb, '[]'::jsonb) || '$stateMetadataJson' ::jsonb, status = 'Success' WHERE entity_id = '$targetedStateId';")
    }

    def createMainProgramsCollection: Int = {
      val (mainCollectionName, mainCollectionDescription) = ("Programs", s"All programs made available on the platform are stored in this collection.\n\nCollection For: Admin")
      val mainCollectionId: Int = Utils.checkAndCreateCollection(mainCollectionName, mainCollectionDescription, metabaseUtil)
      if (mainCollectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, "Report_Admin_Programs", mainCollectionId)
        val adminMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", mainCollectionId).put("collectionName", mainCollectionName))
        postgresUtil.insertData(s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$adminMetadataJson' ::jsonb, status = 'Success' WHERE entity_id = '1';")
        mainCollectionId
      } else {
        -1
      }
    }

    def createProgramCollectionInsideMain(mainProgramsCollectionId: Int, programCollectionName: String, programCollectionDescription: String, reportFor: String): Int = {
      val programCollectionId = Utils.createCollection(programCollectionName.take(100), programCollectionDescription.take(255), metabaseUtil, Some(mainProgramsCollectionId))
      val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName).put("Collection For", reportFor))
      val programMetadataJsonString = programMetadataJson.toString.replace("'", "''")
      postgresUtil.insertData(s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJsonString' ::jsonb, status = 'Success' WHERE entity_id = '$targetedProgramId';")
      programCollectionId
    }

    def createProgramCollectionOutSideMain(programId: String, programCollectionName: String, programCollectionDescription: String, reportFor: String): Int = {
      val programCollectionId = Utils.createCollection(programCollectionName.take(100), programCollectionDescription.take(255), metabaseUtil)
      Utils.createGroupForCollection(metabaseUtil, s"Program_Manager_$programId".take(255), programCollectionId)
      val programMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", programCollectionId).put("collectionName", programCollectionName).put("Collection For", reportFor))
      val programMetadataJsonString = programMetadataJson.toString.replace("'", "''")
      postgresUtil.insertData(s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$programMetadataJsonString' ::jsonb, status = 'Success' WHERE entity_id = '$targetedProgramId';")
      programCollectionId
    }

    def createSolutionCollectionAndDashboard(programCollectionId: Int, solutionCollectionName: String, solutionCollectionDescription: String, reportFor: String): Unit = {
      val solutionCollectionId = Utils.createCollection(solutionCollectionName.take(100), solutionCollectionDescription.take(255), metabaseUtil, Some(programCollectionId))
      println(s">>> Created Solution collection with Id: $solutionCollectionId inside the parent collection Id: $programCollectionId")
      val (solutionDashboardName, solutionDashboardDescription) = ("Dashboard", "A consolidated view of project progress, usage patterns, and unique user participation in Micro Improvement.")
      val (solutionDashboardId, projectTabId, submissionCsvTabId, taskReportCsvTabId, statusReportCsvTabId) = Utils.createSolutionDashboardAndTabs(solutionCollectionId, solutionDashboardName, solutionDashboardDescription, metabaseUtil)
      if (projectTabId != -1 && submissionCsvTabId != -1 && taskReportCsvTabId != -1 && statusReportCsvTabId != -1) {
        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$targetedSolutionId';"
        val stateNameId: Int = getTheColumnId(databaseId, projects, "state_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val districtNameId: Int = getTheColumnId(databaseId, projects, "district_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val programNameId: Int = getTheColumnId(databaseId, solutions, "program_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val blockNameId: Int = getTheColumnId(databaseId, projects, "block_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val clusterNameId: Int = getTheColumnId(databaseId, projects, "cluster_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val orgNameId: Int = getTheColumnId(databaseId, projects, "org_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
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
        val solutionMetadataJsonString = solutionMetadataJson.toString.replace("'", "''")
        postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$solutionMetadataJsonString' ::jsonb, status = 'Success' WHERE entity_id = '$targetedSolutionId';")
      } else {
        println(s"Creating tabs failed please check: $projectTabId, $submissionCsvTabId, $taskReportCsvTabId", statusReportCsvTabId)
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
              val errorMessage =
                s"Column '$columnName' not found in table '$tableName' (tableId: $tableId)"
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
                  desc.contains(s"Program Id: $id") || desc.contains(s"Solution Id: $id") || desc.contains(s"State Id: $id") || desc.contains(s"District Id: $id") || desc.contains(s"Tenant Id: $id")
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

    def validateDashboard(dashboardName: String, reportFor: String, reportId: Option[String] = None): (Boolean, Int) = {
      val mapper = new ObjectMapper()
      println(s">>> Checking Metabase API for dashboard: $dashboardName")
      try {
        val collections = mapper.readTree(metabaseUtil.listDashboards())
        val result = collections match {
          case arr: ArrayNode =>
            arr.asScala.find { c =>
                val name = Option(c.get("name")).map(_.asText).getOrElse("")
                val desc = Option(c.get("description")).map(_.asText).getOrElse("")

                val matchesName = name == dashboardName
                val matchesReportFor = desc.contains(s"Dashboard For: $reportFor")
                val matchesReportId = reportId.forall(id =>
                  desc.contains(s"State Id: $id") || desc.contains(s"District Id: $id")
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
    println(s"***************** End of Processing the Metabase Project Dashboard *****************\n")
  }

}