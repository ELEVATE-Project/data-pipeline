package org.shikshalokam.job.observation.dashboard.creator.functions

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
    val metaDataTable = config.dashboard_metadata
    val report_config: String = config.report_config
    val metabaseDatabase: String = config.metabaseDatabase
    val solution_id = event.solution_id
    val domainTable = s"${solution_id}_domain"
    val questionTable = s"${solution_id}_questions"
    val chartType = event.chartType
    println(s"domainTable: $domainTable")
    println(s"questionTable: $questionTable")
    println(s"chartType: $chartType")
    println(s"isRubric: ${event.isRubric}")
    println(s"solution_name : ${event.solutionName}")

    event.reportType match {
      case "Observation" =>
        println(s">>>>>>>>>>> Started Processing Metabase Observation Dashboards >>>>>>>>>>>>")
        chartType.foreach {
          case ("domain") =>
            if (event.isRubric == "true") {
              println("Executing domain part...")
              try {
                val solutionName: String = event.solutionName
                val collectionName: String = s"Observation Domain Collection[$solutionName]"
                val dashboardName: String = s"Observation Domain Report[$solutionName]"
                val groupName: String = s"${solutionName}_Domain_Program_Manager"
                val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$solution_id';"
                val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"${solution_id}_domain Report", metabaseUtil, postgresUtil, createDashboardQuery)
                val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
                val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
                metabaseUtil.syncDatabaseAndRescanValues(databaseId)
                val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "state_name", postgresUtil, createDashboardQuery)
                val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "district_name", postgresUtil, createDashboardQuery)
                metabaseUtil.updateColumnCategory(statenameId,"State")
                metabaseUtil.updateColumnCategory(districtnameId,"City")
                val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "school_name", postgresUtil, createDashboardQuery)
                val clusterId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "cluster_name", postgresUtil, createDashboardQuery)
                val domainId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "domain", postgresUtil, createDashboardQuery)
                val subDomainId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "subdomain", postgresUtil, createDashboardQuery)
                val criteriaId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, domainTable, "criteria", postgresUtil, createDashboardQuery)
                val reportConfigQuery: String = s"SELECT question_type, config FROM $report_config WHERE dashboard_name = 'Domain' AND report_name = 'Domain-Report';"
                val questionCardIdList = UpdateDomainJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, domainId, subDomainId ,criteriaId, domainTable, metabaseUtil, postgresUtil, report_config)
                val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
                val parametersQuery: String = s"SELECT config FROM $report_config WHERE dashboard_name = 'Domain' AND question_type = 'Domain-Parameter'"
                UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
//                val updateTableQuery = s"UPDATE $metaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', quest ion_ids = '$questionIdsString', status = 'Success', error_message = '' WHERE entity_id = '$solution_id';"
//                postgresUtil.insertData(updateTableQuery)
//                CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
              } catch {
                case e: Exception =>
//                  val updateTableQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE entity_id = '$solution_id';"
//                  postgresUtil.insertData(updateTableQuery)
                  println(s"An error occurred: ${e.getMessage}")
                  e.printStackTrace()
              }
            }

          case ("question") =>
            println("Executing question part...")
            try {
              val solutionName: String = event.solutionName
              val collectionName: String = s"Observation Question Collection[$solutionName]"
              val dashboardName: String = s"Observation Question Report[$solutionName]"
              val groupName: String = s"${solutionName}_Question_Program_Manager"
              val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$solution_id';"
              val collectionId: Int = CreateDashboard.checkAndCreateCollection(collectionName, s"${solution_id}_question Report", metabaseUtil, postgresUtil, createDashboardQuery)
              val dashboardId: Int = CreateDashboard.checkAndCreateDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil, createDashboardQuery)
              val databaseId: Int = CreateDashboard.getDatabaseId(metabaseDatabase, metabaseUtil)
              metabaseUtil.syncDatabaseAndRescanValues(databaseId)
              val statenameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "state_name", postgresUtil, createDashboardQuery)
              val districtnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "district_name", postgresUtil, createDashboardQuery)
              val clusterId : Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "cluster_name", postgresUtil, createDashboardQuery)
              val schoolnameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "school_name", postgresUtil, createDashboardQuery)
              val domainId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "domain_name", postgresUtil, createDashboardQuery)
              val criteriaId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, questionTable, "criteria_name", postgresUtil, createDashboardQuery)
              metabaseUtil.updateColumnCategory(statenameId,"State")
              metabaseUtil.updateColumnCategory(districtnameId,"City")
              val questionCardIdList = UpdateQuestionJsonFiles.ProcessAndUpdateJsonFiles(collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolnameId, clusterId, domainId, criteriaId, questionTable, metabaseUtil, postgresUtil, report_config)
              val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
              val parametersQuery: String = s"SELECT config FROM $report_config WHERE dashboard_name = 'Question' AND question_type = 'Question-Parameter'"
              UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
//              val updateTableQuery = s"UPDATE $metaDataTable SET  collection_id = '$collectionId', dashboard_id = '$dashboardId', question_ids = '$questionIdsString', status = 'Success', error_message = '' WHERE entity_id = '$solution_id';"
//              postgresUtil.insertData(updateTableQuery)
//              CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
            } catch {
              case e: Exception =>
//                val updateTableQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}'  WHERE entity_id = '$solution_id';"
//                postgresUtil.insertData(updateTableQuery)
                println(s"An error occurred: ${e.getMessage}")
                e.printStackTrace()
            }
        }
        println(s"***************** End of Processing the Metabase Observation Dashboard *****************\n")
    }
  }
}
