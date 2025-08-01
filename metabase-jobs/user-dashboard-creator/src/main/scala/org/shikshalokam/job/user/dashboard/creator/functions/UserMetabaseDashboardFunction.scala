package org.shikshalokam.job.user.dashboard.creator.functions

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.user.dashboard.creator.domain.Event
import org.shikshalokam.job.user.dashboard.creator.task.UserMetabaseDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory
import scala.collection.immutable._

class UserMetabaseDashboardFunction(config: UserMetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserMetabaseDashboardFunction])

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

    println(s"***************** Start of Processing the User Metabase Dashboard Event with = ${event.tenantCode} *****************")

    //TODO search success and failed keyword
    val metaDataTable = config.dashboardMetadata
    val userMetrics: String = config.userMetrics
    val metabaseDatabase: String = config.metabaseDatabase
    val reportConfig: String = config.reportConfig
    val tenantCode: String = event.tenantCode
    val tenantUserMetadataTable = s"${tenantCode}_users_metadata"
    val tenantUserTable = s"${tenantCode}_users"
    val tenantName = postgresUtil.fetchData(s"""SELECT entity_name FROM $metaDataTable WHERE entity_id = '$tenantCode'""").collectFirst { case map: Map[_, _] => map.getOrElse("entity_name", "").toString }.getOrElse("")

    //     ----- Report Admin Collection -----
    println("~~~~~~~~ Start Report Admin Collection Processing ~~~~~~~~")
    val reportAdminCollectionName = s"User Activity"
    val reportAdminCheckQuery =
      s"""SELECT CASE
         |WHEN main_metadata::jsonb @> '[{"collectionName":"$reportAdminCollectionName"}]'::jsonb THEN 'Yes'
         |ELSE 'No' END AS result
         |FROM $metaDataTable
         |WHERE entity_id = '1';
         |""".stripMargin

    val isReportAdminPresent = postgresUtil.fetchData(reportAdminCheckQuery)
      .collectFirst {
        case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("")
      }.getOrElse("")

    if (isReportAdminPresent == "Yes") {
      println(s"=====> '$reportAdminCollectionName' already exists.")
    } else {
      println(s"=====> '$reportAdminCollectionName' not found. Creating...")
      val collectionId = createReportAdminCollection()
      if (collectionId != -1) {
        println(s"=====> '$reportAdminCollectionName' created.")
        createUserMetricsDashboard(collectionId, metaDataTable, reportConfig, metabaseDatabase, tenantCode, userMetrics)
      }
      else println("=====> Failed to create Report Admin Collection.")
    }
    println("~~~~~~~~ End Report Admin Collection Processing ~~~~~~~~")

    if (tenantName.nonEmpty) {
      // ----- Tenant Admin Collection -----
      println("~~~~~~~~ Start Tenant Admin Collection Processing ~~~~~~~~")
      val tenantAdminCollectionName = s"User Activity [$tenantCode]"
      val tenantAdminCheckQuery =
        s"""SELECT CASE
           |WHEN main_metadata::jsonb @> '[{"collectionName":"$tenantAdminCollectionName"}]'::jsonb THEN 'Yes'
           |ELSE 'No' END AS result
           |FROM $metaDataTable
           |WHERE entity_id = '$tenantCode';
           |""".stripMargin

      val isTenantAdminPresent = postgresUtil.fetchData(tenantAdminCheckQuery)
        .collectFirst {
          case map: Map[_, _] => map.get("result").map(_.toString).getOrElse("")
        }.getOrElse("")

      if (isTenantAdminPresent == "Yes") {
        println(s"=====> '$tenantAdminCollectionName' already exists.")
      }else {
        println(s"=====> '$tenantAdminCollectionName' not found. Creating...")
        val collectionId = createTenantAdminCollection(tenantCode)
        if (collectionId != -1) {
          println(s"=====> '$tenantAdminCollectionName' created.")
          createTenantDashboard(collectionId, metaDataTable, reportConfig, metabaseDatabase, tenantCode, tenantUserMetadataTable)
        }
        else println("=====> Failed to create Tenant Admin Collection.")
      }
      println("~~~~~~~~ End Tenant Admin Collection Processing ~~~~~~~~")
    }
    else {
      println("Tenant name is null or empty, skipping the processing.")
    }

    def createTenantAdminCollection(tenantCode: String): Int = {
      val collectionName = s"User Activity [$tenantCode]"
      val description = "This report has access to a dedicated dashboard offering insights and metrics specific to their own tenant. \r\n**Collection For:** Tenant Admin"
      val groupName = s"Tenant_Admin [$tenantCode]"
      val collectionId = Utils.checkAndCreateCollection(collectionName, description, metabaseUtil)
      if (collectionId != -1) {
        CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
        val metadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName))
        val query = s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$metadataJson'::jsonb WHERE entity_id = '$tenantCode';"
        postgresUtil.insertData(query)
        collectionId
      } else {
        println("Collection creation failed.")
        -1
      }
    }

    def createReportAdminCollection(): Int = {
      val collectionName = s"User Activity"
      val description = "This report is a centralized dashboard that provides a unified view across all tenants. It enables administrators to monitor user metrics, activity, and performance at a multi-tenant level. \r\n**Collection For:** Report Admin"
      val groupName = s"Report_Admin_User Activity"
      val collectionId = Utils.checkAndCreateCollection(collectionName, description, metabaseUtil)
      if (collectionId != -1) {
        val metadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName))
        val query = s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$metadataJson'::jsonb WHERE entity_id = '1';"
        postgresUtil.insertData(query)
        CreateAndAssignGroup.createGroupToDashboard(metabaseUtil, groupName, collectionId)
        collectionId
      } else {
        println("Collection creation failed.")
        -1
      }
    }

    def createUserMetricsDashboard(collectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, tenantCode: String, userMetrics: String): Unit = {
      try {
        val dashboardName: String = "User Metrics Summary"
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil)
        if (dashboardId == -1) {
          throw new RuntimeException("Dashboard creation failed.")
        }
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId == -1) {
          throw new RuntimeException(s"Database '$metabaseDatabase' not found in Metabase.")
        }
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Admin' AND report_name = 'User-Metrics-Report' AND question_type IN ('big-number', 'table');"
        val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, null.asInstanceOf[Integer], null.asInstanceOf[Integer], null.asInstanceOf[Integer], null.asInstanceOf[Integer], null.asInstanceOf[Integer], null, null, userMetrics, metabaseUtil, postgresUtil)
        if (questionCardIdList.isEmpty) {
          throw new RuntimeException("No cards were generated. Check reportConfig or table mapping.")
        }
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val parametersQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Admin' AND report_name = 'User-Metrics-Report' AND question_type = 'tenant-parameter';"
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
        val userMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString))
        val updateMetadataQuery = s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$userMetadataJson'::jsonb,status = 'Success',error_message = '' WHERE entity_id = '1';"
        postgresUtil.insertData(updateMetadataQuery)
        println(s"=====> Dashboard '$dashboardName' created and metadata updated successfully.")
      } catch {
        case e: Exception =>
          val failQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '$tenantCode';"
          postgresUtil.insertData(failQuery)
          println(s"An error occurred while creating the dashboard: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    def createTenantDashboard(collectionId: Int, metaDataTable: String, reportConfig: String, metabaseDatabase: String, tenantCode: String, tenantUserMetadataTable: String): Unit = {
      try {
        val dashboardName: String = s"User Dashboard [$tenantCode]"
        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '$tenantCode';"
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, metabaseUtil, postgresUtil)
        if (dashboardId == -1) {
          throw new RuntimeException("Dashboard creation failed.")
        }
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        if (databaseId == -1) {
          throw new RuntimeException(s"Database '$metabaseDatabase' not found in Metabase.")
        }
        metabaseUtil.syncDatabaseAndRescanValues(databaseId)
        val statenNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, tenantUserTable, "user_profile_one_name", postgresUtil, createDashboardQuery)
        val districtNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, tenantUserTable, "user_profile_two_name", postgresUtil, createDashboardQuery)
        val blockNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, tenantUserTable, "user_profile_three_name", postgresUtil, createDashboardQuery)
        val clusterNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, tenantUserTable, "user_profile_four_name", postgresUtil, createDashboardQuery)
        val organizationsNameId: Int = GetTableData.getTableMetadataId(databaseId, metabaseUtil, tenantUserMetadataTable, "attribute_code", postgresUtil, createDashboardQuery)
        metabaseUtil.updateColumnCategory(statenNameId, "State")
        metabaseUtil.updateColumnCategory(districtNameId, "City")
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Users' AND report_name = 'User-Dashboard-Report' AND question_type IN ('big-number', 'table', 'graph');"
        val questionCardIdList = UpdateStatusJsonFiles.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, statenNameId, districtNameId, blockNameId, clusterNameId, organizationsNameId, tenantUserTable, tenantUserMetadataTable, userMetrics, metabaseUtil, postgresUtil)

        if (questionCardIdList.isEmpty) {
          throw new RuntimeException("No cards were generated. Check reportConfig or table mapping.")
        }
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val parametersQuery = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Users' AND report_name = 'User-Dashboard-Report' AND question_type = 'metadata-parameters';"
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
        val userMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("questionIds", questionIdsString))
        val updateMetadataQuery = s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$userMetadataJson'::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$tenantCode';"
        postgresUtil.insertData(updateMetadataQuery)
        println(s"=====> Dashboard '$dashboardName' created and metadata updated for tenant [$tenantCode].")
      } catch {
        case e: Exception =>
          val failQuery = s"UPDATE $metaDataTable SET status = 'Failed', error_message = '${e.getMessage}' WHERE entity_id = '$tenantCode';"
          postgresUtil.insertData(failQuery)
          println(s"An error occurred while creating the dashboard for tenant [$tenantCode]: ${e.getMessage}")
          e.printStackTrace()
      }
    }
  }

}