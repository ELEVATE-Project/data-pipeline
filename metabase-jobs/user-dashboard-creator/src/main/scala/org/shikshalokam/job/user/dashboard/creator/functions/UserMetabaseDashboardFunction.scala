package org.shikshalokam.job.user.dashboard.creator.functions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.user.dashboard.creator.domain.Event
import org.shikshalokam.job.user.dashboard.creator.task.UserMetabaseDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
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

    //     ----- Report Admin Collection -----
    println("\n-->> Process Report Admin User Metrics Dashboard")

    val (reportAdminPresent, reportAdminCollectionId) = validateCollection("User Activity", "Report Admin")
    if (reportAdminPresent && reportAdminCollectionId != 0) {
      println(s"=====> 'User Activity' collection present with id: $reportAdminCollectionId, Skipping this step.")
    } else {
      println("=====> Creating Report Admin Collection and User Metrics Dashboard")
      val createdCollectionId = createReportAdminCollection()
      if (createdCollectionId != -1) {
        createUserMetricsDashboard(createdCollectionId, metaDataTable, reportConfig, metabaseDatabase, tenantCode, userMetrics)
      } else {
        println("=====> Failed to create Report Admin Collection.")
      }
    }


    if (tenantCode.nonEmpty) {
      println(s"\n-->> Process Tenant Admin User Metrics Dashboard [$tenantCode]")

      val tenantAdminCollectionName = s"User Activity [$tenantCode]"
      val (tenantCollectionPresent, tenantCollectionId) = validateCollection(tenantAdminCollectionName, "Tenant Admin")

      if (tenantCollectionPresent && tenantCollectionId != 0) {
        println(s"=====> '$tenantAdminCollectionName' collection present with id: $tenantCollectionId, Skipping this step.")
      } else {
        println(s"=====> '$tenantAdminCollectionName' not found. Creating...")
        val collectionId = createTenantAdminCollection(tenantCode)
        if (collectionId != -1) {
          println(s"=====> '$tenantAdminCollectionName' created.")
          createTenantDashboard(collectionId, metaDataTable, reportConfig, metabaseDatabase, tenantCode, tenantUserMetadataTable)
        } else {
          println("=====> Failed to create Tenant Admin Collection.")
        }
      }
    } else {
      println("Tenant name is null or empty, skipping the processing.")
    }


    def createTenantAdminCollection(tenantCode: String): Int = {
      val collectionName = s"User Activity [$tenantCode]"
      val description = "This report has access to a dedicated dashboard offering insights and metrics specific to their own tenant. \n\nCollection For: Tenant Admin"
      val groupName = s"Tenant_Admin [$tenantCode]"
      val collectionId = Utils.checkAndCreateCollection(collectionName, description, metabaseUtil)
      if (collectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, groupName, collectionId)
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
      val description = "This report is a centralized dashboard that provides a unified view across all tenants. It enables administrators to monitor user metrics, activity, and performance at a multi-tenant level. \n\nCollection For: Report Admin"
      val groupName = s"Report_Admin_User_Activity"
      val collectionId = Utils.checkAndCreateCollection(collectionName, description, metabaseUtil)
      if (collectionId != -1) {
        val metadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName))
        val query = s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$metadataJson'::jsonb WHERE entity_id = '1';"
        postgresUtil.insertData(query)
        Utils.createGroupForCollection(metabaseUtil, groupName, collectionId)
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
        val statenNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, tenantUserTable, "user_profile_one_name", postgresUtil, createDashboardQuery)
        val districtNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, tenantUserTable, "user_profile_two_name", postgresUtil, createDashboardQuery)
        val blockNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, tenantUserTable, "user_profile_three_name", postgresUtil, createDashboardQuery)
        val clusterNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, tenantUserTable, "user_profile_four_name", postgresUtil, createDashboardQuery)
        val organizationsNameId: Int = Utils.getTableMetadataId(databaseId, metabaseUtil, tenantUserMetadataTable, "attribute_code", postgresUtil, createDashboardQuery)
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
              val isMatch = if (reportId.isEmpty) matchesName && matchesReportFor else matchesName && matchesReportFor
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