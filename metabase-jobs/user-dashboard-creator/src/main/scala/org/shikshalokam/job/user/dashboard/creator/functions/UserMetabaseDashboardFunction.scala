package org.shikshalokam.job.user.dashboard.creator.functions

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
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
import scala.collection.concurrent.TrieMap
import scala.collection.immutable._
import scala.collection.mutable

class UserMetabaseDashboardFunction(config: UserMetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabasePostgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
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

    println(s"***************** Start of Processing the User Metabase Dashboard Event with = ${event.tenantCode} *****************")

    val startTime = System.currentTimeMillis()
    val metaDataTable = config.dashboardMetadata
    val userMetrics: String = config.userMetrics
    val metabaseDatabase: String = config.metabaseDatabase
    val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
    val reportConfig: String = config.reportConfig
    val metabaseApiKey: String = config.metabaseApiKey
    val tenantCode: String = event.tenantCode
    val filterSync: String = event.filterSync
    val filterTable: String = event.filterTable
    val tenantUserMetadataTable = s"${tenantCode}_users_metadata"
    val tenantUserTable = s"${tenantCode}_users"
    val storedTableIds = TrieMap.empty[(Int, String), Int]
    val storedColumnIds = TrieMap.empty[(Int, String), Int]

    println("\n-->> Process Report Admin User Metrics Dashboard")
    if (filterSync.nonEmpty){
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

    def extractTableId(response: String): Int = {
      val json = ujson.read(response)
      val dataArr = json("data").arr
      if (dataArr.nonEmpty && dataArr(0).obj.contains("table_id")) {
        dataArr(0)("table_id").num.toInt
      } else {
        -1
      }
    }

    val (reportAdminPresent, reportAdminCollectionId) = validateCollection("User Activity", "Admin")
    if (reportAdminPresent && reportAdminCollectionId != 0) {
      println(s"=====> 'User Activity' collection present with id: $reportAdminCollectionId, Skipping this step.")
    } else {
      println("=====> Creating Report Admin Collection and User Metrics Dashboard")
      createUserMetricsCollectionAndDashboardForAdmin()
    }

    if (tenantCode.nonEmpty) {
      println(s"\n-->> Process Tenant Admin User Metrics Dashboard [$tenantCode]")

      val tenantAdminCollectionName = s"User Activity $tenantCode"
      val (tenantCollectionPresent, tenantCollectionId) = validateCollection(tenantAdminCollectionName, "Tenant Admin")

      if (tenantCollectionPresent && tenantCollectionId != 0) {
        println(s"=====> '$tenantAdminCollectionName' collection present with id: $tenantCollectionId, Skipping this step.")
      } else {
        println(s"=====> '$tenantAdminCollectionName' not found. Creating...")
        createUserMetricsCollectionAndDashboardForTenant()
      }
    } else {
      println("Tenant name is null or empty, skipping the processing.")
    }

    def createUserMetricsCollectionAndDashboardForAdmin(): Unit = {
      val (collectionName, collectionDescription) = (s"User Activity", "This report is a centralized dashboard that provides a unified view across all tenants. It enables administrators to monitor user metrics, activity, and performance at a multi-tenant level.\n\nCollection For: Admin")
      val collectionId = Utils.checkAndCreateCollection(collectionName, collectionDescription, metabaseUtil)
      if (collectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, s"Report_Admin_User_Activity", collectionId)
        val (dashboardName, dashboardDescription) = ("User Metrics Summary", "Aggregated user data for a Admin")
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, dashboardDescription, metabaseUtil)
        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed' WHERE entity_id = '1';"
        val tenantCodeId: Int = getTheColumnId(databaseId, userMetrics, "tenant_code", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Admin' AND report_name = 'User-Metrics-Report' AND question_type IN ('big-number', 'table');"
        val questionCardIdList = ProcessAdminConstructor.processAdminJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, tenantCodeId, userMetrics, metabaseUtil, postgresUtil)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val parametersQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Admin' AND report_name = 'User-Metrics-Report' AND question_type = 'tenant-parameter';"
        UpdateParameters.updateDashboardParameters(metabaseUtil, postgresUtil, parametersQuery, dashboardId)
        val userMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("collectionFor", "Admin").put("questionIds", questionIdsString))
        val updateMetadataQuery = s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$userMetadataJson'::jsonb, status = 'Success' WHERE entity_id = '1';"
        postgresUtil.insertData(updateMetadataQuery)
        println(s"=====> Dashboard '$dashboardName' created and metadata updated successfully.")
      }
    }

    def createUserMetricsCollectionAndDashboardForTenant(): Unit = {
      val (collectionName, collectionDescription) = (s"User Activity $tenantCode", "This report has access to a dedicated dashboard offering insights and metrics specific to their own tenant. \n\nCollection For: Tenant Admin")
      val collectionId = Utils.checkAndCreateCollection(collectionName, collectionDescription, metabaseUtil)
      if (collectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, s"Tenant_Admin_$tenantCode", collectionId)
        val (dashboardName, dashboardDescription) = ("User Dashboard", "Overview of Users Across Tenant")
        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed' WHERE entity_id = '$tenantCode';"
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, dashboardDescription, metabaseUtil)
        val stateNameId: Int = getTheColumnId(databaseId, tenantUserTable, "user_profile_one_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val districtNameId: Int = getTheColumnId(databaseId, tenantUserTable, "user_profile_two_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val blockNameId: Int = getTheColumnId(databaseId, tenantUserTable, "user_profile_three_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val clusterNameId: Int = getTheColumnId(databaseId, tenantUserTable, "user_profile_four_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        metabaseUtil.updateColumnCategory(stateNameId, "State")
        metabaseUtil.updateColumnCategory(districtNameId, "City")
        val filterQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'Users-Filter' AND question_type = 'tenant-dashboard-filter'"
        val filterResults: List[Map[String, Any]] = postgresUtil.fetchData(filterQuery)
        val objectMapper = new ObjectMapper()
        val slugNameToFilterMap = mutable.Map[String, Int]()
        for (result <- filterResults) {
          val configString = result.get("config").map(_.toString).getOrElse("")
          val configJson = objectMapper.readTree(configString)
          val slugName = configJson.findValue("name").asText()
          val metadataFilter: Int = AddMetadataFilter.updateAndAddFilter(metabaseUtil, configJson: JsonNode, collectionId, databaseId, tenantUserMetadataTable)
          slugNameToFilterMap(slugName) = metadataFilter
        }
        val immutableSlugNameToFilterMap: Map[String, Int] = slugNameToFilterMap.toMap
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Users' AND report_name = 'User-Dashboard-Report' AND question_type IN ('big-number', 'table', 'graph');"
        val questionCardIdList = ProcessTenantConstructor.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, stateNameId, districtNameId, blockNameId, clusterNameId, tenantUserTable, tenantUserMetadataTable, immutableSlugNameToFilterMap, metabaseUtil, postgresUtil)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val parametersQuery: String = s"SELECT config FROM $reportConfig WHERE report_name = 'User-Dashboard-Report' AND question_type = 'metadata-parameters'"
        UpdateParameters.updateDashboardParameters(metabaseUtil, postgresUtil, parametersQuery, dashboardId, immutableSlugNameToFilterMap)
        val mainMetadataJson = new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("collectionFor", "Tenant Admin").put("questionIds", questionIdsString)
        postgresUtil.insertData(s"UPDATE $metaDataTable SET  main_metadata = '$mainMetadataJson' WHERE entity_id = '$tenantCode';")
        val userMetadataJson = new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("Collection Name", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("collectionFor", "Tenant Admin").put("questionIds", questionIdsString))
        val updateMetadataQuery = s"UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$userMetadataJson'::jsonb, status = 'Success', error_message = '' WHERE entity_id = '$tenantCode';"
        postgresUtil.insertData(updateMetadataQuery)
        println(s"=====> Dashboard '$dashboardName' created and metadata updated for tenant [$tenantCode].")
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
    val endTime = System.currentTimeMillis()
    val totalTimeSeconds = (endTime - startTime) / 1000
    println(s"Total time taken: $totalTimeSeconds seconds")
    println(s"***************** End of Processing the User Metabase Dashboard Event with = ${event.tenantCode} *****************")
  }
  private def validateCollection(collectionName: String, reportFor: String, reportId: Option[String] = None): (Boolean, Int) = {
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