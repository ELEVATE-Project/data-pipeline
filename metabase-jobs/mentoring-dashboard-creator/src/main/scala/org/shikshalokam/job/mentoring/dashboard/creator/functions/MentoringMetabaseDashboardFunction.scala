package org.shikshalokam.job.mentoring.dashboard.creator.functions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.mentoring.dashboard.creator.domain.Event
import org.shikshalokam.job.mentoring.dashboard.creator.task.MentoringMetabaseDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.immutable._


class MentoringMetabaseDashboardFunction(config: MentoringMetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabasePostgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[MentoringMetabaseDashboardFunction])

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

    println(s"***************** Start of Processing the Mentoring Metabase Dashboard Event with = ${event.tenantCode} and Org ${event.orgId} *****************")

    val startTime = System.currentTimeMillis()
    val metaDataTable = config.dashboardMetadata
    val metabaseDatabase: String = config.metabaseDatabase
    val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
    val reportConfig: String = config.reportConfig
    val metabaseApiKey: String = config.metabaseApiKey
    val tenantCode: String = event.tenantCode
    val orgId: String = event.orgId
    val orgName: String = event.orgName
    val filterSync: String = event.filterSync
    val filterTable: String = event.filterTable
    val tenantUserTable = s"${tenantCode}_users"
    val tenantSessionTable: String = s"${tenantCode}_sessions"
    val tenantSessionAttendanceTable: String = s"${tenantCode}_session_attendance"
    val tenantConnectionsTable: String = s"${tenantCode}_connections"
    val tenantOrgMentorRatingTable: String = s"${tenantCode}_org_mentor_rating"
    val tenantOrgRolesTable: String = s"${tenantCode}_org_roles"
    val storedTableIds = TrieMap.empty[(Int, String), Int]
    val storedColumnIds = TrieMap.empty[(Int, String), Int]
    val tabList = List("Overview", "Compare Organizations")
    if (databaseId < 0) {
      logger.error(s"Metabase database '$metabaseDatabase' not found; skipping event.")
      return
    }

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

    def extractTableId(response: String): Int = {
      val json = ujson.read(response)
      val dataArr = json("data").arr
      if (dataArr.nonEmpty && dataArr(0).obj.contains("table_id")) {
        dataArr(0)("table_id").num.toInt
      } else {
        -1
      }
    }

    if (tenantCode.nonEmpty) {
      createCollectionAndDashboardForTenant(tenantCode)
      if (orgId.nonEmpty) {
        createCollectionAndDashboardForOrg(orgId.toInt, tenantCode, orgName)
      } else {
        println(s"[SKIP] Skipping Org Admin dashboard creation due to missing orgId.")
      }
    } else {
      println(s"[SKIP] Skipping event due to missing tenantCode.")
    }

    def createCollectionAndDashboardForTenant(tenantCode: String): Unit = {
      val (collectionName, collectionDescription) = (s"Mentoring Report [tenant: $tenantCode]", s"Dashboards for Tenant Admin with Overview and Compare tabs.\n\nCollection For: Tenant  Admin")
      val collectionId = Utils.checkAndCreateCollection(collectionName, collectionDescription, metabaseUtil)
      if (collectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, s"Tenant_Admin_Mentoring_$tenantCode", collectionId)
        val (dashboardName, dashboardDescription) = ("Dashboard", s"Overview + Comparasion metrics for [$tenantCode]")
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, dashboardDescription, metabaseUtil)
        val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
        createOverviewTabInsideTenantDashboard(collectionId, databaseId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, metabaseApiKey)
        createComparisionTabInsideTenantDashboard(collectionId, databaseId, dashboardId, tabIdMap, metaDataTable, reportConfig, metabaseDatabase, metabaseApiKey)
      }
    }

    def createOverviewTabInsideTenantDashboard(parentCollectionId: Int, databaseId: Int, dashboardId: Int, tabIdMap: Map[String, Int], metaDataTable: String, reportConfig: String, metabaseDatabase: String, metabaseApiKey: String): Unit = {
      try {
        val dashboardName: String = s"Overview"
        val safeTenantCode = tenantCode.replace("'", "''")
        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '${safeTenantCode}_tenant_admin';"
        val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
        val orgNameId: Int = getTheColumnId(databaseId, tenantOrgRolesTable, "org_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Mentoring-Reports' AND report_name = 'Tenant-Overview' AND question_type IN ('big-number', 'graph');"
        val questionCardIdList = ProcessTenantConstructor.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, 0, orgNameId, 0, tenantUserTable,
          tenantSessionTable, tenantSessionAttendanceTable, tenantConnectionsTable, tenantOrgMentorRatingTable,
          tenantOrgRolesTable, Map.empty, orgId.toInt, tabId, metabaseUtil, postgresUtil)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val parametersQuery = s"SELECT config FROM $reportConfig WHERE report_name='Tenant-Overview' AND question_type='overview-parameter'"
        UpdateParameters.updateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
        val objectMapper = new ObjectMapper()
        val userMetadataJson = objectMapper.createArrayNode().add(objectMapper.createObjectNode().put("collectionId", parentCollectionId).put("dashboardName", dashboardName).put("dashboardId", dashboardId).put("collectionFor", "Tenant Admin").put("questionIds", questionIdsString))
        val updateMetadataQuery = s" UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$userMetadataJson'::jsonb, status = 'Success' WHERE entity_id = '${safeTenantCode}_tenant_admin';"
        postgresUtil.insertData(updateMetadataQuery)
      }
      catch {
        case e: Exception =>
          postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '${tenantCode}_tenant_admin';")
          println(s"An error occurred: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    def createComparisionTabInsideTenantDashboard(parentCollectionId: Int, databaseId: Int, dashboardId: Int, tabIdMap: Map[String, Int], metaDataTable: String, reportConfig: String, metabaseDatabase: String, metabaseApiKey: String): Unit = {
      try {
        val dashboardName: String = s"Compare Organizations"
        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed',error_message = 'errorMessage'  WHERE entity_id = '${tenantCode}_tenant_admin';"
        val tabId: Int = tabIdMap.getOrElse(dashboardName, -1)
        val orgIdSession: Int = getTheColumnId(databaseId, tenantSessionTable, "org_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val orgIdMentor: Int = getTheColumnId(databaseId, tenantOrgRolesTable, "org_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val orgIdRating: Int = getTheColumnId(databaseId, tenantOrgMentorRatingTable, "org_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val reportConfigQuery: String = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Mentoring-Reports' AND report_name = 'Tenant-Compare' AND question_type IN ('big-number', 'graph');"
        val questionCardIdList = ProcessTenantConstructor.ProcessAndUpdateJsonFiles(reportConfigQuery, parentCollectionId, databaseId, dashboardId, orgIdSession, orgIdMentor, orgIdRating, tenantUserTable,
          tenantSessionTable, tenantSessionAttendanceTable, tenantConnectionsTable, tenantOrgMentorRatingTable,
          tenantOrgRolesTable, Map.empty, orgId.toInt, tabId, metabaseUtil, postgresUtil)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val parametersQuery = s"SELECT config FROM $reportConfig WHERE report_name='Tenant-Compare' AND question_type='compare-parameter'"
        UpdateParameters.updateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
        val objectMapper = new ObjectMapper()
        val userMetadataJson = objectMapper.createArrayNode().add(objectMapper.createObjectNode().put("collectionId", parentCollectionId).put("dashboardName", dashboardName).put("dashboardId", dashboardId).put("collectionFor", "Tenant Admin").put("questionIds", questionIdsString))
        val updateMetadataQuery = s" UPDATE $metaDataTable SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$userMetadataJson'::jsonb,status = 'Success' WHERE entity_id = '${tenantCode}_tenant_admin';"
        postgresUtil.insertData(updateMetadataQuery)
      }
      catch {
        case e: Exception =>
          postgresUtil.insertData(s"UPDATE $metaDataTable SET status = 'Failed',error_message = '${e.getMessage}' WHERE entity_id = '${tenantCode}_tenant_admin';")
          println(s"An error occurred: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    def createCollectionAndDashboardForOrg(orgId: Int, tenantCode: String, orgName: String): Unit = {
      val collectionName = s"Mentoring Report [org : $orgName ($orgId)]"
      val collectionDescription = s"This report has access to a dedicated dashboard offering insights and metrics specific to their own org. \n\nCollection For: Org Admin \n\nTenant: $tenantCode"
      val collectionId = Utils.checkAndCreateCollection(collectionName, collectionDescription, metabaseUtil)
      if (collectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, s"Org_Admin_Mentoring_$orgId", collectionId)
        val dashboardName = s"Dashboard"
        val dashboardDescription = s"Overview of Mentoring Across [Org: $orgName]"
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, dashboardDescription, metabaseUtil)
        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed' WHERE entity_id = 'org_admin_$orgId';"
        val stateNameId = getTheColumnId(databaseId, tenantUserTable, "user_profile_one_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val districtNameId = getTheColumnId(databaseId, tenantUserTable, "user_profile_two_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val blockNameId = getTheColumnId(databaseId, tenantUserTable, "user_profile_three_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val clusterNameId = getTheColumnId(databaseId, tenantUserTable, "user_profile_four_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val schoolNameId = getTheColumnId(databaseId, tenantUserTable, "user_profile_five_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        metabaseUtil.updateColumnCategory(stateNameId, "State")
        metabaseUtil.updateColumnCategory(districtNameId, "City")
        val reportConfigQuery = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Mentoring-Reports' AND report_name = 'Org-Admin' AND question_type IN ('big-number', 'graph');"
        val questionCardIdList = ProcessOrgConstructor.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, stateNameId, districtNameId, blockNameId, clusterNameId, schoolNameId,
          tenantUserTable, tenantSessionTable, tenantSessionAttendanceTable, tenantConnectionsTable, tenantOrgMentorRatingTable, tenantOrgRolesTable, orgId, Map.empty, metabaseUtil, postgresUtil).toList
        val parametersQuery = s"SELECT config FROM $reportConfig WHERE report_name = 'Org-Admin' AND question_type = 'org-parameters'"
        UpdateParameters.updateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
        val mainMetadataJson = new ObjectMapper().createObjectNode().put("collectionId", collectionId).put("collectionName", collectionName).put("dashboardId", dashboardId).put("dashboardName", dashboardName).put("collectionFor", "Org Admin").put("questionIds", questionIdsString)
        postgresUtil.insertData(s"UPDATE $metaDataTable SET main_metadata = '$mainMetadataJson' WHERE entity_id = 'org_admin_$orgId';")
        println(s"=====> Dashboard '$dashboardName' created and metadata updated for tenant [$tenantCode].")
      }
    }

    def getTheTableId(databaseId: Int, tableName: String, metabaseUtil: MetabaseUtil, metabasePostgresUtil: PostgresUtil, metabaseApiKey: String): Int = {
      storedTableIds.get((databaseId, tableName)) match {
        case Some(tableId) =>
          println(s"[CACHE-HIT] Table ID for '$tableName' in DB $databaseId = $tableId")
          tableId
        case None =>
          val tableQuery =
            s"SELECT id FROM metabase_table WHERE name = '$tableName' AND db_id = $databaseId;"
          val tableIdOpt = metabasePostgresUtil.fetchData(tableQuery) match {
            case List(map: Map[_, _]) =>
              map.get("id").flatMap(id => scala.util.Try(id.toString.toInt).toOption)
            case _ => None
          }
          val tableId = tableIdOpt.getOrElse {
            println(s"[WARN] Table '$tableName' not found in DB $databaseId. Trying to sync with Metabase API.")
            val tableJson = metabaseUtil.syncNewTable(databaseId, tableName, metabaseApiKey)
            val newTableId = tableJson("id").num.toInt
            println(s"[SYNCED] Table '$tableName' synced with Metabase. New table_id = $newTableId")
            newTableId
          }
          storedTableIds.put((databaseId, tableName), tableId)
          println(s"[INFO] Using table_id = $tableId for table '$tableName' in DB $databaseId")
          tableId
      }
    }

    def getTheColumnId(databaseId: Int, tableName: String, columnName: String, metabaseUtil: MetabaseUtil, metabasePostgresUtil: PostgresUtil, metabaseApiKey: String, metaTableQuery: String): Int = {
      try {
        val tableId = getTheTableId(databaseId, tableName, metabaseUtil, metabasePostgresUtil, metabaseApiKey)
        storedColumnIds.get((tableId, columnName)) match {
          case Some(columnId) =>
            println(s"[CACHE-HIT] Column '$columnName' found in table '$tableName' (tableId: $tableId) = $columnId")
            columnId
          case None =>
            val columnQuery = s"SELECT id FROM metabase_field WHERE table_id = $tableId AND name = '$columnName';"
            val columnIdOpt = metabasePostgresUtil.fetchData(columnQuery) match {
              case List(map: Map[_, _]) =>
                map.get("id").flatMap(id => scala.util.Try(id.toString.toInt).toOption)
              case _ => None
            }
            val columnId = columnIdOpt.getOrElse(-1)
            if (columnId != -1) {
              storedColumnIds.put((tableId, columnName), columnId)
              println(s"[INFO] Using column_id = $columnId for column '$columnName' in table '$tableName' (tableId: $tableId)")
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
          println(s"[ERROR] Failed to get column ID for '$columnName' in table '$tableName': ${e.getMessage}")
          -1
      }
    }

    val endTime = System.currentTimeMillis()
    val totalTimeSeconds = (endTime - startTime) / 1000
    println(s"Total time taken: $totalTimeSeconds seconds")
    println(s"***************** End of Processing the Mentoring Metabase Dashboard Event with = ${event.tenantCode} *****************")
  }

}