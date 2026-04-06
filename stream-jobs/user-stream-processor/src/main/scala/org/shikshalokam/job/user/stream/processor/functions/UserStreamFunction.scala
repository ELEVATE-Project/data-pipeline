package org.shikshalokam.job.user.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.user.stream.processor.domain.Event
import org.shikshalokam.job.user.stream.processor.task.UserStreamConfig
import org.shikshalokam.job.util.{PostgresUtil, ScalaJsonUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util
import scala.collection.immutable.{Map, _}
import scala.collection.mutable.ListBuffer

class UserStreamFunction(config: UserStreamConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserStreamFunction])

  override def metricsList(): List[String] = {
    List(config.usersCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val pgHost: String = config.pgHost
    val pgPort: String = config.pgPort
    val pgUsername: String = config.pgUsername
    val pgPassword: String = config.pgPassword
    val pgDataBase: String = config.pgDataBase
    val connectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$pgDataBase"
    postgresUtil = new PostgresUtil(connectionUrl, pgUsername, pgPassword)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {

    println(s"***************** Start of Processing the User Event with User Id = ${event.userId} *****************")

    val userId = event.userId
    val tenantCode = event.tenantCode
    val username = event.username
    val name = event.name
    val status = event.status
    val isDeleted = event.isDeleted
    val createdBy = event.createdBy
    val createdAt = event.createdAt
    val updatedAt = event.updatedAt
    val userProfileOneId = event.userProfileOneId
    val userProfileOneName = event.userProfileOneName
    val userProfileOneExternalId = event.userProfileOneExternalId
    val userProfileTwoId = event.userProfileTwoId
    val userProfileTwoName = event.userProfileTwoName
    val userProfileTwoExternalId = event.userProfileTwoExternalId
    val userProfileThreeId = event.userProfileThreeId
    val userProfileThreeName = event.userProfileThreeName
    val userProfileThreeExternalId = event.userProfileThreeExternalId
    val userProfileFourId = event.userProfileFourId
    val userProfileFourName = event.userProfileFourName
    val userProfileFourExternalId = event.userProfileFourExternalId
    val userProfileFiveId = event.userProfileFiveId
    val userProfileFiveName = event.userProfileFiveName
    val userProfileFiveExternalId = event.userProfileFiveExternalId
    val professionalRoleName = event.professionalRoleName
    val professionalRoleId = event.professionalRoleId
    val professionalSubroles = event.professionalSubroles
    val tenantUserMetadataTable: String = s""""${tenantCode}_users_metadata""""
    val orgRolesTable: String = s""""${tenantCode}_org_roles""""
    val tenantUserTable: String = s""""${tenantCode}_users""""
    val userMetrics: String = config.userMetrics
    val eventType = event.eventType
    val userDashboardReportType = "User Dashboard"
    val mentoringDashboardReportType = "Mentoring"

    println(s"userId: $userId")
    println(s"tenantCode: $tenantCode")
    println(s"username: $username")
    println(s"name: $name")
    println(s"status: $status")
    println(s"isDeleted: $isDeleted")
    println(s"createdBy: $createdBy")
    println(s"createdAt: $createdAt")
    println(s"updatedAt: $updatedAt")
    println(s"userProfileOneId : $userProfileOneId")
    println(s"userProfileOneName : $userProfileOneName")
    println(s"userProfileOneExternalId : $userProfileOneExternalId")
    println(s"userProfileTwoId : $userProfileTwoId")
    println(s"userProfileTwoName : $userProfileTwoName")
    println(s"userProfileTwoExternalId : $userProfileTwoExternalId")
    println(s"userProfileThreeId : $userProfileThreeId")
    println(s"userProfileThreeName : $userProfileThreeName")
    println(s"userProfileThreeExternalId : $userProfileThreeExternalId")
    println(s"userProfileFourId : $userProfileFourId")
    println(s"userProfileFourName : $userProfileFourName")
    println(s"userProfileFourExternalId : $userProfileFourExternalId")
    println(s"userProfileFiveId : $userProfileFiveId")
    println(s"userProfileFiveName : $userProfileFiveName")
    println(s"userProfileFiveExternalId : $userProfileFiveExternalId")

    val userDashboardFilters: List[Map[String, String]] = List(
      Map(
        "user_profile_one_name" -> userProfileOneName,
        "user_profile_two_name" -> userProfileTwoName,
        "user_profile_three_name" -> userProfileThreeName,
        "user_profile_four_name" -> userProfileFourName,
        "user_profile_five_name" -> userProfileFiveName
      )
    )

    val userActivityDashboardFilters: List[Map[String, String]] = List(
      Map(
        "tenant_code" -> tenantCode
      )
    )

    checkAndCreateTable(tenantUserMetadataTable, config.createTenantUserMetadataTable.replace("@tenantTable", tenantUserMetadataTable))
    checkAndCreateTable(tenantUserTable, config.createTenantUserTable.replace("@usersTable", tenantUserTable))
    checkAndCreateTable(userMetrics, config.createUserMetricsTable)

    if (tenantCode.nonEmpty) {
      val createTenantTable = config.createTenantUserMetadataTable.replace("@tenantTable", tenantUserMetadataTable)
      checkAndCreateTable(tenantUserMetadataTable, createTenantTable)

      val createOrgRolesTable = config.createOrgRolesTable.replace("@orgRolesTable", orgRolesTable)
      checkAndCreateTable(orgRolesTable, createOrgRolesTable)

      if (eventType == "update" || eventType == "bulk-update" || eventType == "create" || eventType == "bulk-create") {
        /** checking existance of filter data for tenant user table */
        val userDashboardFiltersList = ListBuffer[String]()
        userDashboardFilters.foreach { filterMap =>
          filterMap.foreach { case (key, value) =>
            userDashboardFiltersList += checkIfValueExists(tenantUserTable.replace("\"", ""), key, value)
          }
        }
        processUsers(tenantUserTable, userId)
        val finalUserDashboardFilterList: List[String] = userDashboardFiltersList.toList
        checkExistenceOfDataAndPushMessageToKafka(userDashboardReportType, finalUserDashboardFilterList, context, tenantUserTable, event.tenantCode)
        checkExistenceOfDataAndPushMessageToKafka(mentoringDashboardReportType, finalUserDashboardFilterList, context, tenantUserTable, event.tenantCode, true)
        val resultList = ListBuffer[String]()
        resultList += checkIfValueExists(tenantUserMetadataTable.replace("\"", ""), "attribute_value", professionalRoleName)
        processUserMetadata(tenantUserMetadataTable, userId, "Professional Role", professionalRoleName, professionalRoleId)
        event.organizations.foreach { org =>
          println(s"Organization ID: ${org.get("id")}")
          val organizationsName = org.get("name").map(_.toString).orNull
          val organizationsId = org.get("id").map(_.toString).orNull
          /**
           * Processing for Orgs data
           */
          if (organizationsName.nonEmpty && organizationsId.nonEmpty) {
            println(s"Upserting for attribute_code: Organizations, attribute_value: $organizationsName, attribute_label: $organizationsId")
            resultList += checkIfValueExists(tenantUserMetadataTable.replace("\"", ""), "attribute_value", organizationsName)
            processUserMetadata(tenantUserMetadataTable, userId, "Organizations", organizationsName, organizationsId)

            val orgDashboardFiltersList = ListBuffer[String]()
            orgDashboardFiltersList += checkIfValueExists(orgRolesTable, "org_name", organizationsName)
            val finalOrgDashboardFilterList: List[String] = orgDashboardFiltersList.toList
            checkExistenceOfDataAndPushMessageToKafka(mentoringDashboardReportType, finalOrgDashboardFilterList, context, orgRolesTable, event.tenantCode, true)
          } else {
            println("Org name or Org Id is empty")
          }

          /**
           * Processing for Roles data
           */
          val roles = org.get("roles").map(_.asInstanceOf[List[Map[String, Any]]]).getOrElse(List.empty)
          if (roles.nonEmpty) {
            val rolePairs = extractUserRolesPerRow(roles)
            val subrolePairs = extractProfessionalSubrolesPerRow(professionalSubroles)
            rolePairs.foreach { case (userRoleName, userRoleId) =>
              resultList += checkIfValueExists(tenantUserMetadataTable.replace("\"", ""), "attribute_value", userRoleName)
              println(s"Upserting for attribute_code: Platform Role, attribute_value: $userRoleName, attribute_label: $userRoleId")
              processUserMetadata(tenantUserMetadataTable, userId, "Platform Role", userRoleName, userRoleId)
              processOrgRolesTable(orgRolesTable, userId, organizationsId.toInt, organizationsName, userRoleId.toInt, userRoleName)
              if (subrolePairs.nonEmpty) {
                subrolePairs.foreach { case (professionalSubrolesName, professionalSubrolesId) =>
                  resultList += checkIfValueExists(tenantUserMetadataTable.replace("\"", ""), "attribute_value", professionalSubrolesName)
                  println(s"Upserting for attribute_code: Professional Subroles, attribute_value: $professionalSubrolesName, attribute_label: $professionalSubrolesId")
                  processUserMetadata(tenantUserMetadataTable, userId, "Professional Subroles", professionalSubrolesName, professionalSubrolesId)
                }
              } else {
                println(s"No professional subroles found for user $userId in organization $organizationsId")
              }
            }
          } else {
            println("Roles object is empty")
          }
        }

        val finalResultList: List[String] = resultList.toList
        checkExistenceOfDataAndPushMessageToKafka(userDashboardReportType, finalResultList, context, tenantUserMetadataTable, event.tenantCode)
      } else if (eventType == "delete") {
        deleteData(tenantUserTable, userId)
      }
      val userMetricDashboardFiltersList = ListBuffer[String]()
      userActivityDashboardFilters.foreach { filterMap =>
        filterMap.foreach { case (key, value) =>
          userMetricDashboardFiltersList += checkIfValueExists(userMetrics.replace("\"", ""), key, value)
        }
      }
      userMetric(tenantUserTable)
      val finalUserMetricDashboardFiltersList: List[String] = userMetricDashboardFiltersList.toList
      checkExistenceOfDataAndPushMessageToKafka(userDashboardReportType, finalUserMetricDashboardFiltersList, context, userMetrics, event.tenantCode)
    } else {
      println("Tenant Code is Empty")
    }

    def extractUserRolesPerRow(roles: List[Map[String, Any]]): List[(String, String)] = {
      roles.map { role =>
        val roleName = role.get("title").map(_.toString).orNull
        val roleId = role.get("id").map(_.toString).orNull
        (roleName, roleId)
      }
    }

    def extractProfessionalSubrolesPerRow(list: List[Map[String, Any]]): List[(String, String)] = {
      list.map { sub =>
        val name = sub.get("name").map(_.toString).getOrElse("")
        val id = sub.get("id").map(_.toString).getOrElse("")
        (name, id)
      }
    }

    def processUserMetadata(tenantUserMetadataTable: String, userId: Int, attributeCode: String, attributeValue: String, attributeLabel: String): Unit = {
      // Skip if either value or label is null/empty
      if (attributeValue == null || attributeValue.trim.isEmpty || attributeLabel == null || attributeLabel.trim.isEmpty) {
        println(s"Skipped metadata insert for [$attributeCode] because attributeValue or attributeLabel is empty for user [$userId]")
        return
      }
      val upsertQuery =
        s"""INSERT INTO $tenantUserMetadataTable (id, user_id, attribute_code, attribute_value, attribute_label)
           |VALUES (DEFAULT, ?, ?, ?, ?)
           |ON CONFLICT (user_id, attribute_value) DO UPDATE SET
           |  attribute_code = EXCLUDED.attribute_code,
           |  attribute_label = EXCLUDED.attribute_label;
        """.stripMargin

      val params = Seq(userId, attributeCode, attributeValue, attributeLabel)
      postgresUtil.executePreparedUpdate(upsertQuery, params, tenantUserMetadataTable, userId.toString)
      println(s"Upserted [$attributeCode] for user [$userId] into [$tenantUserMetadataTable]")
    }

    def processOrgRolesTable(orgRolesTable: String, userId: Int, org_id: Int, org_name: String, role_id: Int, role_name: String): Unit = {
      println(s"processing org role table")
      val upsertQuery =
        s"""INSERT INTO $orgRolesTable (id, user_id, org_id, org_name, role_id, role_name)
           |VALUES (DEFAULT, ?, ?, ?, ?, ?)
           |ON CONFLICT (user_id, org_id, role_id) DO UPDATE SET
           |  org_name = EXCLUDED.org_name,
           |  role_name = EXCLUDED.role_name
        """.stripMargin

      val params = Seq(userId, org_id, org_name, role_id, role_name)
      postgresUtil.executePreparedUpdate(upsertQuery, params, orgRolesTable, userId.toString)
      println(s"Upserted org roles for user [$userId] into [$orgRolesTable]")
    }

    def processUsers(tenantUserTable: String, userId: Int): Unit = {
      println(">>> Started processing user data for a tenant")
      val upsertUserQuery =
        s"""
           |INSERT INTO $tenantUserTable (
           |  id, user_id, tenant_code, username, name, status, is_deleted, created_by, created_at, updated_at, user_profile_one_id, user_profile_one_name, user_profile_one_external_id,
           |  user_profile_two_id, user_profile_two_name, user_profile_two_external_id, user_profile_three_id, user_profile_three_name, user_profile_three_external_id,
           |  user_profile_four_id, user_profile_four_name, user_profile_four_external_id, user_profile_five_id, user_profile_five_name, user_profile_five_external_id
           |)
           |VALUES (DEFAULT, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |ON CONFLICT (user_id)
           |DO UPDATE SET
           |  tenant_code = ?, username = ?, name = ?, status = ?, is_deleted = ?, created_by = ?, created_at = ?, updated_at = ?, user_profile_one_id = ?, user_profile_one_name = ?, user_profile_one_external_id = ?,
           |  user_profile_two_id = ?, user_profile_two_name = ?, user_profile_two_external_id = ?, user_profile_three_id = ?, user_profile_three_name = ?, user_profile_three_external_id = ?,
           |  user_profile_four_id = ?, user_profile_four_name = ?, user_profile_four_external_id = ?, user_profile_five_id = ?, user_profile_five_name = ?, user_profile_five_external_id = ?;
      """.stripMargin

      val userParams = Seq(
        userId, tenantCode, username, name, status, isDeleted, createdBy, createdAt, updatedAt, userProfileOneId, userProfileOneName, userProfileOneExternalId, userProfileTwoId, userProfileTwoName, userProfileTwoExternalId,
        userProfileThreeId, userProfileThreeName, userProfileThreeExternalId, userProfileFourId, userProfileFourName, userProfileFourExternalId, userProfileFiveId, userProfileFiveName, userProfileFiveExternalId,
        tenantCode, username, name, status, isDeleted, createdBy, createdAt, updatedAt, userProfileOneId, userProfileOneName, userProfileOneExternalId, userProfileTwoId, userProfileTwoName, userProfileTwoExternalId,
        userProfileThreeId, userProfileThreeName, userProfileThreeExternalId, userProfileFourId, userProfileFourName, userProfileFourExternalId, userProfileFiveId, userProfileFiveName, userProfileFiveExternalId
      )
      postgresUtil.executePreparedUpdate(upsertUserQuery, userParams, tenantUserTable, userId.toString)
      println(">>> Completed processing user data for a tenant")
    }

    def deleteData(tenantUserTable: String, userId: Int): Unit = {
      val deleteQuery =
        s"""
           |UPDATE $tenantUserTable
           |SET status = 'INACTIVE', is_deleted = true
           |WHERE user_id = ?
        """.stripMargin
      postgresUtil.executePreparedUpdate(deleteQuery, Seq(userId), tenantUserTable, userId.toString)
    }

    def userMetric(tenantUserTable: String): Unit = {
      val upsertQuery =
        s"""
           |INSERT INTO $userMetrics (tenant_code, total_users, active_users, deleted_users, last_updated)
           |SELECT
           |  '$tenantCode',
           |  COUNT(user_id) AS total_users,
           |  COUNT(*) FILTER (WHERE status = 'ACTIVE') AS active_users,
           |  COUNT(*) FILTER (WHERE is_deleted = true) AS deleted_users,
           |  CURRENT_TIMESTAMP
           |FROM $tenantUserTable
           |ON CONFLICT (tenant_code) DO UPDATE SET
           |  total_users = EXCLUDED.total_users,
           |  active_users = EXCLUDED.active_users,
           |  deleted_users = EXCLUDED.deleted_users,
           |  last_updated = EXCLUDED.last_updated;
           |""".stripMargin

      postgresUtil.executePreparedUpdate(upsertQuery, Seq.empty, userMetrics, tenantUserTable)
      println(s"User metrics updated for tenant: $tenantUserTable")

    }

    /**
     * Logic to populate kafka messages for creating user metabase dashboard
     */
    postgresUtil.createTable(config.createDashboardMetadataTable, config.dashboardMetadata)

    val dashboardData = new java.util.HashMap[String, String]()
    val dashboardConfig = Seq(
      ("admin", "1", "admin"),
      ("User Dashboard", event.tenantCode, event.tenantCode)
    )

    dashboardConfig
      .foreach { case (key, value, target) =>
        checkAndInsert(key, value, dashboardData, target)
      }

    if (!dashboardData.isEmpty) {
      pushUserDashboardEvents(userDashboardReportType, dashboardData, context)
    }

    println(s"***************** Completed Processing the User Event with User Id = ${event.userId} *****************")

    def checkAndInsert(entityType: String, targetedId: String, dashboardData: java.util.HashMap[String, String], dashboardKey: String): Unit = {
      if (tenantCode.isEmpty) {
        println(s"Tenant code is empty, skipping insertion.")
        return
      }
      val query = s"SELECT EXISTS (SELECT 1 FROM ${config.dashboardMetadata} WHERE entity_id = '$targetedId') AS is_present"
      val result = postgresUtil.fetchData(query)

      result.foreach { row =>
        row.get(s"is_present") match {
          case Some(isPresent: Boolean) if isPresent =>
            println(s"$entityType details already exist.")
          case _ =>
            if (entityType == "User Dashboard") {
              val insertQuery = s"INSERT INTO ${config.dashboardMetadata} (entity_type, entity_name, entity_id) VALUES ('$entityType', '${event.tenantCode}', '$targetedId')"
              val affectedRows = postgresUtil.insertData(insertQuery)
              println(s"Inserted userDashboard details. Affected rows: $affectedRows")
              dashboardData.put("tenantCode", targetedId)
            }
        }
      }
    }
  }

  private def checkAndCreateTable(tableName: String, createTableQuery: String): Unit = {
    val checkTableExistsQuery =
      s"""SELECT EXISTS (
         |  SELECT FROM information_schema.tables
         |  WHERE table_name = '$tableName'
         |);
         |""".stripMargin

    val tableExists = postgresUtil.executeQuery(checkTableExistsQuery) { resultSet =>
      if (resultSet.next()) resultSet.getBoolean(1) else false
    }

    if (!tableExists) {
      postgresUtil.createTable(createTableQuery, tableName)
    }
  }

  private def checkIfValueExists(tableName: String, columnName: String, value: String)(implicit postgresUtil: PostgresUtil): String = {
    if (value == null || value.trim.isEmpty) {
      return ""
    }
    val rowCountQuery = s"SELECT COUNT(*) AS row_count FROM $tableName"
    val rowCount = postgresUtil.executeQuery(rowCountQuery) { rs =>
      if (rs.next()) rs.getLong("row_count") else 0
    }
    if (rowCount == 0) {
      println(s"Table $tableName has no rows → first time inserting data.")
      ""
    } else {
      val safeValue = value.replace("'", "''")
      val query =
        s"""
           |SELECT
           |    CASE
           |        WHEN EXISTS (
           |            SELECT 1
           |            FROM $tableName
           |            WHERE $columnName = '$safeValue'
           |        )
           |        THEN 'Yes'
           |        ELSE 'No'
           |    END AS exists_flag;
           |""".stripMargin

      postgresUtil.executeQuery(query) { resultSet =>
        if (resultSet.next()) resultSet.getString("exists_flag") else ""
      }
    }
  }

  private def checkExistenceOfDataAndPushMessageToKafka(reportType: String, resultList: List[String], context: ProcessFunction[Event, Event]#Context, tableName: String, tenantCode: String, isMentoring: Boolean = false): Unit = {
    if (resultList.exists(s => s == null || s.trim.isEmpty)) {
      return
    }
    if (resultList.contains("No")) {
      val eventData = new java.util.HashMap[String, String]()
      eventData.put("filterTable", tableName.stripPrefix("\"").stripSuffix("\""))
      eventData.put("filterSync", "Yes")
      eventData.put("tenantCode", tenantCode)
      pushUserDashboardEvents(reportType, eventData, context, isMentoring)
      println(s"eventData: $eventData")
    } else {
      println(s"Data already Exists in $tableName → not sending Kafka message for $tableName")
    }
  }

  private def pushUserDashboardEvents(reportType: String, dashboardData: util.HashMap[String, String], context: ProcessFunction[Event, Event]#Context, isMentoring: Boolean = false): util.HashMap[String, AnyRef] = {
    val objects = new util.HashMap[String, AnyRef]() {
      put("_id", java.util.UUID.randomUUID().toString)
      put("reportType", reportType)
      put("publishedAt", DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(ZoneId.systemDefault())
        .format(Instant.ofEpochMilli(System.currentTimeMillis())).asInstanceOf[AnyRef])
      put("dashboardData", dashboardData)
    }

    val serializedEvent = ScalaJsonUtil.serialize(objects)
    val targetTag = if (isMentoring) config.mentoringEventOutputTag else config.eventOutputTag
    context.output(targetTag, serializedEvent)

    val targetTopic = if (isMentoring) config.mentoringOutputTopic else config.outputTopic
    println(s"----> Pushed new Kafka message to $targetTopic topic")
    println(objects)
    objects
  }

}
