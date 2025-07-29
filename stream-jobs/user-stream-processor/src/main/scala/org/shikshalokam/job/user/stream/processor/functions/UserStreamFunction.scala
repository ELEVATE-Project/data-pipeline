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

    println(s"***************** Start of Processing the User Event with Id = ${event.userId} *****************")
    println(s"userId: ${event.userId}")
    println(s"tenantCode: ${event.tenantCode}")
    println(s"username: ${event.username}")

    println(s"name: ${event.name}")
    println(s"status: ${event.status}")
    println(s"isDeleted: ${event.isDeleted}")
    println(s"createdBy: ${event.createdBy}")
    println(s"createdAt: ${event.createdAt}")
    println(s"updatedAt: ${event.updatedAt}")

    println(s"userProfileOneId (state.id): ${event.userProfileOneId}")
    println(s"userProfileOneName (state.name): ${event.userProfileOneName}")
    println(s"userProfileOneExternalId (state.externalId): ${event.userProfileOneExternalId}")

    println(s"userProfileTwoId (district.id): ${event.userProfileTwoId}")
    println(s"userProfileTwoName (district.name): ${event.userProfileTwoName}")
    println(s"userProfileTwoExternalId (district.externalId): ${event.userProfileTwoExternalId}")

    println(s"userProfileThreeId (block.id): ${event.userProfileThreeId}")
    println(s"userProfileThreeName (block.name): ${event.userProfileThreeName}")
    println(s"userProfileThreeExternalId (block.externalId): ${event.userProfileThreeExternalId}")

    println(s"userProfileFourId (cluster.id): ${event.userProfileFourId}")
    println(s"userProfileFourName (cluster.name): ${event.userProfileFourName}")
    println(s"userProfileFourExternalId (cluster.externalId): ${event.userProfileFourExternalId}")

    println(s"userProfileFiveId (school.id): ${event.userProfileFiveId}")
    println(s"userProfileFiveName (school.name): ${event.userProfileFiveName}")
    println(s"userProfileFiveExternalId (school.externalId): ${event.userProfileFiveExternalId}")

    def checkAndCreateTable(tableName: String, createTableQuery: String): Unit = {
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

    val tenantUserMetadataTable: String = s""""${event.tenantCode}_users_metadata""""
    val tenantUserTable: String = s""""${event.tenantCode}_users""""
    val createTenantTable = config.createTenantUserMetadataTable.replace("@tenantTable", tenantUserMetadataTable)

    checkAndCreateTable(tenantUserMetadataTable, createTenantTable)
    println("Created table if not exists: " + tenantUserMetadataTable)

    var organizationsId: String = null
    var organizationsName: String = null
    val userId: Int = event.userId

    val professionalRoleName = event.professionalRoleName
    val professionalRoleId = event.professionalRoleId
    val (eventType) = (event.eventType)
    if (eventType == "update" || eventType == "bulk-update" || eventType == "create" || eventType == "bulk-create") {
      usersTable(tenantUserTable, userId)
      upsertUserMetadata(tenantUserMetadataTable, userId, "Professional Role", professionalRoleName, professionalRoleId)
      event.organizations.foreach { org =>
        println(s"Organization ID: ${org.get("id")}")
        if (org.get("id").isDefined) {
          organizationsName = org.get("name").map(_.toString).getOrElse(null)
          organizationsId = org.get("id").map(_.toString).getOrElse(null)
          upsertUserMetadata(tenantUserMetadataTable, userId, "Organizations", organizationsName, organizationsId)

          val roles = org.get("roles").map(_.asInstanceOf[List[Map[String, Any]]]).getOrElse(List.empty)
          val rolePairs = extractUserRolesPerRow(roles)
          println(s"Extracted roles: $rolePairs")
          val professionalSubrolesList = event.professionalSubroles
          val subrolePairs = extractProfessionalSubrolesPerRow(professionalSubrolesList)

          rolePairs.foreach { case (userRoleIds, userRoles) =>
            val exists = checkIfRoleExists(tenantUserMetadataTable, userId.toString, organizationsId, userRoleIds)

            if (!exists) {
              upsertUserMetadata(tenantUserMetadataTable, userId, "Platform Role", userRoleIds, userRoles)
              if (subrolePairs.nonEmpty) {
                subrolePairs.foreach { case (professionalSubrolesName, professionalSubrolesId) =>
                  upsertUserMetadata(tenantUserMetadataTable, userId, "Professional Subroles", professionalSubrolesName, professionalSubrolesId)
                }
              } else {
                println(s"No professional subroles found for user $userId in organization $organizationsId")
              }
            } else {
              println(s"Role already exists: user=$userId, org=$organizationsId, role=$userRoleIds")
            }
          }

        } else {
          println(s"Organization with ID ${event.organizationsId} not found in the event data.")
        }
      }
    } else if (eventType == "delete") {
      println(s"Inside the else if loop for delete event")
      deleteData(tenantUserTable, userId)
    }

    userMetrics(tenantUserTable)

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

    def upsertUserMetadata(tenantUserMetadataTable: String, userId: Int, attributeCode: String, attributeValue: String, attributeLabel: String): Unit = {
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

    def usersTable(tenantUserTable: String, userId: Int): Unit = {
      val createUsersTable = config.createTenantUserTable.replace("@usersTable", tenantUserTable)

      checkAndCreateTable(tenantUserTable, createUsersTable)
      println("Created table if not exists: " + tenantUserTable)

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
        // Insert parameters
        userId, event.tenantCode, username, name, status, isDeleted, createdBy, createdAt, updatedAt, userProfileOneId, userProfileOneName, userProfileOneExternalId, userProfileTwoId, userProfileTwoName, userProfileTwoExternalId, userProfileThreeId, userProfileThreeName, userProfileThreeExternalId, userProfileFourId, userProfileFourName, userProfileFourExternalId, userProfileFiveId, userProfileFiveName, userProfileFiveExternalId,

        event.tenantCode, username, name, status, isDeleted, createdBy, createdAt, updatedAt, userProfileOneId, userProfileOneName, userProfileOneExternalId, userProfileTwoId, userProfileTwoName, userProfileTwoExternalId, userProfileThreeId, userProfileThreeName, userProfileThreeExternalId, userProfileFourId, userProfileFourName, userProfileFourExternalId, userProfileFiveId, userProfileFiveName, userProfileFiveExternalId
      )
      println(s"Inserting into table: $tenantUserTable")
      postgresUtil.executePreparedUpdate(upsertUserQuery, userParams, tenantUserTable, userId.toString)
      println("completed a status table " + tenantUserTable)
    }

    def checkIfRoleExists(tenantUserMetadataTable: String, userId: String, organizations_name: String, user_role_ids: String): Boolean = {
      val query =
        s"""
           |SELECT 1 FROM $tenantUserMetadataTable
           |WHERE user_id = '$userId' AND attribute_value = '$organizations_name' AND attribute_label = '$user_role_ids'
           |LIMIT 1
         """.stripMargin

      val result = postgresUtil.fetchData(query)
      result.nonEmpty
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

    def userMetrics(tenantUserTable: String): Unit = {
      val user_metrics = s""""user_metrics""""

      checkAndCreateTable(user_metrics, config.createUserMetricsTable)
      println("Created table if not exists: " + user_metrics)

      val upsertQuery =
        s"""
           |INSERT INTO $user_metrics (tenant_code, total_users, active_users, deleted_users, last_updated)
           |SELECT
           |  '${event.tenantCode}',
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

      postgresUtil.executePreparedUpdate(upsertQuery, Seq.empty, user_metrics, tenantUserTable)
      println(s"User metrics updated for tenant: $tenantUserTable")

    }

    /**
     * Logic to populate kafka messages for creating metabase dashboard
     */
    postgresUtil.createTable(config.createDashboardMetadataTable, config.dashboard_metadata)
    println("Created table if not exists: " + config.dashboard_metadata)

    val dashboardData = new java.util.HashMap[String, String]()
    val dashboardConfig = Seq(
      ("admin", "1", "admin"),
      ("report_admin", event.tenantCode, "report_admin")
    )

    dashboardConfig
      .filter { case (key, _, _) => config.reportsEnabled.contains(key) }
      .foreach { case (key, value, target) =>
        checkAndInsert(key, value, dashboardData, target)
      }

    if (!dashboardData.isEmpty) {
      pushUserDashboardEvents(dashboardData, context, event)
      println(s"\n***************** End of Processing the Project Event *****************")
    }
    else {
      println(s"Skipping the project event with Id = ${event.userId} and status = ${event.status} as it is not in a valid status.")
    }

    def checkAndInsert(entityType: String, targetedId: String, dashboardData: java.util.HashMap[String, String], dashboardKey: String): Unit = {
      val query = s"SELECT EXISTS (SELECT 1 FROM ${config.dashboard_metadata} WHERE entity_id = '$targetedId') AS is_${entityType}_present"
      val result = postgresUtil.fetchData(query)

      result.foreach { row =>
        row.get(s"is_${entityType}_present") match {
          case Some(isPresent: Boolean) if isPresent =>
            println(s"$entityType details already exist.")
          case _ =>
            if (entityType == "report_admin") {
              val insertQuery = s"INSERT INTO ${config.dashboard_metadata} (entity_type, entity_name, entity_id) VALUES ('$entityType', '${event.tenantCode}', '$targetedId')"
              val affectedRows = postgresUtil.insertData(insertQuery)
              println(s"Inserted Tenant Admin details. Affected rows: $affectedRows")
              dashboardData.put(dashboardKey, "1")
            } else {
              val getEntityNameQuery =
                s"""
                   |SELECT DISTINCT ${
                  if (entityType == "report_admin") "name"
                  else s"${entityType}_name"
                } AS ${entityType}_name
                   |FROM ${
                  entityType match {
                    case _ => config.user_metrics
                  }
                }
                   |WHERE ${entityType}_id = '$targetedId'
               """.stripMargin.replaceAll("\n", " ")
              val result = postgresUtil.fetchData(getEntityNameQuery)
              result.foreach { id =>
                val entityName = id.get(s"${entityType}_name").map(_.toString).getOrElse("")
                val upsertMetaDataQuery =
                  s"""INSERT INTO ${config.dashboard_metadata} (
                     |    entity_type, entity_name, entity_id
                     |) VALUES (
                     |    ?, ?, ?
                     |) ON CONFLICT (entity_id) DO UPDATE SET
                     |    entity_type = ?, entity_name = ?;
                     |""".stripMargin

                val dashboardParams = Seq(
                  entityType, entityName, targetedId, // Insert parameters
                  entityType, entityName // Update parameters (matching columns in the ON CONFLICT clause)
                )
                postgresUtil.executePreparedUpdate(upsertMetaDataQuery, dashboardParams, config.dashboard_metadata, targetedId)
                println(s"Inserted [$entityName : $targetedId] details.")
                dashboardData.put(dashboardKey, targetedId)
              }
            }
        }
      }
    }

    def pushUserDashboardEvents(dashboardData: util.HashMap[String, String], context: ProcessFunction[Event, Event]#Context, event: Event): util.HashMap[String, AnyRef] = {
      val objects = new util.HashMap[String, AnyRef]() {
        put("targetedId", java.util.UUID.randomUUID().toString)
        put("reportType", "Report Admin")
        put("tenantCode", event.tenantCode)
        put("publishedAt", DateTimeFormatter
          .ofPattern("yyyy-MM-dd HH:mm:ss")
          .withZone(ZoneId.systemDefault())
          .format(Instant.ofEpochMilli(System.currentTimeMillis())).asInstanceOf[AnyRef])
        put("dashboardData", dashboardData)
      }

      val serializedEvent = ScalaJsonUtil.serialize(objects)
      context.output(config.eventOutputTag, serializedEvent)
      println(s"----> Pushed new Kafka message to ${config.outputTopic} topic")
      println(objects)
      objects
    }


  }
}
