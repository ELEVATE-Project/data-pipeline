package org.shikshalokam.job.user.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.user.stream.processor.domain.Event
import org.shikshalokam.job.user.stream.processor.task.UserStreamConfig
import org.shikshalokam.job.util.JSONUtil.{mapper, serialize}
import org.shikshalokam.job.util.PostgresUtil
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.util.parsing.json.JSON
import java.security.SecureRandom
import scala.collection.JavaConverters._
import scala.collection.immutable.{Map, _}
import java.sql.Timestamp

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

    val tenantTable = s""""${event.tenantCode}_users_metadata""""
    val createTenantTable = config.createTenantTable.replace("@tenantTable", tenantTable)

    checkAndCreateTable(tenantTable, createTenantTable)
    println("Created table if not exists: " + tenantTable)

    var organizationsId: String = null
    var organizationsName: String = null
    val userId = event.userId

    val professionalRoleName = event.professionalRoleName
    val professionalRoleId = event.professionalRoleId
    val (entity, eventType) = (event.entity, event.eventType)
    if (eventType == "update" || eventType == "bulk-update" || eventType == "create" || eventType == "bulk-create") {
      usersTable(userId)
      insertProfessionalRoles(userId, professionalRoleName, professionalRoleId)
      event.organizations.foreach { org =>
        println(s"Organization ID: ${org.get("id")}")
        if (org.get("id").isDefined) {
          organizationsName = org.get("name").map(_.toString).getOrElse(null)
          organizationsId = org.get("id").map(_.toString).getOrElse(null)
          insertOrgsData(userId, organizationsName, organizationsId)

          val roles = org.get("roles").map(_.asInstanceOf[List[Map[String, Any]]]).getOrElse(List.empty)
          val rolePairs = extractUserRolesPerRow(roles)
          println(s"Extracted roles: $rolePairs")
          val professionalSubrolesList = event.professionalSubroles
          val subrolePairs = extractProfessionalSubrolesPerRow(professionalSubrolesList)

          rolePairs.foreach { case (userRoleIds, userRoles) =>
            val exists = checkIfRoleExists(userId.toString, organizationsId, userRoleIds)

            if (!exists) {
              insertPlatformRoles(userId, userRoles, userRoleIds)
              if (subrolePairs.nonEmpty) {
                subrolePairs.foreach { case (professionalSubrolesName, professionalSubrolesId) =>
                  insertProfessionalSubRoles(userId, professionalSubrolesName, professionalSubrolesId)
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
      deleteData(userId)
    }

    userMetrics()

    def extractUserRolesPerRow(roles: List[Map[String, Any]]): List[(String, String)] = {
      roles.map { role =>
        val roleName = role.get("title").map(_.toString).getOrElse(null)
        val roleId = role.get("id").map(_.toString).getOrElse(null)
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

    def insertProfessionalRoles(userId: Int, professionalRole_name: String, professionalRoleId: String): Unit = {
      val tenantTable = s""""${event.tenantCode}_users_metadata""""
      val upsertQuery =
        s"""INSERT INTO $tenantTable (id, user_id, attribute_code, attribute_value, attribute_label)
           |VALUES (DEFAULT, ?, 'professional_role', ?, ?)
           |ON CONFLICT (user_id, attribute_value) DO UPDATE SET
           |  attribute_code = EXCLUDED.attribute_code,
           |  attribute_label = EXCLUDED.attribute_label;""".stripMargin

      val params = Seq(userId, professionalRole_name, professionalRoleId)

      postgresUtil.executePreparedUpdate(upsertQuery, params, tenantTable, userId.toString)
      println(s"Upserted professional role for user $userId into $tenantTable")
    }

    def insertProfessionalSubRoles(userId: Int, professionalSubrolesName: String, professionalSubrolesId: String): Unit = {
      val tenantTable = s""""${event.tenantCode}_users_metadata""""
      val upsertQuery =
        s"""INSERT INTO $tenantTable (id, user_id, attribute_code, attribute_value, attribute_label)
           |VALUES (DEFAULT, ?, 'professional_subroles', ?, ?)
           |ON CONFLICT (user_id, attribute_value) DO UPDATE SET
           |  attribute_code = EXCLUDED.attribute_code,
           |  attribute_label = EXCLUDED.attribute_label;""".stripMargin

      val params = Seq(userId, professionalSubrolesName, professionalSubrolesId)

      postgresUtil.executePreparedUpdate(upsertQuery, params, tenantTable, userId.toString)
      println(s"Upserted professional subrole for user $userId into $tenantTable")
    }

    def insertPlatformRoles(userId: Int, userRoles: String, userRoleIds: String): Unit = {
      val tenantTable = s""""${event.tenantCode}_users_metadata""""
      val upsertQuery =
        s"""INSERT INTO $tenantTable (id, user_id, attribute_code, attribute_value, attribute_label)
           |VALUES (DEFAULT, ?, 'platform_role', ?, ?)
           |ON CONFLICT (user_id, attribute_value) DO UPDATE SET
           |  attribute_code = EXCLUDED.attribute_code,
           |  attribute_label = EXCLUDED.attribute_label;""".stripMargin

      val params = Seq(userId, userRoleIds, userRoles)

      postgresUtil.executePreparedUpdate(upsertQuery, params, tenantTable, userId.toString)
      println(s"Upserted platform role for user $userId into $tenantTable")
    }


    def insertOrgsData(userId: Int, organizationsName: String, organizationsId: String): Unit = {
      val tenantTable = s""""${event.tenantCode}_users_metadata""""
      val insertQuery =
        s"""INSERT INTO $tenantTable (id, user_id, attribute_code, attribute_value, attribute_label)
           |VALUES (DEFAULT, ?, 'organizations', ?, ?)
           |ON CONFLICT (user_id, attribute_value) DO UPDATE
           |SET attribute_code = EXCLUDED.attribute_code,
           |    attribute_label = EXCLUDED.attribute_label;""".stripMargin

      val params = Seq(userId, organizationsName, organizationsId)

      postgresUtil.executePreparedUpdate(insertQuery, params, tenantTable, userId.toString)
      println(s"Upserted organization for user $userId into $tenantTable")
    }

    def usersTable(userId: Int): Unit = {
      val usersTable = s""""${event.tenantCode}_users""""
      val createUsersTable = config.createUsersTable.replace("@usersTable", usersTable)

      checkAndCreateTable(usersTable, createUsersTable)
      println("Created table if not exists: " + usersTable)

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
           |INSERT INTO $usersTable (
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
      println(s"Inserting into table: $usersTable")
      postgresUtil.executePreparedUpdate(upsertUserQuery, userParams, usersTable, userId.toString)
      println("completed a status table " + usersTable)
    }

    def checkIfRoleExists(userId: String, organizations_name: String, user_role_ids: String): Boolean = {
      val tenantTable = s""""${event.tenantCode}_users_metadata""""
      val query =
        s"""
           |SELECT 1 FROM $tenantTable
           |WHERE user_id = '$userId' AND attribute_value = '$organizations_name' AND attribute_label = '$user_role_ids'
           |LIMIT 1
         """.stripMargin

      val result = postgresUtil.fetchData(query)
      result.nonEmpty
    }

    def deleteData(userId: Int): Unit = {
      val tenantTable = s""""${event.tenantCode}_users""""
      val deleteQuery =
        s"""
           |UPDATE $tenantTable
           |SET status = 'INACTIVE', is_deleted = true
           |WHERE user_id = ?
           |""".stripMargin
      postgresUtil.executePreparedUpdate(deleteQuery, Seq(userId), tenantTable, userId.toString)
    }

    def userMetrics() : Unit = {
      val tenantCode = event.tenantCode
      val tenantUserTable = s""""${event.tenantCode}_users""""
      val user_metrics = s""""user_metrics""""

      checkAndCreateTable(user_metrics, config.createUserMetricsTable)
      println("Created table if not exists: " + user_metrics)

      val upsertQuery =
        s"""
           |INSERT INTO $user_metrics (tenant_code, total_users, active_users, deleted_users, last_updated)
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

      postgresUtil.executePreparedUpdate(upsertQuery, Seq.empty, user_metrics, tenantCode)
      println(s"User metrics updated for tenant: $tenantCode")

    }

  }
}
