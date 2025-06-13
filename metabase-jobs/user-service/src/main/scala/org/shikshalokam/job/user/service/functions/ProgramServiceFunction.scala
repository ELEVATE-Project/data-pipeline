package org.shikshalokam.job.user.service.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.user.service.domain.Event
import org.shikshalokam.job.user.service.task.UserServiceConfig
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable.{Map, _}

class ProgramServiceFunction(config: UserServiceConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProgramServiceFunction])

  override def metricsList(): List[String] = {
    List(config.userServiceCleanupHit, config.programServiceCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
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

    if (config.inputTopicTwo == config.inputTopicTwo) {

      println(s"***************** Start of Processing the Program Service Event *****************")

      val (entity, eventType) = (event.entity, event.eventType)

      val isUpdateEvent = (eventType == "update" || eventType == "bulk-update") &&
        Option(event.oldValues).exists(_.nonEmpty) &&
        Option(event.newValues).exists(_.nonEmpty)

      val sourceMap: Map[String, Any] = if (isUpdateEvent) event.oldValues else Map.empty

      def getValue[T](key: String, default: T): T = sourceMap.getOrElse(key, default).asInstanceOf[T]

      // Fetching from oldValues or using fallback
      val uniqueUserName = getValue("username", event.username)
      val programName = event.programName

      println(s"Entity = $entity")
      println(s"EntityType = $eventType")
      println(s"Unique User Name = $uniqueUserName")
      println(s"Program Name = $programName")

      if (entity == "program" && eventType == "create") {
        val userId = getUserId(uniqueUserName)
        if (userId != -1) addUserToGroup("program_manager", None, None, Some(programName), userId) else println("User Not Found. Stopped processing")
      } else if (entity == "program" && eventType == "delete") {
        val userId = getUserId(uniqueUserName)
        if (userId != -1) removeUserFromGroup("program_manager", None, None, Some(programName), userId) else println("User Not Found. Stopped processing")
      }

      println(s"***************** End of Processing the Program Service Event *****************")
    }

  }

  private def getUserId(uniqueUserName: String): Int = {
    val users = mapper.readTree(metabaseUtil.listUsers()).path("data").elements().asScala

    users.find { user =>
      val loginAttrs = user.path("login_attributes")
      loginAttrs.has("userName") && loginAttrs.get("userName").asText().equalsIgnoreCase(uniqueUserName)
    } match {
      case Some(user) =>
        val id = user.get("id").asInt()
        val email = user.get("email")
        if (user.get("is_active").asBoolean()) {
          println(s"User already exists and is active: $uniqueUserName with id: $id and email: $email")
          id
        } else {
          println(s"User with userName: $uniqueUserName exists but has been deactivated (id: $id)")
          id
        }
      case None =>
        println(s"No user found with userName: $uniqueUserName")
        -1
    }
  }

  private def addUserToGroup(userRole: String, stateName: Option[String] = None, districtName: Option[String] = None, programName: Option[String] = None, userId: Int): Unit = {

    val existingUserGroups = metabaseUtil.listGroups()
    val groupName = userRole match {
      case "report_admin" => s"Report_Admin"
      case "state_manager" => s"${stateName}_State_Manager"
      case "district_manager" => s"${districtName}_District_Manager[$stateName]"
      case "program_manager" => s"Program_Manager[${programName.getOrElse("")}]"
      case _ => throw new IllegalArgumentException("Invalid manager type")
    }

    val groupId = findGroupId(existingUserGroups, groupName)
    groupId match {
      case Some(id) =>
        println(s"Found group id as $id for group name $groupName")
        validateUserInGroup(userId, id)
      case None => println(s"No group found for $groupName. Ask Super Admin to create the group")
    }

  }

  private def removeUserFromGroup(userRole: String, stateName: Option[String] = None, districtName: Option[String] = None, programName: Option[String] = None, userId: Int): Unit = {

    val existingUserGroups = metabaseUtil.listGroups()
    val groupName = userRole match {
      case "report_admin" => s"Report_Admin"
      case "state_manager" => s"${stateName}_State_Manager"
      case "district_manager" => s"${districtName}_District_Manager[$stateName]"
      case "program_manager" => s"Program_Manager[${programName.getOrElse("")}]"
      case _ => throw new IllegalArgumentException("Invalid manager type")
    }

    val groupIdOpt = findGroupId(existingUserGroups, groupName)
    groupIdOpt match {
      case Some(groupId) =>
        println(s"Found group id as $groupId for group name $groupName")
        validateUserRemoval(userId, groupId)
      case None =>
        println(s"No group found for $groupName. Skipping removal.")
    }
  }

  private def findGroupId(existingGroupDetails: String, groupName: String): Option[Int] = {
    val groupDetailsJson = mapper.readTree(existingGroupDetails)
    groupDetailsJson.elements().asScala
      .find(node => node.get("name").asText() == groupName)
      .map(node => node.get("id").asInt())
  }

  private def validateUserInGroup(userId: Int, groupId: Int) = {
    val isUserInGroup = mapper.readTree(metabaseUtil.getGroupDetails(groupId))
      .get("members")
      .elements()
      .asScala
      .exists(_.get("user_id").asInt() == userId)
    if (!isUserInGroup) addToGroup(userId, groupId) else println("User is already a member of the group")
  }

  private def validateUserRemoval(userId: Int, groupId: Int): Unit = {
    val groupDetails = mapper.readTree(metabaseUtil.getGroupDetails(groupId))
    val isUserInGroup = groupDetails.get("members").elements().asScala.exists(_.get("user_id").asInt() == userId)
    val membershipId = groupDetails.get("members").elements().asScala.find(_.get("user_id").asInt() == userId).get.get("membership_id").asInt()
    if (isUserInGroup) {
      println(s"User with Id $userId is member of the group $groupId")
      println(s"Removing user from the group with membership Id $membershipId")
      metabaseUtil.removeFromGroup(membershipId)
    } else println("User is not a member of the group")
  }

  private def addToGroup(userId: Int, groupId: Int): Unit = {
    val addToGroupRequestBody =
      s"""
         |{
         |    "user_id": $userId,
         |    "group_id": $groupId
         |}
         |""".stripMargin
    metabaseUtil.addUserToGroup(addToGroupRequestBody)
    println("User added to group")
  }

}