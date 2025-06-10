package org.shikshalokam.job.user.service.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.user.service.domain.Event
import org.shikshalokam.job.user.service.task.UserServiceConfig
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{JSONUtil, MetabaseUtil, PostgresUtil, ScalaJsonUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.security.SecureRandom
import scala.collection.JavaConverters._
import scala.collection.immutable.{Map, _}

class UserServiceFunction(config: UserServiceConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserServiceFunction])

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

    println(s"***************** Start of Processing the User Service Event *****************")

    val (entity, eventType) = (event.entity, event.eventType)

    val isUpdateEvent = (eventType == "update" || eventType == "bulk-update") &&
      Option(event.oldValues).exists(_.nonEmpty) &&
      Option(event.newValues).exists(_.nonEmpty)

    val sourceMap: Map[String, Any] = if (isUpdateEvent) event.oldValues else Map.empty

    def getValue[T](key: String, default: T): T = sourceMap.getOrElse(key, default).asInstanceOf[T]

    def getLabelFromSourceOrDefault(sourceMap: Map[String, Any], key: String, defaultLabel: String): String = {
      sourceMap.get(key) match {
        case Some(map: Map[String, Any]@unchecked) =>
          map.get("label").map(_.toString).getOrElse(defaultLabel)
        case _ => defaultLabel
      }
    }

    // Fetching from oldValues or using fallback
    val name = getValue("name", event.name)
    val uniqueUserName = getValue("username", event.username)
    val email = Option(getValue("email", event.email)).filter(_.trim.nonEmpty).getOrElse(uniqueUserName + config.domainName)
    val password = generatePassword(10)
    val phone = getValue("phone", event.phone)
    val stateName = getLabelFromSourceOrDefault(sourceMap, "state", event.state)
    val districtName = getLabelFromSourceOrDefault(sourceMap, "district", event.district)
    val status = getValue("status", event.status)
    val isUserDeleted = getValue("deleted", event.isUserDeleted)
    val orgDetails = getValue("organizations", event.organizations)
    var userRoles: List[Map[String, Any]] = orgDetails.flatMap(_.get("roles").collect { case roles: List[Map[String, Any]]@unchecked => roles }.getOrElse(Nil))

    if (isUpdateEvent) {
      userRoles ++= event.newValues
        .get("organizations")
        .collect { case orgs: List[Map[String, Any]]@unchecked => orgs }
        .getOrElse(Nil)
        .flatMap(_.get("roles").collect {
          case roles: List[Map[String, Any]]@unchecked => roles
        }.getOrElse(Nil))
    }

    println(s"Entity = $entity")
    println(s"EntityType = $eventType")
    println(s"User Name = $name")
    println(s"Unique User Name = $uniqueUserName")
    println(s"Email = $email")
    println(s"Password = $password")
    println(s"Phone = $phone")
    println(s"State Name = $stateName")
    println(s"District Name = $districtName")
    println(s"Status = $status")
    println(s"Is User Deleted = $isUserDeleted")
    println(s"User Organizations = $orgDetails")
    println(s"User Role = $userRoles")
    println("\n")

    if (entity == "user" && eventType == "delete") {
      val userId = checkUserId(email)
      if (userId != -1) metabaseUtil.deleteUser(userId)
    }

    userRoles.foreach { roleMap =>
      roleMap.get("title") match {
        case Some("report_admin") =>
          handleReportAdmin(entity, eventType, name, email, password)
        case Some("state_manager") =>
          handleStateAdmin(entity, eventType, name, email, password, stateName)
        case Some("district_manager") =>
          handleDistrictUser(entity, eventType, name, email, password, stateName, districtName)
        case Some("program_manager") =>
          handleProgramUser(entity, eventType, name, email, password)
        case Some(unknownRole) =>
          println(s"Unknown Metabase Platform Role: $unknownRole")
        case None =>
          println("Role not found in map")
      }
    }

    def handleReportAdmin(entity: String, eventType: String, name: String, email: String, password: String): Unit = {
      println("<<<======== Processing for the role report_admin ========>>>")
      if (entity == "user" && (eventType == "create" || eventType == "bulk-create")) {
        val userId = checkUserId(email)
        if (userId == -1) {
          val newUserId = createUser(name, email, password)
          addUserToGroup("report_admin", None, None, None, newUserId)
          pushNotification(name, email, password, phone, context)
        } else {
          println("Stopped processing")
        }
      }
      else if (entity == "user" && (eventType == "update" || eventType == "bulk-update")) {
        val oldRoles = extractRoles(event.oldValues)
        val newRoles = extractRoles(event.newValues)
        val hadReportAdmin = oldRoles.contains("report_admin")
        val hasReportAdmin = newRoles.contains("report_admin")
        (hadReportAdmin, hasReportAdmin) match {
          case (false, true) =>
            println("Trying to add user to report_admin role")
            val userId = checkUserId(email)
            if (userId == -1) {
              val newUserId = createUser(name, email, password)
              addUserToGroup("report_admin", None, None, None, newUserId)
              pushNotification(name, email, password, phone, context)
            } else {
              addUserToGroup("report_admin", None, None, None, userId)
            }
          case (true, false) =>
            println("Trying to remove user from report_admin role")
            val userId = checkUserId(email)
            if (userId != -1) removeUserFromGroup("report_admin", None, None, None, userId)
          case (true, true) =>
            //This is a edge case scenario
            println("User already had and still has report_admin role")
            val userId = checkUserId(email)
            val groupId = checkGroupId("Report_Admin")
            validateUserInGroup(userId, groupId)
          case _ => // No action needed
        }
      }
    }

    def handleStateAdmin(entity: String, eventType: String, name: String, email: String, password: String, stateName: String): Unit = {
      println("<<<======== Processing for the role state_manager ========>>>")
      if (entity == "user" && (eventType == "create" || eventType == "bulk-create")) {
        val userId = checkUserId(email)
        if (userId == -1) {
          val newUserId = createUser(name, email, password)
          addUserToGroup("state_manager", Some(stateName), None, None, newUserId)
          pushNotification(name, email, password, phone, context)
        } else {
          println("Stopped processing")
        }
      }
      else if (entity == "user" && (eventType == "update" || eventType == "bulk-update")) {
        val oldRoles = extractRoles(event.oldValues)
        val newRoles = extractRoles(event.newValues)
        val hadReportAdmin = oldRoles.contains("state_manager")
        val hasReportAdmin = newRoles.contains("state_manager")
        (hadReportAdmin, hasReportAdmin) match {
          case (false, true) =>
            println("Trying to add user to state_manager role")
            val userId = checkUserId(email)
            if (userId == -1) {
              val newUserId = createUser(name, email, password)
              addUserToGroup("state_manager", Some(stateName), None, None, newUserId)
              pushNotification(name, email, password, phone, context)
            } else {
              addUserToGroup("state_manager", Some(stateName), None, None, userId)
            }
          case (true, false) =>
            println("Trying to remove user from state_manager role")
            val userId = checkUserId(email)
            if (userId != -1) removeUserFromGroup("state_manager", Some(stateName), None, None, userId)
          case (true, true) =>
            //This is a edge case scenario
            println("User already had and still has state_manager role")
            val userId = checkUserId(email)
            val groupId = checkGroupId(s"${stateName}_State_Manager")
            validateUserInGroup(userId, groupId)
          case _ => // No action needed
        }
      }
    }

    def handleDistrictUser(entity: String, eventType: String, name: String, email: String, password: String, stateName: String, districtName: String): Unit = {
      println("<<<======== Processing for the role district_manager ========>>>")
      if (entity == "user" && (eventType == "create" || eventType == "bulk-create")) {
        val userId = checkUserId(email)
        if (userId == -1) {
          val newUserId = createUser(name, email, password)
          addUserToGroup("district_manager", Some(stateName), Some(districtName), None, newUserId)
          pushNotification(name, email, password, phone, context)
        } else {
          println("Stopped processing")
        }
      }
      else if (entity == "user" && (eventType == "update" || eventType == "bulk-update")) {
        val oldRoles = extractRoles(event.oldValues)
        val newRoles = extractRoles(event.newValues)
        val hadReportAdmin = oldRoles.contains("district_manager")
        val hasReportAdmin = newRoles.contains("district_manager")
        (hadReportAdmin, hasReportAdmin) match {
          case (false, true) =>
            println("Trying to add user to report_admin role")
            val userId = checkUserId(email)
            if (userId == -1) {
              val newUserId = createUser(name, email, password)
              addUserToGroup("district_manager", Some(stateName), Some(districtName), None, newUserId)
              pushNotification(name, email, password, phone, context)
            } else {
              addUserToGroup("district_manager", Some(stateName), Some(districtName), None, userId)
            }
          case (true, false) =>
            println("Trying to remove user from district_manager role")
            val userId = checkUserId(email)
            if (userId != -1) removeUserFromGroup("district_manager", Some(stateName), Some(districtName), None, userId)
          case (true, true) =>
            //This is a edge case scenario
            println("User already had and still has district_manager role")
            val userId = checkUserId(email)
            val groupId = checkGroupId(s"${districtName}_District_Manager[$stateName]")
            validateUserInGroup(userId, groupId)
          case _ => // No action needed
        }
      }
    }

    def handleProgramUser(entity: String, eventType: String, name: String, email: String, password: String): Unit = {
      println("<<<======== Processing for the role program_manager ========>>>")
      if (entity == "user" && (eventType == "create" || eventType == "bulk-create")) {
        val userId = checkUserId(email)
        if (userId == -1) {
          val newUserId = createUser(name, email, password)
          pushNotification(name, email, password, phone, context)
        } else {
          println("Stopped processing")
        }
      }
      else if (entity == "user" && (eventType == "update" || eventType == "bulk-update")) {
        val oldRoles = extractRoles(event.oldValues)
        val newRoles = extractRoles(event.newValues)
        val hadReportAdmin = oldRoles.contains("program_manager")
        val hasReportAdmin = newRoles.contains("program_manager")
        (hadReportAdmin, hasReportAdmin) match {
          case (false, true) =>
            println("Trying to add user to program_manager role")
            val userId = checkUserId(email)
            if (userId == -1) {
              val newUserId = createUser(name, email, password)
              pushNotification(name, email, password, phone, context)
            } else {
              println("Stopped processing")
            }
          //          case (true, false) =>
          //            println("Trying to remove user from district_manager role")
          //            val userId = checkUserId(email)
          //            if (userId != -1) removeUserFromGroup("district_manager", Some(stateName), Some(districtName), None, userId)
          //          case (true, true) =>
          //            //This is a edge case scenario
          //            println("User already had and still has district_manager role")
          //            val userId = checkUserId(email)
          //            val groupId = checkGroupId(s"${districtName}_District_Manager[$stateName]")
          //            validateUserInGroup(userId, groupId)
          case _ => // No action needed
        }
      }
    }

    println(s"***************** End of Processing the User Service Event *****************")
  }

  private def generatePassword(length: Int = 12): String = {
    val charSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()-_=+"
    val random = new SecureRandom()
    (1 to length).map(_ => charSet(random.nextInt(charSet.length))).mkString
  }

  private def extractRoles(data: Map[String, Any]): Set[String] = {
    data.get("organizations") match {
      case Some(orgs: List[Map[String, Any]]@unchecked) =>
        orgs.flatMap(_.get("roles").collect {
          case roles: List[Map[String, Any]]@unchecked =>
            roles.flatMap(_.get("title").map(_.toString))
        }.getOrElse(Nil)).toSet
      case _ => Set.empty[String]
    }
  }

  private def checkUserId(email: String): Int = {
    val users = mapper.readTree(metabaseUtil.listUsers()).path("data").elements().asScala

    users.find(_.get("email").asText().equalsIgnoreCase(email)) match {
      case Some(user) =>
        val id = user.get("id").asInt()
        if (user.get("is_active").asBoolean()) {
          println(s"User already exists and is active: $email with id: $id")
          id
        } else {
          println(s"User with email: $email exists but has been deactivated (id: $id)")
          id
        }
      case None =>
        -1
    }
  }

  private def createUser(firstName: String, email: String, password: String): Int = {
    val requestBody =
      s"""
         |{
         |  "first_name": "$firstName",
         |  "email": "$email",
         |  "password": "$password"
         |}
         |""".stripMargin
    val newUserId = mapper.readTree(metabaseUtil.createUser(requestBody)).get("id").asInt()
    println(s"User created: $email with id: $newUserId")
    newUserId
  }

  private def addUserToGroup(userRole: String, stateName: Option[String] = None, districtName: Option[String] = None, programName: Option[String] = None, userId: Int): Unit = {

    val existingUserGroups = metabaseUtil.listGroups()
    val groupName = userRole match {
      case "report_admin" => s"Report_Admin"
      case "state_manager" => s"${stateName.getOrElse("")}_State_Manager"
      case "district_manager" => s"${districtName.getOrElse("")}_District_Manager[${stateName.getOrElse("")}]"
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
      case "state_manager" => s"${stateName.getOrElse("")}_State_Manager"
      case "district_manager" => s"${districtName.getOrElse("")}_District_Manager[${stateName.getOrElse("")}]"
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

  private def checkGroupId(groupName: String): Int = {
    val existingUserGroups = metabaseUtil.listGroups()
    val groupIdOpt = findGroupId(existingUserGroups, groupName)
    groupIdOpt match {
      case Some(groupId) =>
        println(s"Found group id as $groupId for group name $groupName")
        groupId
      case None =>
        println(s"No group found for $groupName. Skipping removal.")
        -1
    }
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

  private def pushNotification(name: String, email: String, password: String, phone: String, context: ProcessFunction[Event, Event]#Context): Unit = {

    val notificationType = config.notificationType
    val notificationApiUrl = config.notificationApiUrl
    val notificationEmailTemplate = config.notificationEmailTemplate
    val notificationSmsTemplate = config.notificationSmsTemplate
    val hasEmail = email != null && email.nonEmpty
    val hasPhone = phone != null && phone.nonEmpty

    val replacementsForNotification = Map(
      "name" -> name,
      "email" -> email,
      "password" -> password,
      "dashboardLink" -> config.metabaseDomainName,
      "mobile" -> phone
    )

    def replacePlaceholders(template: String, values: Map[String, String]): String = {
      //println(s"Replacement Values:\n$values")
      val replaced = values.foldLeft(template) {
        case (temp, (key, value)) =>
          val replacement = if (value == null || value.trim.isEmpty) "null" else value
          val updated = temp.replaceAllLiterally(s"{$key}", replacement)
          //println(s"""Replacing {$key} with $replacement""")
          updated
      }
      val json = JSONUtil.mapper.readTree(replaced).toPrettyString
      json
    }

    val emailJson = replacePlaceholders(notificationEmailTemplate, replacementsForNotification)
    val smsJson = replacePlaceholders(notificationSmsTemplate, replacementsForNotification)

    if (notificationType == "kafka") {
      println(s"----> Pushing notification via kafka")
      val emailEvent = ScalaJsonUtil.serialize(emailJson)
      val smsEvent = ScalaJsonUtil.serialize(smsJson)
      if (hasEmail && hasPhone) {
        context.output(config.eventOutputTag, emailEvent)
        context.output(config.eventOutputTag, smsEvent)
      } else if (hasEmail) {
        context.output(config.eventOutputTag, emailEvent)
      } else if (hasPhone) {
        context.output(config.eventOutputTag, smsEvent)
      }
      println(s"----> Pushed new Kafka message to ${config.outputTopic} topic")
    } else if (notificationType == "api") {
      println(s"----> Pushing notification via api")
      var emailResponse: Option[requests.Response] = None
      var smsResponse: Option[requests.Response] = None
      if (hasEmail) {
        println(emailJson)
        emailResponse = Some(
          requests.post(
            notificationApiUrl,
            data = emailJson,
            headers = Map("Content-Type" -> "application/json")
          )
        )
      }

      if (hasPhone) {
        println(smsJson)
        smsResponse = Some(
          requests.post(
            notificationApiUrl,
            data = smsJson,
            headers = Map("Content-Type" -> "application/json")
          )
        )
      }

      val success = Seq(emailResponse, smsResponse).flatten.exists(_.statusCode == 200)

      if (success) {
        println("----> Pushed notification via API")
      } else {
        throw new Exception(
          s"""Failed to send notification:
             |Email status: ${emailResponse.map(_.statusCode).getOrElse("N/A")}, message: ${emailResponse.map(_.text).getOrElse("N/A")}
             |SMS status: ${smsResponse.map(_.statusCode).getOrElse("N/A")}, message: ${smsResponse.map(_.text).getOrElse("N/A")}
     """.stripMargin
        )
      }
    }
  }

}