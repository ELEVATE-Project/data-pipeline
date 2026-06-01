package org.shikshalokam.job.combined.dashboard.creator.functions.userMapping

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.combined.dashboard.creator.domain.UserMappingEvent
import org.shikshalokam.job.combined.dashboard.creator.task.CombinedDashboardCreatorConfig
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{JSONUtil, MetabaseUtil, PostgresUtil, ScalaJsonUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.security.SecureRandom
import scala.collection.JavaConverters._
import scala.collection.immutable.{Map, _}

class UserServiceFunction(config: CombinedDashboardCreatorConfig)(implicit val mapTypeInfo: TypeInformation[UserMappingEvent], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[UserMappingEvent, UserMappingEvent](config) {

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

  override def processElement(event: UserMappingEvent, context: ProcessFunction[UserMappingEvent, UserMappingEvent]#Context, metrics: Metrics): Unit = {
    try {
      logger.info("Start of Processing the User Service Event")

      val (entity, eventType) = (event.entity, event.eventType)

    val isUpdateEvent = (eventType == "update" || eventType == "bulk-update") &&
      Option(event.oldValues).exists(_.nonEmpty) &&
      Option(event.newValues).exists(_.nonEmpty)

    val sourceMap: Map[String, Any] = if (isUpdateEvent) event.oldValues else Map.empty

    def getValue[T](key: String, default: T): T = sourceMap.getOrElse(key, default).asInstanceOf[T]

    def getLabelFromSourceOrDefault(sourceMap: Map[String, Any], key: String, defaultLabel: String): String = {
      sourceMap.get(key) match {
        case Some(map: Map[String, Any] @unchecked) =>
          map.get("id").map(_.toString).getOrElse(defaultLabel)
        case _ => defaultLabel
      }
    }

    // Fetching from oldValues or using fallback
    val name = getValue("name", event.name)
    val uniqueUserName = getValue("username", event.username)
    val tenantCode = getValue("tenant_code", event.tenantCode)
    val email = Option(getValue("email", event.email)).filter(_.trim.nonEmpty).getOrElse(uniqueUserName + config.domainName)
    val password = generatePassword(10)
    val phone = getValue("phone", event.phone)
    val stateId = getLabelFromSourceOrDefault(sourceMap, "state", event.stateId)
    val districtId = getLabelFromSourceOrDefault(sourceMap, "district", event.districtId)
    val status = getValue("status", event.status)
    val isUserDeleted = getValue("deleted", event.isUserDeleted)
    val orgDetails = getValue("organizations", event.organizations)
    var userRoles: List[Map[String, Any]] = orgDetails.flatMap(_.get("roles").collect { case roles: List[Map[String, Any]] @unchecked => roles }.getOrElse(Nil))
    val orgIdOpt: Option[Int] = orgDetails.headOption.flatMap(_.get("id")).flatMap {
      case i: Int => Some(i)
      case l: Long => Some(l.toInt)
      case s: String => scala.util.Try(s.toInt).toOption
      case _ => None
    }

    if (isUpdateEvent) {
      userRoles ++= event.newValues
        .get("organizations")
        .collect { case orgs: List[Map[String, Any]] @unchecked => orgs }
        .getOrElse(Nil)
        .flatMap(_.get("roles").collect {
          case roles: List[Map[String, Any]] @unchecked => roles
        }.getOrElse(Nil))
    }

    logger.info(s"Entity = $entity")
    logger.info(s"EntityType = $eventType")
    logger.info(s"User Name = $name")
    logger.info(s"Unique User Name = $uniqueUserName")
    logger.info(s"Tenant Code = $tenantCode")
    logger.info(s"Org Id = ${orgIdOpt.getOrElse(-1)}")
    logger.info(s"Email = $email")
    logger.info(s"Password = $password")
    logger.info(s"Phone = $phone")
    logger.info(s"State Id = $stateId")
    logger.info(s"District ID = $districtId")
    logger.info(s"Status = $status")
    logger.info(s"Is User Deleted = $isUserDeleted")
    logger.info(s"User Organizations = $orgDetails")
    logger.info(s"User Role = $userRoles")

    if (entity == "user" && eventType == "delete") {
      val userId = checkUserId(email)
      if (userId != -1) metabaseUtil.deleteUser(userId)
    }

    userRoles.foreach { roleMap =>
      roleMap.get("title") match {
        case Some("report_admin") =>
          handleReportAdmin(entity, eventType, name, email, password, uniqueUserName)
        case Some("tenant_admin") =>
          handleTenantAdmin(entity, eventType, name, email, password, uniqueUserName, Some(tenantCode))
        case Some("org_admin") =>
          handleOrgAdmin(entity, eventType, name, email, password, uniqueUserName, orgIdOpt)
        case Some("state_manager") =>
          handleStateAdmin(entity, eventType, name, email, password, uniqueUserName, stateId)
        case Some("district_manager") =>
          handleDistrictUser(entity, eventType, name, email, password, uniqueUserName, stateId, districtId)
        case Some("program_manager") =>
          handleProgramUser(entity, eventType, name, email, password, uniqueUserName)
        case Some(unknownRole) =>
          logger.info(s"Unknown Metabase Platform Role: $unknownRole")
        case None =>
          logger.info("Role not found in map")
      }
    }

    def handleReportAdmin(entity: String, eventType: String, name: String, email: String, password: String, uniqueUserName: String): Unit = {
      logger.info("<<<======== Processing for the role report_admin ========>>>")
      if (entity == "user" && (eventType == "create" || eventType == "bulk-create")) {
        val userId = checkUserId(email)
        if (userId == -1) {
          val newUserId = createUser(name, email, password, uniqueUserName)
          addUserToGroup("report_admin", None, None, newUserId)
          pushNotification(name, email, password, phone, context)
        } else {
          logger.info("Stopped processing")
        }
      }
      else if (entity == "user" && (eventType == "update" || eventType == "bulk-update")) {
        val oldRoles = extractRoles(event.oldValues)
        val newRoles = extractRoles(event.newValues)
        val hadReportAdmin = oldRoles.contains("report_admin")
        val hasReportAdmin = newRoles.contains("report_admin")
        (hadReportAdmin, hasReportAdmin) match {
          case (false, true) =>
            logger.info("Trying to add user to report_admin role")
            val userId = checkUserId(email)
            if (userId == -1) {
              val newUserId = createUser(name, email, password, uniqueUserName)
              addUserToGroup("report_admin", None, None, newUserId)
              pushNotification(name, email, password, phone, context)
            } else {
              addUserToGroup("report_admin", None, None, userId)
            }
          case (true, false) =>
            logger.info("Trying to remove user from report_admin role")
            val userId = checkUserId(email)
            if (userId != -1) removeUserFromGroup("report_admin", None, None, userId)
          case (true, true) =>
            logger.info("User already had and still has report_admin role")
          case _ => // No action needed
        }
      }
    }

    def handleTenantAdmin(entity: String, eventType: String, name: String, email: String, password: String, uniqueUserName: String, tenantCodeOpt: Option[String]): Unit = {
      val tcOpt = tenantCodeOpt.map(_.trim).filter(_.nonEmpty)
      if (tcOpt.isEmpty) {
        logger.info("Missing tenant_code for tenant_admin; skipping.")
        return
      }
      val tc = tcOpt.get
      logger.info(s"<<<======== Processing for the role Tenant_Admin_$tc ========>>>")
      if (entity == "user" && (eventType == "create" || eventType == "bulk-create")) {
        val userId = checkUserId(email)
        if (userId == -1) {
          val newUserId = createUser(name, email, password, uniqueUserName)
          addUserToGroup("tenant_admin", None, None, newUserId, Some(tc))
          pushNotification(name, email, password, phone, context)
        } else {
          logger.info("Stopped processing")
        }
      }
      else if (entity == "user" && (eventType == "update" || eventType == "bulk-update")) {
        val oldRoles = extractRoles(event.oldValues)
        val newRoles = extractRoles(event.newValues)
        val hadTenantAdmin = oldRoles.contains("tenant_admin")
        val hasTenantAdmin = newRoles.contains("tenant_admin")
        (hadTenantAdmin, hasTenantAdmin) match {
          case (false, true) =>
            logger.info("Trying to add user to tenant_admin role")
            val userId = checkUserId(email)
            if (userId == -1) {
              val newUserId = createUser(name, email, password, uniqueUserName)
              addUserToGroup("tenant_admin", None, None, newUserId, Some(tc))
              pushNotification(name, email, password, phone, context)
            } else {
              addUserToGroup("tenant_admin", None, None, userId, Some(tc))
            }
          case (true, false) =>
            logger.info("Trying to remove user from tenant_admin role")
            val userId = checkUserId(email)
            if (userId != -1) removeUserFromGroup("tenant_admin", None, None, userId, Some(tc))
          case (true, true) =>
            logger.info("User already had and still has tenant_admin role")
          case _ => // No action needed
        }
      }
    }

    def handleOrgAdmin(entity: String, eventType: String, name: String, email: String, password: String, uniqueUserName: String, orgId: Option[Int]): Unit = {
      val orgIdOpt = orgId.filter(_ > 0)
      if (orgIdOpt.isEmpty) {
        logger.info("Missing orgId for org_admin; skipping.")
        return
      }
      val orgIdSafe = orgIdOpt.get
      logger.info(s"<<<======== Processing for the role Org_Admin_$orgIdSafe ========>>>")
      if (entity == "user" && (eventType == "create" || eventType == "bulk-create")) {
        val userId = checkUserId(email)
        if (userId == -1) {
          val newUserId = createUser(name, email, password, uniqueUserName)
          addUserToGroup("org_admin", None, None, newUserId, None, Some(orgIdSafe))
          pushNotification(name, email, password, phone, context)
        } else {
          logger.info("Stopped processing")
        }
      }
      else if (entity == "user" && (eventType == "update" || eventType == "bulk-update")) {
        val oldRoles = extractRoles(event.oldValues)
        val newRoles = extractRoles(event.newValues)
        val hadOrgAdmin = oldRoles.contains("org_admin")
        val hasOrgAdmin = newRoles.contains("org_admin")
        (hadOrgAdmin, hasOrgAdmin) match {
          case (false, true) =>
            logger.info("Trying to add user to org_admin role")
            val userId = checkUserId(email)
            if (userId == -1) {
              val newUserId = createUser(name, email, password, uniqueUserName)
              addUserToGroup("org_admin", None, None, newUserId, None, Some(orgIdSafe))
              pushNotification(name, email, password, phone, context)
            } else {
              addUserToGroup("org_admin", None, None, userId, None, Some(orgIdSafe))
            }
          case (true, false) =>
            logger.info("Trying to remove user from org_admin role")
            val userId = checkUserId(email)
            if (userId != -1) removeUserFromGroup("org_admin", None, None, userId, None, Some(orgIdSafe))
          case (true, true) =>
            logger.info("User already had and still has org_admin role")
          case _ => // No action needed
        }
      }
    }


    def handleStateAdmin(entity: String, eventType: String, name: String, email: String, password: String, uniqueUserName: String, stateId: String): Unit = {
      logger.info("<<<======== Processing for the role state_manager ========>>>")
      if (entity == "user" && (eventType == "create" || eventType == "bulk-create")) {
        val userId = checkUserId(email)
        if (userId == -1) {
          val newUserId = createUser(name, email, password, uniqueUserName)
          addUserToGroup("state_manager", Some(stateId), None, newUserId)
          pushNotification(name, email, password, phone, context)
        } else {
          logger.info("Stopped processing")
        }
      }
      else if (entity == "user" && (eventType == "update" || eventType == "bulk-update")) {
        val oldRoles = extractRoles(event.oldValues)
        val newRoles = extractRoles(event.newValues)
        val hadReportAdmin = oldRoles.contains("state_manager")
        val hasReportAdmin = newRoles.contains("state_manager")
        (hadReportAdmin, hasReportAdmin) match {
          case (false, true) =>
            logger.info("Trying to add user to state_manager role")
            val userId = checkUserId(email)
            if (userId == -1) {
              val newUserId = createUser(name, email, password, uniqueUserName)
              addUserToGroup("state_manager", Some(stateId), None, newUserId)
              pushNotification(name, email, password, phone, context)
            } else {
              addUserToGroup("state_manager", Some(stateId), None, userId)
            }
          case (true, false) =>
            logger.info("Trying to remove user from state_manager role")
            val userId = checkUserId(email)
            if (userId != -1) removeUserFromGroup("state_manager", Some(stateId), None, userId)
          case (true, true) =>
            logger.info("User already had and still has state_manager role")
          case _ => // No action needed
        }
      }
    }

    def handleDistrictUser(entity: String, eventType: String, name: String, email: String, password: String, uniqueUserName: String, stateId: String, districtId: String): Unit = {
      logger.info("<<<======== Processing for the role district_manager ========>>>")
      if (entity == "user" && (eventType == "create" || eventType == "bulk-create")) {
        val userId = checkUserId(email)
        if (userId == -1) {
          val newUserId = createUser(name, email, password, uniqueUserName)
          addUserToGroup("district_manager", Some(stateId), Some(districtId), newUserId)
          pushNotification(name, email, password, phone, context)
        } else {
          logger.info("Stopped processing")
        }
      }
      else if (entity == "user" && (eventType == "update" || eventType == "bulk-update")) {
        val oldRoles = extractRoles(event.oldValues)
        val newRoles = extractRoles(event.newValues)
        val hadReportAdmin = oldRoles.contains("district_manager")
        val hasReportAdmin = newRoles.contains("district_manager")
        (hadReportAdmin, hasReportAdmin) match {
          case (false, true) =>
            logger.info("Trying to add user to report_admin role")
            val userId = checkUserId(email)
            if (userId == -1) {
              val newUserId = createUser(name, email, password, uniqueUserName)
              addUserToGroup("district_manager", Some(stateId), Some(districtId), newUserId)
              pushNotification(name, email, password, phone, context)
            } else {
              addUserToGroup("district_manager", Some(stateId), Some(districtId), userId)
            }
          case (true, false) =>
            logger.info("Trying to remove user from district_manager role")
            val userId = checkUserId(email)
            if (userId != -1) removeUserFromGroup("district_manager", Some(stateId), Some(districtId), userId)
          case (true, true) =>
            logger.info("User already had and still has district_manager role")
          case _ => // No action needed
        }
      }
    }

    def handleProgramUser(entity: String, eventType: String, name: String, email: String, password: String, uniqueUserName: String): Unit = {
      logger.info("<<<======== Processing for the role program_manager ========>>>")
      if (entity == "user" && (eventType == "create" || eventType == "bulk-create")) {
        val userId = checkUserId(email)
        if (userId == -1) {
          createUser(name, email, password, uniqueUserName)
          pushNotification(name, email, password, phone, context)
        } else {
          logger.info("Stopped processing")
        }
      }
      else if (entity == "user" && (eventType == "update" || eventType == "bulk-update")) {
        val oldRoles = extractRoles(event.oldValues)
        val newRoles = extractRoles(event.newValues)
        val hadReportAdmin = oldRoles.contains("program_manager")
        val hasReportAdmin = newRoles.contains("program_manager")
        (hadReportAdmin, hasReportAdmin) match {
          case (false, true) =>
            logger.info("Trying to add user to program_manager role")
            val userId = checkUserId(email)
            if (userId == -1) {
              createUser(name, email, password, uniqueUserName)
              pushNotification(name, email, password, phone, context)
            } else {
              logger.info("Stopped processing")
            }
          case _ => // No action needed
        }
      }
    }

    logger.info(s"***************** End of Processing the User Service Event *****************")
    } catch {
      case e: Exception =>
        logger.error(s"Error processing User Service Event: ${e.getMessage}", e)
    }
  }

  private def generatePassword(length: Int = 12): String = {
    val charSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()-_=+"
    val random = new SecureRandom()
    (1 to length).map(_ => charSet(random.nextInt(charSet.length))).mkString
  }

  private def extractRoles(data: Map[String, Any]): Set[String] = {
    data.get("organizations") match {
      case Some(orgs: List[Map[String, Any]] @unchecked) =>
        orgs.flatMap(_.get("roles").collect {
          case roles: List[Map[String, Any]] @unchecked =>
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
          logger.info(s"User already exists and is active: $email with id: $id")
          id
        } else {
          logger.info(s"User with email: $email exists but has been deactivated (id: $id)")
          id
        }
      case None =>
        -1
    }
  }

  private def createUser(firstName: String, email: String, password: String, userName: String): Int = {
    val requestBody =
      s"""
         |{
         |  "first_name": "$firstName",
         |  "email": "$email",
         |  "password": "$password",
         |  "login_attributes": {
         |        "userName": "$userName"
         |    }
         |}
         |""".stripMargin
    val newUserId = mapper.readTree(metabaseUtil.createUser(requestBody)).get("id").asInt()
    logger.info(s"User created: $email with id: $newUserId")
    newUserId
  }

  private def addUserToGroup(userRole: String, stateId: Option[String] = None, districtId: Option[String] = None, userId: Int, tenantCode: Option[String] = None, orgId: Option[Int] = None): Unit = {

    if (userRole == "tenant_admin" && tenantCode.forall(tc => tc == null || tc.trim.isEmpty)) {
      logger.info("tenant_admin requires tenantCode; skipping group assignment")
      return
    }

    if (userRole == "org_admin" && orgId.forall(_ <= 0)) {
      logger.info("org_admin requires orgId; skipping group assignment")
      return
    }

    val existingUserGroups = metabaseUtil.listGroups()

    val groupNames: List[String] = userRole match {
      case "report_admin" =>
        List("Report_Admin_Micro_Improvement", "Report_Admin_National_Overview", "Report_Admin_Programs", "Report_Admin_User_Activity")
      case "tenant_admin" =>
        List(s"Tenant_Admin_${tenantCode.map(_.trim).getOrElse("")}", s"Tenant_Admin_Mentoring_${tenantCode.map(_.trim).getOrElse("")}")
      case "org_admin" =>
        List(s"Org_Admin_Mentoring_${orgId.get}")
      case "state_manager" =>
        List(s"State_Manager_${stateId.getOrElse("")}")
      case "district_manager" =>
        List(s"District_Manager_${districtId.getOrElse("")}")
      case _ =>
        throw new IllegalArgumentException("Invalid manager type")
    }

    groupNames.foreach { groupName =>
      findGroupId(existingUserGroups, groupName) match {
        case Some(id) =>
          logger.info(s"Found group id as $id for group name $groupName")
          validateUserInGroup(userId, id)
        case None =>
          logger.info(s"No group found for $groupName. Ask Super Admin to create the group")
      }
    }
  }


  private def removeUserFromGroup(userRole: String, stateId: Option[String] = None, districtId: Option[String] = None, userId: Int, tenantCode: Option[String] = None, orgId: Option[Int] = None): Unit = {

    if (userRole == "tenant_admin" && tenantCode.forall(tc => tc == null || tc.trim.isEmpty)) {
      logger.info("tenant_admin requires tenantCode; skipping group removal")
      return
    }

    if (userRole == "org_admin" && orgId.forall(_ <= 0)) {
      logger.info("org_admin requires orgId; skipping group removal")
      return
    }

    val existingUserGroups = metabaseUtil.listGroups()

    val groupNames: List[String] = userRole match {
      case "report_admin" =>
        List("Report_Admin_Micro_Improvement", "Report_Admin_National_Overview", "Report_Admin_Programs", "Report_Admin_User_Activity")
      case "tenant_admin" =>
        List(s"Tenant_Admin_${tenantCode.map(_.trim).getOrElse("")}", s"Tenant_Admin_Mentoring_${tenantCode.map(_.trim).getOrElse("")}")
      case "org_admin" =>
        List(s"Org_Admin_Mentoring_${orgId.get}")
      case "state_manager" =>
        List(s"State_Manager_${stateId.getOrElse("")}")
      case "district_manager" =>
        List(s"District_Manager_${districtId.getOrElse("")}")
      case _ =>
        throw new IllegalArgumentException("Invalid manager type")
    }

    groupNames.foreach { groupName =>
      findGroupId(existingUserGroups, groupName) match {
        case Some(groupId) =>
          logger.info(s"Found group id as $groupId for group name $groupName")
          validateUserRemoval(userId, groupId)
        case None =>
          logger.info(s"No group found for $groupName. Skipping removal.")
      }
    }
  }

  private def findGroupId(existingGroupDetails: String, groupName: String): Option[Int] = {
    val groupDetailsJson = mapper.readTree(existingGroupDetails)
    groupDetailsJson.elements().asScala
      .find(node => node.get("name").asText() == groupName)
      .map(node => node.get("id").asInt())
  }

  private def validateUserInGroup(userId: Int, groupId: Int): Unit = {
    val isUserInGroup = mapper.readTree(metabaseUtil.getGroupDetails(groupId))
      .get("members")
      .elements()
      .asScala
      .exists(_.get("user_id").asInt() == userId)
    if (!isUserInGroup) addToGroup(userId, groupId) else logger.info("User is already a member of the group")
  }

  private def validateUserRemoval(userId: Int, groupId: Int): Unit = {
    val groupDetails = mapper.readTree(metabaseUtil.getGroupDetails(groupId))
    val isUserInGroup = groupDetails.get("members").elements().asScala.exists(_.get("user_id").asInt() == userId)
    val membershipId = groupDetails.get("members").elements().asScala.find(_.get("user_id").asInt() == userId).get.get("membership_id").asInt()
    if (isUserInGroup) {
      logger.info(s"User with Id $userId is member of the group $groupId")
      logger.info(s"Removing user from the group with membership Id $membershipId")
      metabaseUtil.removeFromGroup(membershipId)
    } else logger.info("User is not a member of the group")
  }

  private def checkGroupId(groupName: String): Int = {
    val existingUserGroups = metabaseUtil.listGroups()
    val groupIdOpt = findGroupId(existingUserGroups, groupName)
    groupIdOpt match {
      case Some(groupId) =>
        logger.info(s"Found group id as $groupId for group name $groupName")
        groupId
      case None =>
        logger.info(s"No group found for $groupName. Skipping removal.")
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
    logger.info("User added to group")
  }

  private def pushNotification(name: String, email: String, password: String, phone: String, context: ProcessFunction[UserMappingEvent, UserMappingEvent]#Context): Unit = {

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
      logger.info(s"----> Pushing notification via kafka")
      val emailEvent = ScalaJsonUtil.serialize(emailJson)
      val smsEvent = ScalaJsonUtil.serialize(smsJson)
      if (hasEmail && hasPhone) {
        context.output(config.userServiceOutputTag, emailEvent)
        context.output(config.userServiceOutputTag, smsEvent)
      } else if (hasEmail) {
        context.output(config.userServiceOutputTag, emailEvent)
      } else if (hasPhone) {
        context.output(config.userServiceOutputTag, smsEvent)
      }
      logger.info(s"Pushed new Kafka message to ${config.notificationOutputTopic} topic")
    } else if (notificationType == "api") {
      logger.info(s"----> Pushing notification via api")
      var emailResponse: Option[requests.Response] = None
      var smsResponse: Option[requests.Response] = None
      if (hasEmail) {
        try {
          // logger.debug(emailJson)
          val response = requests.post(
            notificationApiUrl,
            data = emailJson,
            headers = Map("Content-Type" -> "application/json")
          )
          emailResponse = Some(response)
          logger.info(s"Email sent with status: ${response.statusCode}")
        } catch {
          case e: Exception =>
            logger.error(s"Failed to send email notification: ${e.getMessage}", e)
        }
      }

      if (hasPhone) {
        try {
          // logger.debug(smsJson)
          val response = requests.post(
            notificationApiUrl,
            data = smsJson,
            headers = Map("Content-Type" -> "application/json")
          )
          smsResponse = Some(response)
          logger.info(s"SMS sent with status: ${response.statusCode}")
        } catch {
          case e: Exception =>
            logger.error(s"Failed to send SMS notification: ${e.getMessage}", e)
        }
      }

      val success = Seq(emailResponse, smsResponse).flatten.exists(_.statusCode == 200)

      if (success) {
        logger.info("----> Pushed notification via API")
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