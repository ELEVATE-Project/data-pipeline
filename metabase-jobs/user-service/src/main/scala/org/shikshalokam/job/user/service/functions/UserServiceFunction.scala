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
import scala.collection.immutable._

class UserServiceFunction(config: UserServiceConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserServiceFunction])

  override def metricsList(): List[String] = {
    List(config.userServiceCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
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

    def getMetaValue(meta: Map[String, Any], key: String): String =
      meta.get(key).collect {
        case m: Map[_, _] => m.asInstanceOf[Map[String, Any]].get("name")
      }.flatten.collect { case s: String => s }.getOrElse(null)

    val entity = event.entity
    val eventType = event.eventType
    val name = event.name
    val uniqueUserName = event.username
    val meta = event.metaInformation
    val status = event.status
    val isUserDeleted = event.isUserDeleted
    val orgDetails = event.organizations
    val userRoles: List[Map[String, Any]] = event.organizations.flatMap(_.get("roles").collect { case roles: List[Map[String, Any]] @unchecked => roles }.getOrElse(Nil))
    val stateName = getMetaValue(event.metaInformation, "state")
    val districtName = getMetaValue(event.metaInformation, "district")
    //TODO get program info
    val email = uniqueUserName + config.domainName
    val password = uniqueUserName + config.domainPassword



    println(s"Entity = $entity")
    println(s"EntityType = $eventType")
    println(s"User Name = $name")
    println(s"Unique User Name = $uniqueUserName")
    println(s"Meta Information = $meta")
    println(s"Status = $status")
    println(s"Is User Deleted = $isUserDeleted")
    println(s"User Organizations = $orgDetails")
    println(s"User Role = $userRoles")
    println(s"State Name = $stateName")
    println(s"District Name = $districtName")
    println(s"Email = $email")
    println(s"Password = $password")
    println("\n\n")


//    val existingUserGroups = metabaseUtil.listGroups()
//    val groupId = findGroupId(existingUserGroups, "Report_Admin")
//
//    println(existingUserGroups)
//    println(groupId)


//    def getAllSubCollectionIds(parentId: Int): List[Int] = {
//      val collectionsJson = metabaseUtil.listCollections()
//      println("LISTING COLLECTION = " + collectionsJson)
//      val collections = mapper.readTree(collectionsJson).elements().asScala.toList
//      println(collections)
//
//      def recurse(currentId: Int): List[Int] = {
//        val children = collections.filter(col => col.get("parent_id").asInt() == currentId)
//        val childIds = children.map(_.get("id").asInt())
//        childIds ++ childIds.flatMap(recurse)
//      }
//      recurse(parentId)
//    }
//
//
//    val allCollectionIds = getAllSubCollectionIds(450)
//    println(allCollectionIds)
//    val collectionsPermissionMap = allCollectionIds.map(id => s""""$id": "read"""").mkString(",\n")
//
//    val addCollectionToUserRequestBody =
//      s"""
//         |{
//         |    "revision": $revisionId,
//         |    "groups": {
//         |        "$groupId": {
//         |            $collectionsPermissionMap
//         |        }
//         |    }
//         |}
//         |""".stripMargin






    userRoles.foreach { roleMap =>
      roleMap.get("title") match {
        case Some("report_admin") =>
          handleReportAdmin(entity, eventType, name, email, password)
        //        case Some("state_manager") =>
        //          handleStateAdmin()
        //        case Some("district_manager") =>
        //          handleDistrictUser()
        //        case Some("program_manager") =>
        //          handleProgramUser()
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
        } else {
          println("Stopped processing")
        }
      } else if (entity == "user" && eventType == "delete") {
        val userId = checkUserId(email)
        metabaseUtil.deleteUser(userId)
      }
      //TODO Check how to handle user update
      //else if(entity == "user" && eventType == "update") {
      //        val userId = checkUserId(email)
      //        metabaseUtil.deleteUser(userId)
      //}
    }



    //TODO METHOD to delete user
    //    metabaseUtil.deleteUser(userId)


  }

  //  def checkAndCreateUser(firstName: String, email: String, password: String): Int = {
  //    val users = mapper.readTree(metabaseUtil.listUsers()).path("data").elements().asScala
  //
  //    users.find(_.get("email").asText().equalsIgnoreCase(email)) match {
  //      case Some(user) =>
  //        val id = user.get("id").asInt()
  //        if (user.get("is_active").asBoolean()) {
  //          println(s"User already exists and is active: $email with id: $id")
  //          -1
  //        } else {
  //          println(s"User with email: $email exists but has been deactivated (id: $id)")
  //          -1
  //        }
  //
  //      case None =>
  //        val requestBody =
  //          s"""
  //             |{
  //             |  "first_name": "$firstName",
  //             |  "email": "$email",
  //             |  "password": "$password"
  //             |}
  //             |""".stripMargin
  //        val newUserId = mapper.readTree(metabaseUtil.createUser(requestBody)).get("id").asInt()
  //        println(s"User created: $email with id: $newUserId")
  //        newUserId
  //    }
  //  }

  //  def checkUserId(email: String): Option[Int] = {
  //    val users = mapper.readTree(metabaseUtil.listUsers()).path("data").elements().asScala
  //
  //    users.find(_.get("email").asText().equalsIgnoreCase(email)) match {
  //      case Some(user) =>
  //        val id = user.get("id").asInt()
  //        if (user.get("is_active").asBoolean()) {
  //          println(s"User already exists and is active: $email with id: $id")
  //          Some(-1)
  //        } else {
  //          println(s"User with email: $email exists but has been deactivated (id: $id)")
  //          Some(-1)
  //        }
  //      case None =>
  //        None
  //    }
  //  }

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

  private def addUserToGroup (userRole: String, stateName: Option[String] = None, districtName: Option[String] = None, programName: Option[String] = None, userId: Int): Unit = {

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