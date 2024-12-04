package org.shikshalokam.job.users.via.csv.functions

import com.typesafe.config.ConfigFactory
import org.shikshalokam.job.users.via.csv.models.CsvSchema
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.matching.Regex

object MetabaseUserManagementFunction {

  private val config = ConfigFactory.load()
  private val sinkDirectory = config.getString("file.sinkDirectory")
  private val pgHost = config.getString("postgres.host")
  private val pgPort = config.getString("postgres.port")
  private val pgUsername = config.getString("postgres.username")
  private val pgPassword = config.getString("postgres.password")
  private val pgDataBase = config.getString("postgres.database")
  private val metabaseUrl = config.getString("metabase.url")
  private val metabaseUsername = config.getString("metabase.username")
  private val metabasePassword = config.getString("metabase.password")
  private val connectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$pgDataBase"
  private val postgresUtil = new PostgresUtil(connectionUrl, pgUsername, pgPassword)
  private val metabaseUtil = new MetabaseUtil(metabaseUrl, metabaseUsername, metabasePassword)

  def processCsvFile(filename: String): Unit = {
    val filePath = Paths.get(sinkDirectory, filename).toString
    if (Files.exists(Paths.get(filePath))) {
      val source = Source.fromFile(filePath)
      try {
        val header = source.getLines().take(1).mkString
        val headerColumns = header.split(",").map(_.trim)
        val headersMatch = CsvSchema.headers.diff(headerColumns).isEmpty
        if (headersMatch) {
          var rowCount = 0
          for (line <- source.getLines()) {
            rowCount += 1
            val columns = parseCsvLine(line)
            val columnMap = headerColumns.zip(columns).toMap
            val firstName = columnMap.get("firstName").getOrElse(null)
            val lastName = columnMap.get("lastName").getOrElse(null)
            val email = columnMap.get("email").getOrElse(null)
            val password = columnMap.get("password").getOrElse(null)
            val roles = columnMap.get("roles").getOrElse(null)
            val stateId = columnMap.get("stateId").getOrElse(null)
            val districtId = columnMap.get("districtId").getOrElse(null)
            val programName = columnMap.get("programName").getOrElse(null)

            println(s"Processing row $rowCount : firstName=$firstName, lastName=$lastName, email=$email, password=$password, roles=$roles, stateId=$stateId, districtId=$districtId, programName=$programName")
            val userId = checkAndInsertUser(firstName, lastName, email, password)
            assignDashboardToUser(userId, roles, stateId, districtId, programName)
            println(s"******* Row $rowCount Processed *******\n")
          }
        } else {
          println("CSV header does not match the expected schema!")
        }
      } finally {
        source.close()
      }
    } else {
      println(s"File $filePath does not exist!")
    }
  }

  private def checkAndInsertUser(firstName: String, lastName: String, email: String, password: String): Int = {
    val existingUserJson = mapper.readTree(metabaseUtil.listUsers())
    val emailExists = existingUserJson.path("data").elements().asScala
      .find(_.get("email").asText() == email.toLowerCase)
      .map(user => user.get("id").asInt())

    emailExists match {
      case Some(id) =>
        println(s"User already exists: $email with id: $id")
        val updateUserRequestBody =
          s"""
             |{
             |  "first_name": "$firstName",
             |  "last_name": "$lastName"
             |}
             |""".stripMargin
        metabaseUtil.updateUser(id, updateUserRequestBody)
        val updatePasswordRequestBody = s"""{ "password": "$password" }"""
        metabaseUtil.updatePassword(id, updatePasswordRequestBody)
        println(s"User with id : $id updated:")
        id

      case None =>
        println(s"User does not exist: $email")
        val requestBody =
          s"""
             |{
             |  "first_name": "$firstName",
             |  "last_name": "$lastName",
             |  "email": "$email",
             |  "password": "$password"
             |}
             |""".stripMargin
        val createUser = mapper.readTree(metabaseUtil.createUser(requestBody))
        val newUserId = createUser.get("id").asInt()
        println(s"User with id: $newUserId created:")
        newUserId
    }
  }

  private def assignDashboardToUser(userId: Int, userRoles: String, stateId: String, districtId: String, programName: String): Unit = {

    val userRolesList = userRoles.split(",").map(_.trim).toList
    val existingUserGroups = metabaseUtil.listGroups()
    userRolesList.foreach {
      case "state_manager" =>
        if (stateId == null || stateId.isEmpty) {
          println("Error: stateId is null or empty. Cannot proceed with handling State Manager.")
        } else {
          handleManager("State", stateId = Some(stateId), existingGroupDetails = existingUserGroups, userId = userId)
        }

      case "district_manager" =>
        if (stateId == null || stateId.isEmpty || districtId == null || districtId.isEmpty) {
          println("Error: stateId or districtId cannot be null or empty. Cannot proceed with handling District Manager.")
        } else {
          handleManager("District", stateId = Some(stateId), districtId = Some(districtId), existingGroupDetails = existingUserGroups, userId = userId)
        }

      case "program_manager" =>
        if (programName == null || programName.isEmpty) {
          println("Error: programName is null or empty. Cannot proceed with handling Program Manager.")
        } else {
          handleManager("Program", programName = Some(programName), existingGroupDetails = existingUserGroups, userId = userId)
        }

      case otherRole =>
        println(s"Unknown role: $otherRole")
    }

  }

  private def handleManager(managerType: String, stateId: Option[String] = None, districtId: Option[String] = None, programName: Option[String] = None, existingGroupDetails: String, userId: Int): Unit = {

    println(s"--> $managerType Manager method called")
    val stateName = stateId.map(id => fetchName(s"SELECT DISTINCT state_name FROM projects WHERE state_id = '$id';", "state_name")).getOrElse("")
    val districtName = districtId.map(id => fetchName(s"SELECT DISTINCT district_name FROM projects WHERE district_id = '$id';", "district_name")).getOrElse("")

    val groupName = managerType match {
      case "State" => s"${stateName}_State_Manager"
      case "District" => s"${districtName}_District_Manager[$stateName]"
      case "Program" => s"Program_Manager[${programName.getOrElse("")}]"
      case _ => throw new IllegalArgumentException("Invalid manager type")
    }
    val groupId = findGroupId(existingGroupDetails, groupName)
    groupId match {
      case Some(id) =>
        println(s"Found group id as $id for group name $groupName")
        validateUserInGroup(userId, id)
      case None => println(s"No group found for $groupName. Ask Super Admin to create the group")
    }
  }

  private def parseCsvLine(line: String): Array[String] = {
    val csvRegex: Regex = """\s*(?:"([^"]*)"|([^",]*))(?:,|$)""".r
    csvRegex.findAllMatchIn(line).map {
      m => Option(m.group(1)).getOrElse(m.group(2))
    }.toArray
  }

  private def fetchName(query: String, key: String): String = {
    val result = postgresUtil.fetchData(query)
    result.flatMap(_.get(key).map(_.toString)).headOption.getOrElse("Unknown")
  }

  private def findGroupId(existingGroupDetails: String, groupName: String): Option[Int] = {
    val groupDetailsJson = mapper.readTree(existingGroupDetails)
    groupDetailsJson.elements().asScala
      .find(node => node.get("name").asText() == groupName)
      .map(node => node.get("id").asInt())
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

  private def validateUserInGroup(userId: Int, groupId: Int) = {
    val isUserInGroup = mapper.readTree(metabaseUtil.getGroupDetails(groupId))
      .get("members")
      .elements()
      .asScala
      .exists(_.get("user_id").asInt() == userId)
    if (!isUserInGroup) addToGroup(userId, groupId) else println("User is already a member of the group")
  }

}



