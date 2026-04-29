package org.shikshalokam.job.util

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.{List, Map}

class MetabaseUtil(url: String, metabaseUsername: String, metabasePassword: String, metabasePostgresUtil: PostgresUtil, postgresUtil: PostgresUtil) {

  private val metabaseUrl: String = url
  private val username: String = metabaseUsername
  private val password: String = metabasePassword
  //  println("Metabase URL: " + url)
  //  println("Username: " + username)
  //  println("Password: " + password)
  val storedTableIds = TrieMap.empty[(Int, String), Int]
  val storedColumnIds = TrieMap.empty[(Int, String), Int]

  private var sessionToken: Option[String] = None

  /**
   * Method to get or refresh the session token
   */
  private def authenticate(): String = {
    val url = s"$metabaseUrl/session"
    val requestBody = s"""{"username": "$username", "password": "$password"}"""

    val response = requests.post(url,
      data = requestBody,
      headers = Map("Content-Type" -> "application/json")
    )
    if (response.statusCode == 200) {
      val token = ujson.read(response.text)("id").str
      token
    } else {
      throw new Exception(s"Authentication failed with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to get or refresh the session token,
   * If cached token is not available get the token from authenticate method.
   */
  private def getSessionToken: String = {
    sessionToken match {
      case Some(token) =>
        //TODO : Remove bellow line
        //        println(s"SessionToken already exists: $token")
        token
      case None =>
        val token = authenticate()
        //TODO : Remove bellow line
        //        println(s"Generated new token: $token")
        sessionToken = Some(token)
        token
    }
  }

  /**
   * Method to fetch the Metabase database ID from the Metabase internal DB.
   *
   * @param metabaseDatabase Name of the Metabase database
   * @return Database ID as Int if found, otherwise -1
   */

  def getDatabaseID(metabaseDatabase: String): Int = {
    def escape(v: String) = v.replace("'", "''")

    val databaseID =
      metabasePostgresUtil
        .fetchData(s"SELECT id FROM metabase_database WHERE name = '${escape(metabaseDatabase)}' LIMIT 1")
        .headOption
        .flatMap(_.get("id"))
        .map(_.toString.toInt)
        .getOrElse {
          println(s"Database '$metabaseDatabase' not found.")
          -1
        }

    println(s"Database ID = $databaseID")
    databaseID
  }

  /**
   * Method to list collections from Metabase
   *
   * @return JSON string representing the collections
   */
  def listCollections(): String = {
    val url = s"$metabaseUrl/collection"

    val response = requests.get(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val collectionsJson = ujson.read(response.text).render()
      collectionsJson
    } else {
      throw new Exception(s"Failed to retrieve collections with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to list dashboards from Metabase
   *
   * @return JSON string representing the dashboards
   */
  def listDashboards(): String = {
    val url = s"$metabaseUrl/dashboard"

    val response = requests.get(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val dashboardsJson = ujson.read(response.text).render()
      dashboardsJson
    } else {
      throw new Exception(s"Failed to retrieve dashboards with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to get dashboard details by Id from Metabase
   *
   * @param dashboardId ID of the dashboard to retrieve details for
   * @return JSON string representing the dashboard details
   */
  def getDashboardDetailsById(dashboardId: Int): String = {
    val url = s"$metabaseUrl/dashboard/$dashboardId"
    val response = requests.get(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val getDashboardDetailsByIdJson = ujson.read(response.text).render()
      getDashboardDetailsByIdJson
    } else {
      throw new Exception(s"Failed to retrieve dashboard by Id with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * To update the column category in Metabase
   * ex state_name -> state and column_name -> city
   * first get the database id then table id(getTableDetailsByName) and then column id
   * (getColumnIdDetailsByName) and then call this method
   */

  def updateColumnCategory(columnId: Int, category: String): Unit = {
    val url = s"$metabaseUrl/field/$columnId"
    val semanticType = s"type/$category"
    val payload = ujson.Obj("semantic_type" -> semanticType)

    val response = requests.put(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      ),
      data = payload.render()
    )

    if (response.statusCode == 200) {
      println(s"Successfully updated column category for field ID: $columnId to $category")
    } else {
      throw new Exception(s"Failed to update column category: ${response.text()}")
    }
  }


  /**
   * Method to list database details from Metabase
   *
   * @return JSON string representing the database details
   */
  def listDatabaseDetails(): String = {
    val url = s"$metabaseUrl/database"

    val response = requests.get(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val databasesJson = ujson.read(response.text).render()
      databasesJson
    } else {
      throw new Exception(s"Failed to retrieve database details with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to get database metadata from Metabase
   *
   * @param databaseId ID of the database to retrieve metadata for
   * @return JSON string representing the database metadata
   */
  def getDatabaseMetadata(databaseId: Int): String = {
    val url = s"$metabaseUrl/database/$databaseId/metadata"

    val response = requests.get(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val databaseMetaDataJson = ujson.read(response.text).render()
      databaseMetaDataJson
    } else {
      throw new Exception(s"Failed to retrieve database metadata with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to create a new collection in Metabase
   *
   * @param requestData JSON string representing the collection data
   * @return JSON string representing the created collection
   */
  def createCollection(requestData: String): String = {
    val url = s"$metabaseUrl/collection"

    val response = requests.post(
      url,
      data = requestData,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val collectionResponseBody = ujson.read(response.text).render()
      collectionResponseBody
    } else {
      throw new Exception(s"Failed to create collection with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to create a new dashboard in Metabase
   *
   * @param requestData JSON string representing the dashboard data
   * @return JSON string representing the created dashboard
   */
  def createDashboard(requestData: String): String = {
    val url = s"$metabaseUrl/dashboard"

    val response = requests.post(
      url,
      data = requestData,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val dashboardResponseBody = ujson.read(response.text).render()
      dashboardResponseBody
    } else {
      throw new Exception(s"Failed to create dashboard with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to create a new question card in Metabase
   *
   * @param requestData JSON string representing the question card data
   * @return JSON string representing the created question card
   */
  def createQuestionCard(requestData: String): String = {
    val url = s"$metabaseUrl/card"

    val response = requests.post(
      url,
      data = requestData,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val questionCardResponseBody = ujson.read(response.text).render()
      questionCardResponseBody
    } else {
      throw new Exception(s"Failed to create question card with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to add a question card to a dashboard in Metabase
   *
   * @param dashboardId ID of the dashboard to add the question card to
   * @param requestData JSON string representing the question card data
   * @return JSON string representing the updated dashboard with the added question card
   */
  def addQuestionCardToDashboard(dashboardId: Int, requestData: String): String = {
    val url = s"$metabaseUrl/dashboard/$dashboardId"

    val response = requests.put(
      url,
      data = requestData,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val questionCardToDashboardResponseBody = ujson.read(response.text).render()
      questionCardToDashboardResponseBody
    } else {
      throw new Exception(s"Failed to add card to dashboard with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to create a new group in Metabase
   *
   * @param requestData JSON string representing the group data
   * @return JSON string representing the created group
   */
  def createGroup(requestData: String): String = {
    val url = s"$metabaseUrl/permissions/group"

    val response = requests.post(
      url,
      data = requestData,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val groupResponseBody = ujson.read(response.text).render()
      groupResponseBody
    } else {
      throw new Exception(s"Failed to create group with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to get revision id from Metabase
   *
   * @return JSON string representing the revision id
   */
  def getRevisionId(): String = {
    val url = s"$metabaseUrl/collection/graph"

    val response = requests.get(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val revisionId = ujson.read(response.text).render()
      revisionId
    } else {
      throw new Exception(s"Failed to get revision id with status code: ${response.statusCode}, message: ${response.text}")
    }
  }


  /**
   * Method to add collection to group in Metabase
   *
   * @param requestData JSON string representing the collection and group data
   * @return JSON string representing the updated group with the added collection
   */
  def addCollectionToGroup(requestData: String): String = {
    val url = s"$metabaseUrl/collection/graph"

    val response = requests.put(
      url,
      data = requestData,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val addCollectionToGroupResponseBody = ujson.read(response.text).render()
      addCollectionToGroupResponseBody
    } else {
      throw new Exception(s"Failed to add collection to group with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to list users from Metabase
   * @return JSON string representing the users
   */
  def listUsers(): String = {
    val url = s"$metabaseUrl/user/?status=all"

    val response = requests.get(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val usersJson = ujson.read(response.text).render()
      usersJson
    } else {
      throw new Exception(s"Failed to retrieve users with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to create a new user in Metabase
   *
   * @param requestData JSON string representing the user data
   * @return JSON string representing the created user
   */
  def createUser(requestData: String): String = {
    val url = s"$metabaseUrl/user"

    val response = requests.post(
      url,
      data = requestData,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val userResponseBody = ujson.read(response.text).render()
      userResponseBody
    } else {
      throw new Exception(s"Failed to create user with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to delete a existing user in Metabase
   *
   * @param requestData JSON string representing the userId
   * @return Boolean value
   */
  def deleteUser(userId: Int): Boolean = {
    val url = s"$metabaseUrl/user/$userId"

    val response = requests.delete(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      println(s"User with ID $userId deleted successfully.")
      true
    } else {
      println(s"Failed to delete user $userId. Status code: ${response.statusCode}, message: ${response.text}")
      false
    }
  }

  /**
   * Method to update a user in Metabase
   *
   * @param userId      ID of the user to update
   * @param requestData JSON string representing the updated user data
   * @return JSON string representing the updated user
   */
  def updateUser(userId: Int, requestData: String): String = {
    val url = s"$metabaseUrl/user/$userId"

    val response = requests.put(
      url,
      data = requestData,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val userResponseBody = ujson.read(response.text).render()
      userResponseBody
    } else {
      throw new Exception(s"Failed to update user with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to rescan values in Metabase
   * @param tableId
   * @return JSON string representing the status of the rescan values
   */

  def rescanValues(tableId: Int): String = {
    val url = s"$metabaseUrl/table/$tableId/rescan_values"

    val response = requests.post(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val responseBody = ujson.read(response.text).render()
      responseBody
    } else {
      throw new Exception(s"Failed to rescan the table: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to discard values in Metabase
   * @param tableId
   * @return JSON string representing the status of the discard values
   */


  def discardValues(tableId: Int): String = {
    val url = s"$metabaseUrl/table/$tableId/discard_values"

    val response = requests.post(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val responseBody = ujson.read(response.text).render()
      responseBody
    } else {
      throw new Exception(s"Failed to discard filter values from the table: ${response.statusCode}, message: ${response.text}")
    }
  }


  /**
   * Method to update a user's password in Metabase
   *
   * @param userId      ID of the user to update the password for
   * @param requestData JSON string representing the new password
   * @return JSON string representing the updated user with the new password
   */
  def updatePassword(userId: Int, requestData: String): String = {
    val url = s"$metabaseUrl/user/$userId/password"

    val response = requests.put(
      url,
      data = requestData,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 204) {
      val userResponseBody = "Password Updated"
      userResponseBody
    } else {
      throw new Exception(s"Failed to update password with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to list groups from Metabase
   *
   * @return JSON string representing the groups
   */
  def listGroups(): String = {
    val url = s"$metabaseUrl/permissions/group"

    val response = requests.get(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val groupsJson = ujson.read(response.text).render()
      groupsJson
    } else {
      throw new Exception(s"Failed to retrieve groups with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to get group details by Id from Metabase
   *
   * @param groupId ID of the group to retrieve details for
   * @return JSON string representing the group details
   */
  def getGroupDetails(groupId: Int): String = {
    val url = s"$metabaseUrl/permissions/group/$groupId"

    val response = requests.get(
      url,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val groupsJson = ujson.read(response.text).render()
      groupsJson
    } else {
      throw new Exception(s"Failed to retrieve group details with status code: ${response.statusCode}, message: ${response.text}")
    }
  }


  /**
   * Method to add a user to a group in Metabase
   *
   * @param requestData JSON string representing the group membership data
   * @return JSON string representing the updated group with the added user
   */
  def addUserToGroup(requestData: String): String = {
    val url = s"$metabaseUrl/permissions/membership"

    val response = requests.post(
      url,
      data = requestData,
      headers = Map(
        "Content-Type" -> "application/json",
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 200) {
      val userResponseBody = ujson.read(response.text).render()
      userResponseBody
    } else {
      throw new Exception(s"Failed to add user to group with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to remove a user from a group in Metabase
   *
   * @param membershipId JSON string representing the membershipId
   * @return JSON string representing the updated group with the removed user
   */
  def removeFromGroup(membershipId: Int): Unit = {
    val deleteUrl = s"$metabaseUrl/permissions/membership/$membershipId"

    val response = requests.delete(
      deleteUrl,
      headers = Map(
        "X-Metabase-Session" -> getSessionToken
      )
    )

    if (response.statusCode == 204) {
      println("User successfully removed from group")
    } else {
      throw new Exception(s"Failed to remove user from group. Status: ${response.statusCode}, Message: ${response.text}")
    }
  }

  /**
   * Method to sync a new table in Metabase
   * @param dbId       ID of the database to sync the new table with
   * @param tableName  Name of the new table to sync
   * @param apiKey     API key for authentication
   * @return JSON string representing the response from the sync operation
   */

  def syncNewTable(dbId: Int, tableName: String, apiKey: String): ujson.Value = {
    val url = s"$metabaseUrl/notify/db/$dbId/new-table"
    val payload = ujson.Obj(
      "schema_name" -> "public",
      "table_name" -> tableName
    ).render()
    val headers = Map(
      "Content-Type" -> "application/json",
      "X-METABASE-APIKEY" -> apiKey
    )
    val response = requests.post(url, data = payload, headers = headers)
    if (response.statusCode == 200) {
      ujson.read(response.text)
    } else {
      throw new Exception(s"Failed to retrieve database metadata with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

  /**
   * Method to search a table in Metabase DB by table name and database ID.
   *
   * @param tableName Name of the table to search
   * @param tableDbId Metabase database ID where the table exists
   * @return Table ID as Int if found, otherwise -1
   */

  def searchTable(tableName: String, tableDbId: Int): Int = {
    def escape(value: String) = value.replace("'", "''")
    val safeName = escape(tableName)

    val query =s"""SELECT id FROM metabase_table WHERE db_id = $tableDbId AND (name = '$safeName' OR display_name = '$safeName') AND active = true LIMIT 1 """.stripMargin
    metabasePostgresUtil.fetchData(query).headOption.flatMap(_.get("id")).map(_.toString.toInt).getOrElse(-1)
  }

  /**
   * Method to validate whether a collection exists in Metabase DB based on name, report type, and optional report identifier.
   *
   * @param collectionName Name of the collection
   * @param reportFor Type/category of report (e.g., Admin, Tenant)
   * @param reportId Optional identifier (Program/State/District/Tenant/Solution) used for filtering
   * @return Tuple (Boolean, Int) where:
   *         - Boolean indicates if collection exists
   *         - Int represents collection ID if found, otherwise 0
   */

  def validateCollection(collectionName: String, reportFor: String, reportId: Option[String] = None, reportIdType: Option[String] = None): (Boolean, Int) = {
    def esc(s: String): String = s.replace("'", "''")
    val safeName      = esc(collectionName)
    val safeReportFor = s"%Collection For: ${esc(reportFor)}%"

    val baseQuery =s"""SELECT id FROM collection WHERE name = '$safeName' AND description LIKE '$safeReportFor' AND archived = false""".stripMargin
    val idFilter = reportId match {

      case Some(id) =>
        reportIdType match {
          case Some(idType) =>
            val safeType = esc(idType)
            s" AND description LIKE '%$safeType Id: $id%'"
          case None => ""
        }

      case None => ""
    }

    val finalQuery = s"$baseQuery$idFilter LIMIT 1"

    try {
      val result = metabasePostgresUtil.fetchData(finalQuery)

      result.collectFirst {
        case map: Map[_, _] =>
          val id = map.get("id").flatMap {
            case i: Int => Some(i)
            case s: String if s.nonEmpty => scala.util.Try(s.toInt).toOption
            case _ => None
          }.getOrElse(0)

          println(s"[DEBUG] Collection found: id=$id")
          (true, id)
      }.getOrElse((false, 0))

    } catch {
      case e: Exception =>
        println(s"[ERROR] validateCollection failed for '$collectionName' (reportFor='$reportFor'): ${e.getMessage}")
        e.printStackTrace()
        (false, 0)
    }
  }

  /**
   * Method to validate whether a dashboard exists in Metabase DB based on name,
   * collection, report type, and optional report identifier.
   *
   * @param dashboardName Name of the dashboard
   * @param reportFor Type/category of report (e.g., Admin, Tenant)
   * @param collectionId Collection ID under which the dashboard should exist
   * @param reportId Optional identifier (State/District) used for filtering
   * @return Tuple (Boolean, Int) where:
   *         - Boolean indicates if dashboard exists
   *         - Int represents dashboard ID if found, otherwise 0
   */

  def validateDashboard(dashboardName: String, reportFor: String, collectionId: Int, reportId: Option[String] = None, reportIdType: Option[String] = None): (Boolean, Int) = {
    def esc(s: String): String = s.replace("'", "''")
    val safeName      = esc(dashboardName)
    val safeReportFor = s"%Dashboard For: ${esc(reportFor)}%"
    val baseQuery = s"""SELECT id FROM report_dashboard WHERE name = '$safeName' AND collection_id = $collectionId AND description LIKE '$safeReportFor' AND archived = false """.stripMargin

    val idFilter = reportId match {
      case Some(id) =>
        reportIdType match {
          case Some(idType) =>
            val safeType = esc(idType)
            s" AND description LIKE '%$safeType Id: $id%'"
          case None => ""
        }
      case None => ""
    }

    val finalQuery = s"$baseQuery$idFilter LIMIT 1"

    try {
      val result = metabasePostgresUtil.fetchData(finalQuery)

      result.collectFirst {
        case map: Map[_, _] =>
          val id = map.get("id").flatMap {
            case i: Int => Some(i)
            case s: String if s.nonEmpty => scala.util.Try(s.toInt).toOption
            case _ => None
          }.getOrElse(0)

          (true, id)
      }.getOrElse((false, 0))

    } catch {
      case e: Exception =>
        println(s"[ERROR] validateDashboard failed for '$dashboardName': ${e.getMessage}")
        (false, 0)
    }
  }

  /**
   * Method to fetch the table ID from Metabase DB using database ID and table name.
   * If the table is not found, it triggers a sync to create the table and retrieves its ID.
   * Also caches the result to avoid repeated lookups.
   *
   * @param databaseId Metabase database ID
   * @param tableName Name of the table
   * @param metabaseApiKey API key used for syncing new table if not found
   * @return Table ID as Int
   */

  def getTheTableId(databaseId: Int, tableName: String, metabaseApiKey: String): Int = {
    def escape(v: String): String = v.replace("'", "''")
    storedTableIds.get((databaseId, tableName)) match {
      case Some(tableId) =>
        tableId

      case None =>
        val safeTableName = escape(tableName)
        val tableQuery =s"""SELECT id FROM metabase_table WHERE db_id = $databaseId AND name = '$safeTableName' AND active = true LIMIT 1""".stripMargin
        val tableIdOpt = metabasePostgresUtil.fetchData(tableQuery) match {
          case map :: _ =>
            map.get("id").flatMap(id => scala.util.Try(id.toString.toInt).toOption)
          case _ => None
        }

        val tableId = tableIdOpt.getOrElse {
          val tableJson = syncNewTable(databaseId, tableName, metabaseApiKey)
          tableJson("id").num.toInt
        }

        storedTableIds.put((databaseId, tableName), tableId)
        println(s"tableId = $tableId")
        tableId
    }
  }

  /**
   * Method to fetch the column ID from Metabase DB using database ID, table name, and column name.
   * @param databaseId Metabase database ID
   * @param tableName Name of the table containing the column
   * @param columnName Name of the column to fetch
   * @param metabaseApiKey API key used for resolving table (via sync if required)
   * @param metaTableQuery Query template used to log error messages into meta table
   * @return Column ID as Int if found, otherwise -1 in case of failure
   */

  def getTheColumnId(databaseId: Int, tableName: String, columnName: String, metabaseApiKey: String, metaTableQuery: String): Int = {
    def escape(value: String): String = value.replace("'", "''")
    val tableId = getTheTableId(databaseId, tableName, metabaseApiKey)
    storedColumnIds.get((tableId, columnName)) match {
      case Some(columnId) =>
        columnId
      case None =>
        val columnQuery =s"""SELECT id FROM metabase_field WHERE table_id = $tableId AND name = '${escape(columnName)}' AND active = true LIMIT 1""".stripMargin
        val columnIdOpt = metabasePostgresUtil.fetchData(columnQuery).headOption.flatMap(_.get("id"))
          .flatMap {
            case i: Int => Some(i)
            case s: String if s.nonEmpty => scala.util.Try(s.toInt).toOption
            case _ => None
          }

        columnIdOpt match {
          case Some(columnId) =>
            storedColumnIds.put((tableId, columnName), columnId)
            columnId
          case None =>
            val errorMessage =s"Column '$columnName' not found in table '$tableName' (tableId: $tableId)"
            val escapedError = escape(errorMessage)
            val updateTableQuery = metaTableQuery.replace("'errorMessage'", s"'$escapedError'")
            postgresUtil.insertData(updateTableQuery)
            throw new NoSuchElementException(errorMessage)
        }
    }
  }

  /**
   * Method to validate whether a group exists in Metabase DB based on group name.
   * Performs a case-insensitive search and returns the group ID if found.
   *
   * @param groupName Name of the group to search
   * @return Tuple (Boolean, Int) where:
   *         - Boolean indicates if the group exists
   *         - Int represents group ID if found, otherwise 0
   */

  def getGroupByName(groupName: String): (Boolean, Int) = {
    def escape(v: String) = v.replace("'", "''")
    val safeName = escape(groupName)
    val query = s"""SELECT id FROM permissions_group WHERE LOWER(name) = LOWER('$safeName') LIMIT 1""".stripMargin

    try {
      val result = metabasePostgresUtil.fetchData(query)

      result.collectFirst {
        case map: Map[_, _] =>
          val id = map.get("id").map(_.toString.toInt).getOrElse(0)
          (true, id)
      }.getOrElse((false, 0))

    } catch {
      case e: Exception =>
        println(s"[ERROR] getGroupByName failed: ${e.getMessage}")
        (false, 0)
    }
  }
}
