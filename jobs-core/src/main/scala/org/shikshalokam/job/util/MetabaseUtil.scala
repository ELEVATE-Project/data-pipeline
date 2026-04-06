package org.shikshalokam.job.util

class MetabaseUtil(url: String, metabaseUsername: String, metabasePassword: String) {

  private val metabaseUrl: String = url
  private val username: String = metabaseUsername
  private val password: String = metabasePassword
  //  println("Metabase URL: " + url)
  //  println("Username: " + username)
  //  println("Password: " + password)

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
   * Method to search a table in Metabase
    * @param tableName
   * @param tableDbId
   * @return JSON string representing the status of the search table
   */

  def searchTable(tableName: String, tableDbId: Int): String = {
    val url = s"$metabaseUrl/search"
    val params = Map(
      "q" -> tableName,
      "models" -> "table",
      "table_db_id" -> tableDbId.toString
    )
    val headers = Map(
      "X-Metabase-Session" -> getSessionToken
    )
    val response = requests.get(url, params = params, headers = headers)
    if (response.statusCode == 200) response.text
    else throw new Exception(s"Search failed: ${response.statusCode}, ${response.text}")
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
}
