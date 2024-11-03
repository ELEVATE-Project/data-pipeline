package org.shikshalokam.job.util

class MetabaseUtil(url: String, metabaseUsername: String, metabasePassword: String) {

  private val metabaseUrl: String = url
  private val username: String = metabaseUsername
  private val password: String = metabasePassword
  println("Metabase URL: " + url)
  println("Username: " + username)
  println("Password: " + password)

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
      //      println(s"Session Token: $token")
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
        println(s"SessionToken already exists: $token")
        token
      case None =>
        val token = authenticate()
        println(s"Generated new token: $token")
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
      //println(s"Collections JSON: $collectionsJson")
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
      //println(s"Dashboards JSON: $dashboardsJson")
      dashboardsJson
    } else {
      throw new Exception(s"Failed to retrieve dashboards with status code: ${response.statusCode}, message: ${response.text}")
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
      //println(s"Database Details JSON: $databasesJson")
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
      //println(s"database MetaData Json: $databaseMetaDataJson")
      databaseMetaDataJson
    } else {
      throw new Exception(s"Failed to retrieve database metadata with status code: ${response.statusCode}, message: ${response.text}")
    }
  }

}
