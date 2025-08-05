package org.shikshalokam.job.observation.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import scala.collection.JavaConverters._
import scala.concurrent.blocking

object Utils {

  def checkAndCreateCollection(collectionName: String, description: String, metabaseUtil: MetabaseUtil, parentId: Option[Int] = None): Int = {
    val collectionListJson = mapper.readTree(metabaseUtil.listCollections())
    val existingCollectionId = collectionListJson.elements().asScala
      .find(_.path("name").asText() == collectionName)
      .map(_.path("id").asInt())
    existingCollectionId match {
      case Some(id) =>
        println(s"$collectionName : already exists with ID: $id.")
        -1

      case None =>
        val parentIdField = parentId.map(pid => s""""parent_id": $pid,""").getOrElse("")
        val collectionRequestBody =
          s"""{
             |  $parentIdField
             |  "name": "$collectionName",
             |  "description": "$description"
             |}""".stripMargin
        val collectionId = mapper.readTree(metabaseUtil.createCollection(collectionRequestBody)).path("id").asInt()
        println(s"$collectionName : collection created with ID = $collectionId")
        collectionId
    }
  }

  def createCollection(collectionName: String, description: String, metabaseUtil: MetabaseUtil, parentId: Option[Int] = None): Int = {
    val parentIdField = parentId.map(pid => s""""parent_id": $pid,""").getOrElse("")
    val collectionRequestBody =
      s"""{
         |  $parentIdField
         |  "name": "$collectionName",
         |  "description": "$description"
         |}""".stripMargin
    val collectionId = mapper.readTree(metabaseUtil.createCollection(collectionRequestBody)).path("id").asInt()
    println(s"$collectionName : collection created with ID = $collectionId")
    collectionId

  }

  def createDashboard(collectionId: Int, dashboardName: String, dashboardDescription: String, metabaseUtil: MetabaseUtil): Int = {
    val dashboardRequestBody =
      s"""{
         |  "name": "$dashboardName",
         |  "description": "$dashboardDescription",
         |  "collection_id": "$collectionId",
         |  "collection_position": "1"
         |}""".stripMargin
    val dashboardId = mapper.readTree(metabaseUtil.createDashboard(dashboardRequestBody)).path("id").asInt()
    println(s"$dashboardName : dashboard created with ID = $dashboardId")
    dashboardId
  }

  def createTabs(dashboardId: Int, tabNames: List[String], metabaseUtil: MetabaseUtil): Map[String, Int] = {
    val objectMapper = new ObjectMapper()
    val dashboardResponse: String = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardResponseJson = objectMapper.readTree(dashboardResponse)

    val tabsJson =
      if (dashboardResponseJson.has("tabs") && dashboardResponseJson.get("tabs").isArray) {
        dashboardResponseJson.get("tabs").asInstanceOf[com.fasterxml.jackson.databind.node.ArrayNode]
      } else {
        objectMapper.createArrayNode()
      }

    // Add new tabs based on tabNames and their index
    tabNames.zipWithIndex.foreach { case (tabName, index) =>
      val tabNode = objectMapper.createObjectNode()
      tabNode.put("id", index + 1)
      tabNode.put("dashboard_id", dashboardId)
      tabNode.put("name", tabName)
      tabNode.put("position", index)
      tabsJson.add(tabNode)
    }

    val copiedDashboardResponse = dashboardResponseJson.deepCopy().asInstanceOf[ObjectNode]
    copiedDashboardResponse.set("tabs", tabsJson)

    val updateDashboardRequestBody = copiedDashboardResponse.toString
    val updatedDashboardResponse = objectMapper.readTree(
      metabaseUtil.addQuestionCardToDashboard(dashboardId, updateDashboardRequestBody)
    )

    val tabIds = updatedDashboardResponse.path("tabs").elements().asScala
      .map(tab => tab.path("name").asText() -> tab.path("id").asInt())
      .toMap

    // Logging and return
    tabNames.foreach { tabName =>
      println(s"$tabName : tab created with ID = ${tabIds.getOrElse(tabName, -1)}")
    }

    tabIds
  }


  def getDatabaseId(metabaseDatabase: String, metabaseUtil: MetabaseUtil): Int = {
    val databaseListJson = mapper.readTree(metabaseUtil.listDatabaseDetails())
    val databaseId = databaseListJson.path("data").elements().asScala
      .find(_.path("name").asText() == metabaseDatabase)
      .map(_.path("id").asInt())
      .getOrElse {
        println(s"Database '$metabaseDatabase' not found. Process stopped.")
        -1
      }
    println(s"Database ID = $databaseId")
    databaseId
  }

  def getTableMetadataId(databaseId: Int, metabaseUtil: MetabaseUtil, tableName: String, columnName: String, postgresUtil: PostgresUtil, metaTableQuery: String): Int = {
    val metadataJson = mapper.readTree(metabaseUtil.getDatabaseMetadata(databaseId))
    metadataJson.path("tables").elements().asScala
      .find(_.path("name").asText() == s"$tableName")
      .flatMap(table => table.path("fields").elements().asScala
        .find(_.path("name").asText() == s"$columnName"))
      .map(field => {
        val fieldId = field.path("id").asInt()
        println(s"Field ID for $columnName: $fieldId")
        fieldId
      }).getOrElse {
        val errorMessage = s"$columnName field not found"
        val updateTableQuery = metaTableQuery.replace("'errorMessage'", s"'${errorMessage.replace("'", "''")}'")
        postgresUtil.insertData(updateTableQuery)
        throw new Exception(s"$columnName field not found")
      }
  }

  def createGroupForCollection(metabaseUtil: MetabaseUtil = null, groupName: String, collectionId: Int) {

    val existingGroups = mapper.readTree(metabaseUtil.listGroups())
    val existingGroup = existingGroups.elements().asScala.find { node =>
      node.get("name").asText().equalsIgnoreCase(groupName)
    }
    existingGroup match {
      case Some(group) =>
        println(s"Group '$groupName' already exists with ID: ${group.get("id").asInt()}")
        group.get("id").asInt()
      case None =>
        val createGroupRequestData =
          s"""
             |{
             |  "name": "$groupName"
             |}
             |""".stripMargin
        val response = metabaseUtil.createGroup(createGroupRequestData)
        val id = mapper.readTree(response).get("id").asInt()
        println(s"Created new group '$groupName' with ID: $id")
        val revisionData = metabaseUtil.getRevisionId()
        val revisionId = mapper.readTree(revisionData).get("revision").asInt()
        val addCollectionToUserRequestBody =
          s"""
             |{
             |    "revision": $revisionId,
             |    "groups": {
             |        "$id": {
             |            "$collectionId": "read"
             |        }
             |    }
             |}
             |""".stripMargin
        metabaseUtil.addCollectionToGroup(addCollectionToUserRequestBody)
    }
  }

  def getTableMetadataId(databaseId: Int, metabaseUtil: MetabaseUtil, tableName: String, columnName: String, postgresUtil: PostgresUtil, metaTableQuery: String): Int = {
    var attempts = 0
    val maxRetries = 3
    val retryDelayMillis = 10000 // 10 seconds

    while (attempts < maxRetries) {
      try {
        val metadataJson = mapper.readTree(metabaseUtil.getDatabaseMetadata(databaseId))
        return metadataJson.path("tables").elements().asScala
          .find(_.path("name").asText() == s"$tableName")
          .flatMap(table => table.path("fields").elements().asScala
            .find(_.path("name").asText() == s"$columnName"))
          .map(field => {
            val fieldId = field.path("id").asInt()
            println(s"Field ID for $columnName: $fieldId")
            fieldId
          }).getOrElse {
            throw new Exception(s"$columnName field not found")
          }
      } catch {
        case e: Exception =>
          attempts += 1
          if (attempts >= maxRetries) {
            val errorMessage = s"$columnName field not found after $maxRetries attempts"
            val updateTableQuery = metaTableQuery.replace("'errorMessage'", s"'${errorMessage.replace("'", "''")}'")
            postgresUtil.insertData(updateTableQuery)
            throw new Exception(errorMessage, e)
          } else {
            println(s"Retrying... Attempt $attempts of $maxRetries. Error: ${e.getMessage}")
            blocking(Thread.sleep(retryDelayMillis))
          }
      }
    }
    throw new Exception(s"Unexpected error while fetching $columnName field")
  }
}
