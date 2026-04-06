package org.shikshalokam.job.mentoring.dashboard.creator.functions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.MetabaseUtil

import scala.collection.JavaConverters._

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
        val body = mapper.createObjectNode()
          .put("name", collectionName)
          .put("description", description)
        parentId.foreach(pid => body.put("parent_id", pid))
        val collectionRequestBody = body.toString
        val collectionId = mapper.readTree(metabaseUtil.createCollection(collectionRequestBody)).path("id").asInt()
        println(s"$collectionName : collection created with ID = $collectionId")
        collectionId
    }
  }

  def createCollection(collectionName: String, description: String, metabaseUtil: MetabaseUtil, parentId: Option[Int] = None): Int = {
    val body = mapper.createObjectNode()
      .put("name", collectionName)
      .put("description", description)
    parentId.foreach(pid => body.put("parent_id", pid))
    val collectionRequestBody = body.toString
    val collectionId = mapper.readTree(metabaseUtil.createCollection(collectionRequestBody)).path("id").asInt()
    println(s"$collectionName : collection created with ID = $collectionId")
    collectionId

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


  def createDashboard(collectionId: Int, dashboardName: String, dashboardDescription: String, metabaseUtil: MetabaseUtil): Int = {
    val body = mapper.createObjectNode()
      .put("name", dashboardName)
      .put("description", dashboardDescription)
      .put("collection_id", collectionId)
      .put("collection_position", 1)
    val dashboardId = mapper.readTree(metabaseUtil.createDashboard(body.toString)).path("id").asInt()
    println(s"$dashboardName : dashboard created with ID = $dashboardId")
    dashboardId
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

  def createGroupForCollection(metabaseUtil: MetabaseUtil = null, groupName: String, collectionId: Int): Unit = {

    val existingGroups = mapper.readTree(metabaseUtil.listGroups())
    val existingGroup = existingGroups.elements().asScala.find { node => node.get("name").asText().equalsIgnoreCase(groupName) }
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
             |         "$collectionId": "read"
             |        }
             |    }
             |}
             |""".stripMargin
        metabaseUtil.addCollectionToGroup(addCollectionToUserRequestBody)
    }
  }

  val objectMapper = new ObjectMapper()

  def appendDashCardToDashboard(metabaseUtil: MetabaseUtil, dashcardsArray: ArrayNode, dashboardId: Int): Unit = {

    val dashboardResponse = objectMapper.readTree(
      metabaseUtil.getDashboardDetailsById(dashboardId)
    )

    val existingDashcards = dashboardResponse.path("dashcards") match {
      case array: ArrayNode => array
      case _ => objectMapper.createArrayNode()
    }

    val maxExistingId = existingDashcards.elements().asScala
      .flatMap(node => Option(node.path("id")).filter(_.isInt).map(_.asInt()))
      .foldLeft(0)(Math.max)

    dashcardsArray.elements().asScala.zipWithIndex.foreach { case (node, idx) =>
      node match {
        case obj: ObjectNode => obj.put("id", maxExistingId + idx + 1)
        case _ => // skip non-object nodes
      }
      existingDashcards.add(node)
    }

    dashboardResponse.asInstanceOf[ObjectNode]
      .set("dashcards", existingDashcards)

    val updatedDashboardStr = objectMapper.writeValueAsString(dashboardResponse)
    metabaseUtil.addQuestionCardToDashboard(dashboardId, updatedDashboardStr)
  }

}