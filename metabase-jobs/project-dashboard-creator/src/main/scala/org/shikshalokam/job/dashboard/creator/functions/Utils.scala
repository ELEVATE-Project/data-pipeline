package org.shikshalokam.job.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.ObjectNode
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import scala.collection.JavaConverters._

object Utils {

  def checkAndCreateCollection(collectionName: String, description: String, metabaseUtil: MetabaseUtil, parentId: Option[Int] = None): Int = {
    val collectionListJson = mapper.readTree(metabaseUtil.listCollections())
    val existingCollectionId = collectionListJson.elements().asScala
      .find(_.path("name").asText() == collectionName)
      .map(_.path("id").asInt())
    existingCollectionId match {
      case Some(id) =>
        println(s"$collectionName : collection already exists with ID: $id.")
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

  def checkAndCreateDashboard(collectionId: Int, dashboardName: String, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil): Int = {
    val dashboardListJson = mapper.readTree(metabaseUtil.listDashboards())
    val existingDashboardId = dashboardListJson.elements().asScala
      .find(_.path("name").asText() == dashboardName)
      .map(_.path("id").asInt())
    existingDashboardId match {
      case Some(id) =>
        println(s"$dashboardName : already exists with ID: $id.")
        -1

      case None =>
        val dashboardRequestBody =
          s"""{
             |  "name": "$dashboardName",
             |  "collection_id": "$collectionId",
             |  "collection_position": "1"
             |}""".stripMargin
        val dashboardId = mapper.readTree(metabaseUtil.createDashboard(dashboardRequestBody)).path("id").asInt()
        println(s"$dashboardName : dashboard created with ID = $dashboardId")
        dashboardId
    }
  }

  def createDashboard(collectionId: Int, dashboardName: String, dashboardDescription: String, metabaseUtil: MetabaseUtil, pinDashboard: String): Int = {
    val dashboardRequestBody =
      if (pinDashboard == "Yes") {
        s"""{
           |  "name": "$dashboardName",
           |  "description": "$dashboardDescription",
           |  "collection_id": "$collectionId",
           |  "collection_position": "1"
           |}""".stripMargin
      } else {
        s"""{
           |  "name": "$dashboardName",
           |  "description": "$dashboardDescription",
           |  "collection_id": "$collectionId"
           |}""".stripMargin
      }
//    println(s"Creating dashboard with request body: $dashboardRequestBody")
    val dashboardId = mapper.readTree(metabaseUtil.createDashboard(dashboardRequestBody)).path("id").asInt()
    println(s"$dashboardName : dashboard created with ID = $dashboardId")
    dashboardId
  }

  def createMicroImprovementsDashboardAndTabs(collectionId: Int, dashboardName: String, dashboardDescription: String, metabaseUtil: MetabaseUtil): (Int, Int, Int, Int) = {
    val dashboardRequestBody =
      s"""{
         |  "name": "$dashboardName",
         |  "description": "$dashboardDescription",
         |  "collection_id": "$collectionId",
         |  "collection_position": "1"
         |}""".stripMargin
    val createDashboardResponse = mapper.readTree(metabaseUtil.createDashboard(dashboardRequestBody))
    val dashboardId = createDashboardResponse.path("id").asInt()
    val tabsJson = mapper.createArrayNode()
    val tabNames = Seq("Project Details", "User Details", "Submission CSV")
    tabNames.zipWithIndex.foreach { case (name, idx) =>
      val tabNode = mapper.createObjectNode()
      tabNode.put("id", idx + 1)
      tabNode.put("dashboard_id", dashboardId)
      tabNode.put("name", name)
      tabNode.put("position", idx)
      tabsJson.add(tabNode)
    }
    val updatedDashboardResponse = createDashboardResponse.deepCopy().asInstanceOf[ObjectNode]
    updatedDashboardResponse.set("tabs", tabsJson)
    val updateDashboardRequestBody = updatedDashboardResponse.toString
    val updateDashboardResponse = mapper.readTree(metabaseUtil.addQuestionCardToDashboard(dashboardId, updateDashboardRequestBody))
    val tabIds = updateDashboardResponse.path("tabs").elements().asScala
      .map(tab => tab.path("name").asText() -> tab.path("id").asInt())
      .toMap

    val projectTabId = tabIds.getOrElse("Project Details", -1)
    val userTabId = tabIds.getOrElse("User Details", -1)
    val csvTabId = tabIds.getOrElse("Submission CSV", -1)

    println(s"$dashboardName : Dashboard created with ID = $dashboardId, Project tab with ID = $projectTabId, User tab with ID = $userTabId, Submission tab with ID = $csvTabId" )
    (dashboardId, projectTabId, userTabId, csvTabId)
  }

  def createSolutionDashboardAndTabs(collectionId: Int, dashboardName: String, dashboardDescription: String, metabaseUtil: MetabaseUtil): (Int, Int, Int, Int, Int) = {
    val dashboardRequestBody =
      s"""{
         |  "name": "$dashboardName",
         |  "description": "$dashboardDescription",
         |  "collection_id": "$collectionId",
         |  "collection_position": "1"
         |}""".stripMargin
    val createDashboardResponse = mapper.readTree(metabaseUtil.createDashboard(dashboardRequestBody))
    val dashboardId = createDashboardResponse.path("id").asInt()
    val tabsJson = mapper.createArrayNode()
    val tabNames = Seq("Project Details", "Submission CSV", "Task Report CSV", "Status Report CSV")
    tabNames.zipWithIndex.foreach { case (name, idx) =>
      val tabNode = mapper.createObjectNode()
      tabNode.put("id", idx + 1)
      tabNode.put("dashboard_id", dashboardId)
      tabNode.put("name", name)
      tabNode.put("position", idx)
      tabsJson.add(tabNode)
    }
    val updatedDashboardResponse = createDashboardResponse.deepCopy().asInstanceOf[ObjectNode]
    updatedDashboardResponse.set("tabs", tabsJson)
    val updateDashboardRequestBody = updatedDashboardResponse.toString
    val updateDashboardResponse = mapper.readTree(metabaseUtil.addQuestionCardToDashboard(dashboardId, updateDashboardRequestBody))
    val tabIds = updateDashboardResponse.path("tabs").elements().asScala
      .map(tab => tab.path("name").asText() -> tab.path("id").asInt())
      .toMap

    val projectTabId = tabIds.getOrElse("Project Details", -1)
    val submissionCsvTabId = tabIds.getOrElse("Submission CSV", -1)
    val taskReportCsvTabId = tabIds.getOrElse("Task Report CSV", -1)
    val statusReportCsvTabId = tabIds.getOrElse("Status Report CSV", -1)

    println(s"$dashboardName : Dashboard created with ID = $dashboardId, Project tab with ID = $projectTabId, Submission tab with ID = $submissionCsvTabId, Task Report CSV = $taskReportCsvTabId, Status Report CSV = $statusReportCsvTabId," )
    (dashboardId, projectTabId, submissionCsvTabId, taskReportCsvTabId, statusReportCsvTabId)
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
             |            "$collectionId": "read"
             |        }
             |    }
             |}
             |""".stripMargin
        metabaseUtil.addCollectionToGroup(addCollectionToUserRequestBody)
    }
  }

}
