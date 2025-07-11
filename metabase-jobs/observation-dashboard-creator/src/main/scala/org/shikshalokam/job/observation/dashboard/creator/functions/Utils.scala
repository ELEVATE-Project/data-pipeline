package org.shikshalokam.job.observation.dashboard.creator.functions

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
}
