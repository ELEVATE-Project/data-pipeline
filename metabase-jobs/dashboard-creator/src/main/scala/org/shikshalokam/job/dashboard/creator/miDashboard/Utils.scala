package org.shikshalokam.job.dashboard.creator.miDashboard


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
        val errorMessage = s"$collectionName : already exists with ID: $id."
        throw new IllegalStateException(s"$errorMessage. Process stopped.")

      case None =>
        val parentIdField = parentId.map(pid => s""""parent_id": "$pid",""").getOrElse("")
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

  def checkAndCreateDashboard(collectionId: Int, dashboardName: String, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil): Int = {
    val dashboardListJson = mapper.readTree(metabaseUtil.listDashboards())
    val existingDashboardId = dashboardListJson.elements().asScala
      .find(_.path("name").asText() == dashboardName)
      .map(_.path("id").asInt())
    existingDashboardId match {
      case Some(id) =>
        val errorMessage = s"$dashboardName : already exists with ID: $id."
        throw new IllegalStateException(s"$errorMessage. Process stopped.")

      case None =>
        val dashboardRequestBody =
          s"""{
             |  "name": "$dashboardName",
             |  "collection_id": "$collectionId",
             |  "collection_position": "1"
             |}""".stripMargin
        val dashboardId = mapper.readTree(metabaseUtil.createDashboard(dashboardRequestBody)).path("id").asInt()
        println(s"$dashboardName : dashboard created with ID = $collectionId")
        dashboardId
    }
  }

}