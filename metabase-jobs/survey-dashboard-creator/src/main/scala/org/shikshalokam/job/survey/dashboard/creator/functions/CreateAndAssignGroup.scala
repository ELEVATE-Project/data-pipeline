package org.shikshalokam.job.survey.dashboard.creator.functions

import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.MetabaseUtil
import scala.collection.JavaConverters._

object CreateAndAssignGroup {
  def createGroupToDashboard(metabaseUtil: MetabaseUtil = null, groupName: String, collectionId: Int) {

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

}
