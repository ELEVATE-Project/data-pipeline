package org.shikshalokam.job.observation.dashboard.creator.functions

import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.MetabaseUtil

object CreateAndAssignGroup {
  def createGroupToDashboard(metabaseUtil: MetabaseUtil = null, groupName: String, collectionId: Int) {
    val createGroupRequestData =
      s"""
         |{
         |  "name": "$groupName"
         |}
         |""".stripMargin

    val groupId = mapper.readTree(metabaseUtil.createGroup(createGroupRequestData)).get("id").asInt()
    println("GroupId = " + groupId)
    val revisionData = metabaseUtil.getRevisionId()
    val revisionId = mapper.readTree(revisionData).get("revision").asInt()
    val addCollectionToUserRequestBody =
      s"""
         |{
         |    "revision": $revisionId,
         |    "groups": {
         |        "$groupId": {
         |            "$collectionId": "read"
         |        }
         |    }
         |}
         |""".stripMargin
    metabaseUtil.addCollectionToGroup(addCollectionToUserRequestBody)
  }
}
