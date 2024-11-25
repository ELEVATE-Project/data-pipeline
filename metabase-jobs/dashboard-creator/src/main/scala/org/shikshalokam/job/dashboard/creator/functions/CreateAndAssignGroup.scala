package org.shikshalokam.job.dashboard.creator.functions
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.util.JSONUtil.mapper

object CreateAndAssignGroup {
  def createGroupToDashboard(metabaseUtil: MetabaseUtil = null,GroupName:String,collectionId:Int) {
    val createGroupRequestData =
      s"""
        |{
        |  "name": "$GroupName"
        |}
        |""".stripMargin

    val groupId = mapper.readTree(metabaseUtil.createGroup(createGroupRequestData)).get("id").asInt()
    println("GroupId = " + groupId)


    val revisionData = metabaseUtil.getRevisionId()
//    println(revisionData)

    val revisionId = mapper.readTree(revisionData).get("revision").asInt()
//    println("RevisionId = " + revisionId)

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

//    println(addCollectionToUserRequestBody)
    val addCollectionToUser = metabaseUtil.addCollectionToGroup(addCollectionToUserRequestBody)
    println(addCollectionToUser)
  }
}
