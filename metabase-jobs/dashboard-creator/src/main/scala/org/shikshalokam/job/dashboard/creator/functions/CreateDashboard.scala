package org.shikshalokam.job.dashboard.creator.functions

import org.json4s.DefaultFormats
import org.shikshalokam.job.dashboard.creator.task.MetabaseDashboardConfig
import org.shikshalokam.job.util.MetabaseUtil
import play.api.libs.json.{JsValue, Json}

import scala.collection.immutable.Seq

object CreateDashboard {
    def Get_the_required_ids(CollectionName: String, DashboardName: String, reportType: String, metabaseUtil: MetabaseUtil = null, config: MetabaseDashboardConfig): (Int,Int,Int) = {
      println(s"--------------Started processing Create dashboard function---------------")
      val ReportType:String = reportType
      val collection_name: String = CollectionName
      val dashboard_name:String = DashboardName
      //Get Collection Id
      val collectionRequestBody =
        s"""{
           |  "name": "$collection_name",
           |  "description": "Collection for $ReportType reports"
           |}""".stripMargin
      val collection = metabaseUtil.createCollection(collectionRequestBody)
      val collectionJson: JsValue = Json.parse(collection)
      val collectionId: Int = (collectionJson \ "id").asOpt[Int].getOrElse {
        throw new Exception("Failed to extract collection id")
      }
      println("CollectionId = " + collectionId)

      //get the dashboard id
      val dashboardRequestBody =
        s"""{
           |  "name": "$dashboard_name",
           |  "collection_id": "$collectionId"
           |}""".stripMargin
      val Dashboard: String = metabaseUtil.createDashboard(dashboardRequestBody)
      val parsedJson: ujson.Value = ujson.read(Dashboard)

      implicit val formats: DefaultFormats.type = DefaultFormats
      val dashboardId: Int = parsedJson.obj.get("id") match {
        case Some(id: ujson.Num) => id.num.toInt
        case _ =>
          println("Error: Could not extract dashboard ID from response.")
          -1
      }
      println(s"dashboardId = $dashboardId")

      //get database id
      val listDatabaseDetails = metabaseUtil.listDatabaseDetails()


      // function to fetch database id from the output of listDatabaseDetails API
      def getDatabaseId(databasesResponse: String, databaseName: String): Option[Int] = {
        val json = Json.parse(databasesResponse)

        // Extract the ID of the database with the given name
        (json \ "data").as[Seq[JsValue]].find { db =>
          (db \ "name").asOpt[String].contains(databaseName)
        }.flatMap { db =>
          (db \ "id").asOpt[Int]
        }
      }

      val databaseName: String = config.metabaseDatabase
      val databaseId: Int = getDatabaseId(listDatabaseDetails, databaseName) match {
        case Some(id) => id
        case None => throw new RuntimeException(s"Database $databaseName not found")
      }
      println("databaseId = " + databaseId)
      println(s"--------------Processed Create dashboard function---------------")
      (collectionId,databaseId,dashboardId)
    }

}
