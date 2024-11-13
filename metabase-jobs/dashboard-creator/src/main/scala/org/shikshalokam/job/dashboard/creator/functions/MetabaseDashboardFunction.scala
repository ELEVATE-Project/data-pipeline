package org.shikshalokam.job.dashboard.creator.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.BaseProcessFunction
import org.shikshalokam.job.dashboard.creator.domain.Event
import org.shikshalokam.job.util.PostgresUtil
import org.shikshalokam.job.dashboard.creator.task.MetabaseDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory
import play.api.libs.json._
import java.io.File
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import scala.io.Source
import scala.sys.process._
import scala.util.{Try}
import java.io.{File, PrintWriter}
import scala.util.{Try, Success, Failure}
import scala.io.Source
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.collection.immutable._

class MetabaseDashboardFunction(config: MetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[MetabaseDashboardFunction])

  override def metricsList(): List[String] = {
    List(config.metabaseDashboardCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val pgHost: String = config.pgHost
    val pgPort: String = config.pgPort
    val pgUsername: String = config.pgUsername
    val pgPassword: String = config.pgPassword
    val pgDataBase: String = config.pgDataBase
    val metabaseUrl: String = config.metabaseUrl
    val metabaseUsername: String = config.metabaseUsername
    val metabasePassword: String = config.metabasePassword
    val connectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$pgDataBase"
    postgresUtil = new PostgresUtil(connectionUrl, pgUsername, pgPassword)
    metabaseUtil = new MetabaseUtil(metabaseUrl, metabaseUsername, metabasePassword)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    println(s"***************** Start of Processing the Metabase Dashboard Event with Id = ${event._id}*****************")

    println("reportType = " + event.reportType)
    println("admin = " + event.admin)
    println("targetedProgram = " + event.targetedProgram)
    println("targetedDistrict = " + event.targetedDistrict)
    println("targetedState = " + event.targetedState)
    println("publishedAt = " + event.publishedAt)
    if (event.reportType == "Project") {
      val mainDir: String = "/home/user1/Documents/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson"
      if (event.admin == List("Admin")) {
        val collectionName: String = s"Admin Collection"
        val dashboardName: String = s"Admin Report"
        val parameterFilePath:String = "/home/user1/Documents/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/admin-parameter.json"
        val (collectionId, databaseId, dashboardId) = CreateDashboard.Get_the_required_ids(CollectionName = collectionName, DashboardName = dashboardName, reportType = event.reportType,metabaseUtil,config)
        val (statenameId,districtnameId,programnameId) = GetTableData.getTableMetadata(databaseId, metabaseUtil, config)
        UpdateJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDir, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil)
        addQuestionCards.AddQuestionCardsFunction(metabaseUtil, mainDir, dashboardId)
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parameterFilePath, dashboardId)
      }else{
        println(s"Admin Dashboard Migth Be Already Created")
      }
      for (state <- event.targetedState) {
        val collectionName = s"State collection [$state]"
        val dashboardName = s"State Report [$state]"
        val stateName = s"$state"
        val parameterFilePath:String = "/home/user1/Documents/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/state-parameter.json"
        val (collectionId, databaseId, dashboardId) = CreateDashboard.Get_the_required_ids(CollectionName = collectionName, DashboardName = dashboardName, reportType = event.reportType,metabaseUtil,config)
        val (statenameId,districtnameId,programnameId) = GetTableData.getTableMetadata(databaseId, metabaseUtil, config)
        UpdateJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDir, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil)
        addQuestionCards.AddQuestionCardsFunction(metabaseUtil, mainDir, dashboardId)
        UpdateParameters.UpdateStateParameterFunction(metabaseUtil, parameterFilePath, dashboardId,stateName)
      }
      for(programname <- event.targetedProgram){
        val collectionName = s"Program collection [$programname]"
        val dashboardName = s"Program Report [$programname]"
        val programName = s"$programname"
        val parameterFilePath:String = "/home/user1/Documents/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/program-parameter.json"
        val (collectionId, databaseId, dashboardId) = CreateDashboard.Get_the_required_ids(CollectionName = collectionName, DashboardName = dashboardName, reportType = event.reportType,metabaseUtil,config)
        val (statenameId,districtnameId,programnameId) = GetTableData.getTableMetadata(databaseId, metabaseUtil, config)
        UpdateJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDir, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil)
        addQuestionCards.AddQuestionCardsFunction(metabaseUtil, mainDir, dashboardId)
        UpdateParameters.UpdateProgramParameterFunction(metabaseUtil, parameterFilePath, dashboardId,programName)
      }
//      for (districtname <- event.targetedDistrict) {
//        val collectionName = s"District collection [$districtname]"
//        val dashboardName = s"District Report [$districtname]"
//        val programName = s"$districtname"
//        val (collectionId, databaseId, dashboardId) = CreateDashboard.Get_the_required_ids(CollectionName = collectionName, DashboardName = dashboardName, reportType = event.reportType,metabaseUtil,config)
//        val (statenameId,districtnameId,programnameId) = GetTableData.getTableMetadata(databaseId, metabaseUtil, config)
//        UpdateJsonFiles.ProcessAndUpdateJsonFiles(mainDir = mainDir, dashboardId = dashboardId, collectionId = collectionId, databaseId = databaseId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, metabaseUtil)
//        addQuestionCards.AddQuestionCardsFunction(metabaseUtil, mainDir, dashboardId)
//        UpdateParameters.UpdateDistrictParameterFunction(metabaseUtil,parameterFilePath,dashboardId,statename,districtname)
//      }
    }
//    if (event.reportType == "Project") {
//      var ReportType = event.reportType
//      if (event.admin == List("Admin")) {
//        val listCollections = metabaseUtil.listCollections()
//        // Parse the JSON string
//        val json = Json.parse(listCollections)
//
//        // Check if any collection contains "name": "Super Admin Collection"
//        val exists = (json \\ "name").exists(name => name.as[String].trim == "Super Admin Collection")
//
//        if (exists) {
//          println("Super Admin Collection exists in the list.")
//        } else {
//          println("Super Admin Collection does not exist in the list.")
//        }
//
//      }
//      // let's create state report
//      for (state <- event.targetedState) {
//        // get the collection id
//        val collection_name = s"state collection [$state]"
//        val collectionRequestBody =
//          s"""{
//             |  "name": "$collection_name",
//             |  "description": "Collection for $ReportType reports"
//             |}""".stripMargin
//        val collection = metabaseUtil.createCollection(collectionRequestBody)
//        val collectionJson: JsValue = Json.parse(collection)
//        //        println("collectionJson = " + collectionJson)
//        val collectionId: Int = (collectionJson \ "id").asOpt[Int].getOrElse {
//          throw new Exception("Failed to extract collection id")
//        }
//        println("CollectionId = " + collectionId)
//
//        //get the dashboard id
//        val dashboard_name = s"state report [$state]"
//        val dashboardRequestBody =
//          s"""{
//             |  "name": "$dashboard_name",
//             |  "collection_id": "$collectionId"
//             |}""".stripMargin
//        val Dashboard: String = metabaseUtil.createDashboard(dashboardRequestBody)
//        val parsedJson: ujson.Value = ujson.read(Dashboard)
//        println("Create Dashboard Json = " + parsedJson)
//        implicit val formats: DefaultFormats.type = DefaultFormats
//        val dashboardId: Int = parsedJson.obj.get("id") match {
//          case Some(id: ujson.Num) => id.num.toInt
//          case _ =>
//            println("Error: Could not extract dashboard ID from response.")
//            -1 // Default value or handle the error as needed
//        }
//        println(s"dashboardId = $dashboardId")
//        //get database id
//        val listDatabaseDetails = metabaseUtil.listDatabaseDetails()
//        println("Database Details JSON = " + listDatabaseDetails)
//
//        // function to fetch database id from the output of listDatabaseDetails API
//        def getDatabaseId(databasesResponse: String, databaseName: String): Option[Int] = {
//          val json = Json.parse(databasesResponse)
//
//          // Extract the ID of the database with the given name
//          (json \ "data").as[Seq[JsValue]].find { db =>
//            (db \ "name").asOpt[String].contains(databaseName)
//          }.flatMap { db =>
//            (db \ "id").asOpt[Int]
//          }
//        }
//
//        val databaseName: String = config.metabaseDatabase
////        val databaseId: Int = getDatabaseId(listDatabaseDetails, databaseName).get
//        val databaseId: Int = getDatabaseId(listDatabaseDetails, databaseName) match {
//          case Some(id) => id
//          case None => throw new RuntimeException(s"Database $databaseName not found")
//        }
//        println("databaseId = " + databaseId)
//
//        // Function to extract table names and IDs
//        def extractTables(metadata: String): List[Map[String, Any]] = {
//          // Parse JSON and extract the "tables" section
//          (for {
//            JObject(table) <- parse(metadata) \ "tables"
//            JField("name", JString(name)) <- table
//            JField("id", JInt(id)) <- table
//          } yield Map("name" -> name, "id" -> id)).toList
//        }
//
//        // Function to extract table names and field IDs from nested JSON
//        def extractTablesAndFields(metadata: String): List[Map[String, Any]] = {
//          // Parse JSON and extract the "tables" section and their "fields"
//          (for {
//            JObject(table) <- parse(metadata) \ "tables"
//            JField("name", JString(tableName)) <- table
//            JArray(fields) = table.find(_._1 == "fields").map(_._2).getOrElse(JArray(Nil))
//            JObject(field) <- fields
//            JField("name", JString(fieldName)) <- field
//            JField("id", JInt(fieldId)) <- field
//          } yield Map("table" -> tableName, "field_name" -> fieldName, "field_id" -> fieldId)).toList
//        }
//
//        def getMetadataJson(databaseId: Int): String = {
//          val metadata = metabaseUtil.getDatabaseMetadata(databaseId) // Directly get the String
//          val tables = extractTables(metadata)
//          val tablesAndFields = extractTablesAndFields(metadata)
//
//          // Creating a Map with the extracted tables and fields
//          val result = Map(
//            "tables" -> tables,
//            "fields" -> tablesAndFields
//          )
//
//          // Convert to JSON string with pretty formatting
//          Serialization.writePretty(result)
//        }
//
//        val metadataJson = getMetadataJson(databaseId.toInt)
//
//
//        def getFieldId(metadata: String, tableName: String, fieldName: String): Option[BigInt] = {
//          val parsedJson = parse(metadata)
//
//          // Extract the field ID based on table and field name
//          (for {
//            JObject(field) <- parsedJson \ "fields"
//            JField("table", JString(t)) <- field if t == tableName
//            JField("field_name", JString(f)) <- field if f == fieldName
//            JField("field_id", JInt(fieldId)) <- field
//          } yield BigInt(fieldId.toString)).headOption
//        }
//
//        // Assuming getFieldId returns Option[BigInt]
//        val statenameId: Int = getFieldId(metadataJson, "projects", "statename").map(_.toInt).getOrElse(0)
//        val districtnameId: Int = getFieldId(metadataJson, "projects", "districtname").map(_.toInt).getOrElse(0)
//        val programnameId: Int = getFieldId(metadataJson, "solutions", "programname").map(_.toInt).getOrElse(0)
//
//        println("statenameID" + statenameId)
//        println("districtnameID" + districtnameId)
//        println("programnameID" + programnameId)
//
//        def parseJson(fileName: String): Option[JsValue] = {
//          Try {
//            val source = Source.fromFile(fileName)
//            try {
//              Json.parse(source.getLines().mkString)
//            } finally {
//              source.close()
//            }
//          }.toOption
//        }
//
//        def validateJson(fileName: String): Boolean = {
//          val validateCommand = s"jq empty $fileName"
//          validateCommand.! == 0
//        }
//
//        def writeToFile(fileName: String, content: String): Unit = {
//          val writer = new PrintWriter(new File(fileName))
//          try {
//            writer.write(content)
//          } finally {
//            writer.close()
//          }
//        }
//
//        def processJsonFiles(mainDir: String): Unit = {
//          val mainDirectory = new File(mainDir)
//          if (mainDirectory.exists() && mainDirectory.isDirectory) {
//            val dirs = mainDirectory.listFiles().filter(_.isDirectory)
//
//            // Loop through each directory inside the main directory
//            dirs.foreach { dir =>
//              println(s"Processing directory: ${dir.getName}")
//
//              // Look for "json" sub-directory
//              val jsonDir = new File(dir, "json")
//              if (jsonDir.exists() && jsonDir.isDirectory) {
//                // Get all directories inside the "json" directory, excluding those named "heading"
//                val subDirs = jsonDir.listFiles().filter(dir => dir.isDirectory && dir.getName != "heading")
////                // Get all directories inside the "json" directory
////                val subDirs = jsonDir.listFiles().filter(_.isDirectory)
//
//                // Loop through each subdirectory
//                subDirs.foreach { subDir =>
//                  println(s"  Processing subdirectory: ${subDir.getName}")
//
//                  // Loop through each JSON file inside the subdirectory
//                  val jsonFiles = subDir.listFiles().filter(_.getName.endsWith(".json"))
//                  jsonFiles.foreach { jsonFile =>
//                    println(s"    Reading JSON file: ${jsonFile.getName}")
//                    val jsonFileName = jsonFile.getAbsolutePath
//                    println(s"jsonFileName = $jsonFileName")
//                    // Load and parse JSON
//                    val jsonOpt = parseJson(jsonFileName)
//                    jsonOpt match {
//                      case Some(json) =>
//                        val chartName = (json \ "questionCard" \ "name").asOpt[String].getOrElse("Unknown Chart")
//                        println(s"  --- Started Processing For The Chart: $chartName")
//
//                        // Check JSON validation
//                        if (validateJson(jsonFileName)) {
//
//                          // Extract request body for question card creation
//                          val requestBody = (json \ "questionCard").get
//
//                          // Make API call to create question card
//                          val response = metabaseUtil.createQuestionCard(requestBody.toString())
//
//                          // Extract card_id from the response
//                          val cardIdOpt = Try(Json.parse(response)).toOption.flatMap(json => (json \ "id").asOpt[Int])
//                          println(s"cardIdOpt = $cardIdOpt")
//                          cardIdOpt match {
//                            case Some(cardId) =>
//                              println(s"   >> Successfully created question card with card_id: $cardId for $chartName")
//                              val updatedJsonOpt = jsonOpt.map { json =>
//                                json.transform(
//                                  (__ \ "dashCards" \ "card_id").json.update {
//                                    case _ => JsSuccess(JsNumber(cardId))
//                                  }
//                                ).getOrElse(json)
//                              }
//                              updatedJsonOpt match {
//                                case Some(updatedJson) =>
//                                  //                              println(s"Updated JSON:\n${Json.prettyPrint(updatedJson)}")
//                                  writeToFile(jsonFileName, Json.prettyPrint(updatedJson))
//                                case None =>
//                                  println("Failed to update JSON: jsonOpt is None.")
//                              }
//                              println(s"--------Successfully updated the json file---------")
//                            // Append the new dashCard to the dashboard
//                            //                        appendDashCardToDashboard(jsonFileName,dashboardId)
//
//                            case None =>
//                              println(s"Error: Failed to extract card_id from the API response for $chartName.")
//                          }
//
//                        } else {
//                          println(s"Warning: File '$jsonFileName' is not valid JSON. Skipping...")
//                        }
//
//                      case None =>
//                        println(s"Warning: File '$jsonFileName' could not be parsed as JSON. Skipping...")
//                    }
//                  }
//                }
//              }
//            }
//          }
//        }
//
//        def updateJsonFiles(mainDir: String): Unit = {
//          val mainDirectory = new File(mainDir)
//          var idCount : Int = 0
//          if (mainDirectory.exists() && mainDirectory.isDirectory) {
//            val dirs = mainDirectory.listFiles().filter(_.isDirectory)
//
//            // Loop through each directory inside the main directory
//            dirs.foreach { dir =>
//              println(s"Processing directory: ${dir.getName}")
//
//              // Look for "json" sub-directory
//              val jsonDir = new File(dir, "json")
//              if (jsonDir.exists() && jsonDir.isDirectory) {
//                // Get all directories inside the "json" directory
//                val subDirs = jsonDir.listFiles().filter(_.isDirectory)
//
//                // Loop through each subdirectory
//                subDirs.foreach { subDir =>
//                  println(s"  Processing subdirectory: ${subDir.getName}")
//
//                  // Loop through each JSON file inside the subdirectory
//                  val jsonFiles = subDir.listFiles().filter(_.getName.endsWith(".json"))
//                  jsonFiles.foreach { jsonFile =>
//                    println(s"    Reading JSON file: ${jsonFile.getName}")
//                    Try {
//                      val source = Source.fromFile(jsonFile)
//                      val jsonStr = try source.mkString finally source.close()
//                      val json = parse(jsonStr)
//
//                      // Update JSON fields
//                      val updatedJson = json.transformField {
//                        case JField("dashCards", JObject(fields)) =>
//                          val updatedFields = fields.map {
//                            case JField("id", _) =>
//                              idCount += 1
//                              JField("id", JInt(BigInt(idCount)))
//                            case other => other
//                          }
//                          ("dashCards", JObject(updatedFields))
//                        case JField("collection_id", _) =>
//                          ("collection_id", JInt(BigInt(collectionId)))
//                        case JField("dashboard_id", _) =>
//                          ("dashboard_id", JInt(BigInt(dashboardId.toString)))
//                        case JField("database", _) =>
//                          ("database", JInt(BigInt(databaseId)))
//                        case JField("state_param", JObject(List(JField("dimension", JArray(dimensions))))) =>
//                          ("state_param", JObject(List(JField("dimension", JArray(JInt(BigInt(statenameId)) :: dimensions.tail)))))
//                        case JField("district_param", JObject(List(JField("dimension", JArray(dimensions))))) =>
//                          ("district_param", JObject(List(JField("dimension", JArray(JInt(BigInt(districtnameId)) :: dimensions.tail)))))
//                        case JField("program_param", JObject(List(JField("dimension", JArray(dimensions))))) =>
//                          ("program_param", JObject(List(JField("dimension", JArray(JInt(BigInt(programnameId)) :: dimensions.tail)))))
//                      }
//
//                      // Convert updated JSON back to a string and write it to the file
//                      val updatedJsonStr = pretty(render(updatedJson))
//                      Files.write(Paths.get(jsonFile.getPath), updatedJsonStr.getBytes)
//                    } match {
//                      case Success(_) =>
//                        println(s"    Updated ${jsonFile.getName} successfully.")
//                      case Failure(e) =>
//                        println(s"    Warning: File '${jsonFile.getName}' is not valid JSON or could not be updated. Error: ${e.getMessage}")
//                    }
//                  }
//                }
//              } else {
//                println(s"  Warning: Directory 'json' not found in ${dir.getName}. Skipping...")
//              }
//            }
//          } else {
//            println(s"Error: Main directory '$mainDir' does not exist or is not a directory.")
//          }
//        }
//
//        def appendDashCardToDashboard(mainDir: String, dashboardId: Int): Unit = {
//          val mainDirectory = new File(mainDir)
//
//          // Step 7: Get the Dashboard response
//          val DashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
//
//          // Step 8: Extract the existing dashcards
//          val DashboardJson = Json.parse(DashboardResponse)
//          var existingDashcards = (DashboardJson \ "dashcards").asOpt[JsArray] match {
//            case Some(dashcards) => dashcards
//            case None => JsArray()
//          }
//          println(s"existingDashcards = $existingDashcards")
//          // Step 1: Check if the main directory exists and is a directory
//          if (mainDirectory.exists() && mainDirectory.isDirectory) {
//            // Step 2: Get all directories inside the main directory
//            val dirs = mainDirectory.listFiles().filter(_.isDirectory)
//
//            // Loop through each directory inside the main directory
//            dirs.foreach { dir =>
//              println(s"Processing directory: ${dir.getName}")
//
//              // Step 3: Look for "json" sub-directory
//              val jsonDir = new File(dir, "json")
//              if (jsonDir.exists() && jsonDir.isDirectory) {
//                // Step 4: Get all directories inside the "json" directory
//                val subDirs = jsonDir.listFiles().filter(_.isDirectory)
//
//                // Loop through each subdirectory
//                subDirs.foreach { subDir =>
//                  println(s"  Processing subdirectory: ${subDir.getName}")
//
//                  // Step 5: Loop through each JSON file inside the subdirectory
//                  val jsonFiles = subDir.listFiles().filter(_.getName.endsWith(".json"))
//                  jsonFiles.foreach { jsonFile =>
//                    println(s"Reading JSON file: ${jsonFile.getName}")
//
//                    // Step 6: Read JSON file and fetch the value of the "dashboard" key
//                    val dashboardValue = readJsonFile(jsonFile)
//                    println(s"dashboardValue : $dashboardValue")
//                    dashboardValue match {
//                      case Some(value) =>
//                        // If a valid dashboard value exists, append it to the existingDashcards (as JsObject or JsArray)
//                        val newCard = Json.obj("dashCards" -> value)
//                        val dashCards: JsValue = (newCard \ "dashCards").get
//                        println(s"dashCards = $dashCards")
//                        existingDashcards = existingDashcards :+ dashCards
//                        println(s"existingDashcards = $existingDashcards")
//                      case None =>
//                        println("dashCards key not found in the JSON.")
//                    }
//                  }
//                }
//              }
//            }
//            // Convert JsArray to String
//            val finalDashcards: JsObject = Json.obj("dashcards" -> existingDashcards)
//            println(s"finalDashcards = $finalDashcards")
//            val DashcardString: String = Json.stringify(finalDashcards)
//            val UpdateDashcards = metabaseUtil.addQuestionCardToDashboard(dashboardId,DashcardString)
//            println(s"********************* successfully updated Dashcard : $UpdateDashcards  *********************")
//          } else {
//            println(s"$mainDir is not a valid directory.")
//          }
//
//          def readJsonFile(file: File): Option[JsValue] = {
//            Try {
//              val source = Source.fromFile(file)
//              val content = try source.mkString finally source.close()
////              println(s"File content from ${file.getName}: $content")
//              Json.parse(content) \ "dashCards" // Extract the "dashboard" key
//            } match {
//              case Success(JsDefined(value)) =>
//                println(s"Successfully extracted 'dashCards' key: $value")
//                Some(value)
//              case Success(JsUndefined()) =>
//                println(s"'dashCards' key not found in file: ${file.getName}")
//                None
//              case Failure(exception) =>
//                println(s"Error reading or parsing JSON file ${file.getName}: ${exception.getMessage}")
//                None
//            }
//          }
//        }
//
//        def UpdateParameterFunction(parameterFilePath:String,dashboardId:Int,stateName:String): Unit = {
//          val fileContent = new String(Files.readAllBytes(Paths.get(parameterFilePath)), "UTF-8")
//          val parameterJson = Json.parse(fileContent).as[JsArray]
//
//          // Update parameter value dynamically
//          val updatedParameterJson = parameterJson.value.map { param =>
//            val updatedParam = param.as[JsObject]
//            if ((updatedParam \ "slug").as[String] == "select_state") {
//              updatedParam ++ Json.obj(
//                "default" -> Json.arr(stateName),
//                "values_source_config" -> Json.obj("values" -> Json.arr(stateName))
//              )
//            } else {
//              updatedParam
//            }
//          }
//
//          // Fetch existing dashboard details
//          val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
//
//          // Parse current parameters
//          val currentParametersJson = (Json.parse(dashboardResponse) \ "parameters").as[JsArray]
//
//          // Combine current parameters with the updated ones
//          val finalParametersJson = Json.toJson(currentParametersJson.value ++ updatedParameterJson)
//
//          // Update the dashboard with new parameters
//          val updatePayload = Json.obj("parameters" -> finalParametersJson)
//
//          val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId , updatePayload.toString())
//
//          println(s"----------------successfullly updated datshboard parameter $updateResponse----------------")
//        }
//        val jsonFilePath = "/home/user1/Documents/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson/state-parameter.json"
//        val PROJECT_JSON_DIR =  "/home/user1/Documents/elevate-data-pipeline/metabase-jobs/dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/utils/projectJson"
//        updateJsonFiles((PROJECT_JSON_DIR))
//        processJsonFiles(PROJECT_JSON_DIR)
//        appendDashCardToDashboard(PROJECT_JSON_DIR,dashboardId)
//        UpdateParameterFunction(jsonFilePath,dashboardId,state)
//      }
//    }
    println(s"***************** End of Processing the Metabase Dashboard Event *****************\n")
  }
}
