package org.shikshalokam.job.dashboard.creator.functions

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.shikshalokam.job.dashboard.creator.task.MetabaseDashboardConfig
import org.shikshalokam.job.util.MetabaseUtil
import play.api.libs.json._

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.collection.immutable._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.sys.process._
import scala.util.{Failure, Success, Try}

object UpdateJsonFiles {
  def ProcessAndUpdateJsonFiles(mainDir: String, dashboardId: BigInt, collectionId: BigInt, databaseId: BigInt, statenameId: BigInt, districtnameId: BigInt, programnameId: BigInt, metabaseUtil: MetabaseUtil): ListBuffer[Int] = {
    println(s"---------------started processing ProcessAndUpdateJsonFiles function----------------")
    val questionCardId = ListBuffer[Int]()

    def parseJson(fileName: String): Option[JsValue] = {
      Try {
        val source = Source.fromFile(fileName)
        try {
          Json.parse(source.getLines().mkString)
        } finally {
          source.close()
        }
      }.toOption
    }

    def validateJson(fileName: String): Boolean = {
      val validateCommand = s"jq empty $fileName"
      validateCommand.! == 0
    }

    def writeToFile(fileName: String, content: String): Unit = {
      val writer = new PrintWriter(new File(fileName))
      try {
        writer.write(content)
      } finally {
        writer.close()
      }
    }

    def processJsonFiles(mainDir: String): Unit = {
      val mainDirectory = new File(mainDir)
      if (mainDirectory.exists() && mainDirectory.isDirectory) {
        val dirs = mainDirectory.listFiles().filter(_.isDirectory)

        // Loop through each directory inside the main directory
        dirs.foreach { dir =>
          println(s"Processing directory: ${dir.getName}")

          // Look for "json" sub-directory
          val jsonDir = new File(dir, "json")
          if (jsonDir.exists() && jsonDir.isDirectory) {
            // Get all directories inside the "json" directory, excluding those named "heading"
            val subDirs = jsonDir.listFiles().filter(dir => dir.isDirectory && dir.getName != "heading")
            // Loop through each subdirectory
            subDirs.foreach { subDir =>
              println(s"  Processing subdirectory: ${subDir.getName}")

              // Loop through each JSON file inside the subdirectory
              val jsonFiles = subDir.listFiles().filter(_.getName.endsWith(".json"))
              jsonFiles.foreach { jsonFile =>
                println(s"    Reading JSON file: ${jsonFile.getName}")
                val jsonFileName = jsonFile.getAbsolutePath
                println(s"jsonFileName = $jsonFileName")
                // Load and parse JSON
                val jsonOpt = parseJson(jsonFileName)
                jsonOpt match {
                  case Some(json) =>
                    val chartName = (json \ "questionCard" \ "name").asOpt[String].getOrElse("Unknown Chart")
                    println(s"  --- Started Processing For The Chart: $chartName")

                    // Check JSON validation
                    if (validateJson(jsonFileName)) {

                      // Extract request body for question card creation
                      val requestBody = (json \ "questionCard").get

                      // Make API call to create question card
                      val response = metabaseUtil.createQuestionCard(requestBody.toString())

                      // Extract card_id from the response
                      val cardIdOpt = Try(Json.parse(response)).toOption.flatMap(json => (json \ "id").asOpt[Int])
                      println(s"cardIdOpt = $cardIdOpt")
                      cardIdOpt match {
                        case Some(cardId) =>
                          println(s"   >> Successfully created question card with card_id: $cardId for $chartName")
                          val updatedJsonOpt = jsonOpt.map { json =>
                            json.transform(
                              (__ \ "dashCards" \ "card_id").json.update {
                                case _ => JsSuccess(JsNumber(cardId))
                              }
                            ).getOrElse(json)
                          }
                          questionCardId.append(cardId)
                          updatedJsonOpt match {
                            case Some(updatedJson) =>
                              writeToFile(jsonFileName, Json.prettyPrint(updatedJson))
                            case None =>
                              println("Failed to update JSON: jsonOpt is None.")
                          }
                          println(s"--------Successfully updated the json file---------")

                        case None =>
                          println(s"Error: Failed to extract card_id from the API response for $chartName.")
                      }

                    } else {
                      println(s"Warning: File '$jsonFileName' is not valid JSON. Skipping...")
                    }

                  case None =>
                    println(s"Warning: File '$jsonFileName' could not be parsed as JSON. Skipping...")
                }
              }
            }
          }
        }
      }
    }

    def updateJsonFiles(mainDir: String): Unit = {
      val mainDirectory = new File(mainDir)
      var idCount: Int = 0
      if (mainDirectory.exists() && mainDirectory.isDirectory) {
        val dirs = mainDirectory.listFiles().filter(_.isDirectory)

        // Loop through each directory inside the main directory
        dirs.foreach { dir =>
          println(s"Processing directory: ${dir.getName}")

          // Look for "json" sub-directory
          val jsonDir = new File(dir, "json")
          if (jsonDir.exists() && jsonDir.isDirectory) {
            // Get all directories inside the "json" directory
            val subDirs = jsonDir.listFiles().filter(_.isDirectory)

            // Loop through each subdirectory
            subDirs.foreach { subDir =>
              println(s"  Processing subdirectory: ${subDir.getName}")

              // Loop through each JSON file inside the subdirectory
              val jsonFiles = subDir.listFiles().filter(_.getName.endsWith(".json"))
              jsonFiles.foreach { jsonFile =>
                println(s"    Reading JSON file: ${jsonFile.getName}")
                Try {
                  val source = Source.fromFile(jsonFile)
                  val jsonStr = try source.mkString finally source.close()
                  val json = parse(jsonStr)

                  // Update JSON fields
                  val updatedJson = json.transformField {
                    case JField("dashCards", JObject(fields)) =>
                      val updatedFields = fields.map {
                        case JField("id", _) =>
                          idCount += 1
                          JField("id", JInt(BigInt(idCount)))
                        case other => other
                      }
                      ("dashCards", JObject(updatedFields))
                    case JField("collection_id", _) =>
                      ("collection_id", JInt(BigInt(collectionId.toString)))
                    case JField("dashboard_id", _) =>
                      ("dashboard_id", JInt(BigInt(dashboardId.toString)))
                    case JField("database", _) =>
                      ("database", JInt(BigInt(databaseId.toString)))
                    case JField("state_param", JObject(List(JField("dimension", JArray(dimensions))))) =>
                      ("state_param", JObject(List(JField("dimension", JArray(JInt(BigInt(statenameId.toString)) :: dimensions.tail)))))
                    case JField("district_param", JObject(List(JField("dimension", JArray(dimensions))))) =>
                      ("district_param", JObject(List(JField("dimension", JArray(JInt(BigInt(districtnameId.toString)) :: dimensions.tail)))))
                    case JField("program_param", JObject(List(JField("dimension", JArray(dimensions))))) =>
                      ("program_param", JObject(List(JField("dimension", JArray(JInt(BigInt(programnameId.toString)) :: dimensions.tail)))))
                  }

                  // Convert updated JSON back to a string and write it to the file
                  val updatedJsonStr = pretty(render(updatedJson))
                  Files.write(Paths.get(jsonFile.getPath), updatedJsonStr.getBytes)
                } match {
                  case Success(_) =>
                    println(s"    Updated ${jsonFile.getName} successfully.")
                  case Failure(e) =>
                    println(s"    Warning: File '${jsonFile.getName}' is not valid JSON or could not be updated. Error: ${e.getMessage}")
                }
              }
            }
          } else {
            println(s"  Warning: Directory 'json' not found in ${dir.getName}. Skipping...")
          }
        }
      } else {
        println(s"Error: Main directory '$mainDir' does not exist or is not a directory.")
      }
    }

    updateJsonFiles(mainDir)
    processJsonFiles(mainDir)
    println(s"---------------processed ProcessAndUpdateJsonFiles function----------------")
    questionCardId
  }
}
