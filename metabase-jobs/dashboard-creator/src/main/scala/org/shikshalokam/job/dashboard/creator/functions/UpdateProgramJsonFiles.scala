package org.shikshalokam.job.dashboard.creator.functions

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.shikshalokam.job.dashboard.creator.task.MetabaseDashboardConfig
import org.shikshalokam.job.util.MetabaseUtil
import play.api.libs.json._
import java.io.File
import scala.util.Try
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.collection.immutable._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.sys.process._
import scala.util.{Failure, Success, Try}

object UpdateProgramJsonFiles {
  def ProcessAndUpdateJsonFiles(mainDir: String, dashboardId: BigInt, collectionId: BigInt, databaseId: BigInt, statenameId: BigInt, districtnameId: BigInt, programnameId: BigInt, metabaseUtil: MetabaseUtil,programname:String): ListBuffer[Int] = {
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

        dirs.foreach { dir =>
          println(s"Processing directory: ${dir.getName}")
          val jsonDir = new File(dir, "json")
          if (jsonDir.exists() && jsonDir.isDirectory) {
            val subDirs = jsonDir.listFiles().filter(dir => dir.isDirectory && dir.getName != "heading")
            subDirs.foreach { subDir =>
              println(s"  Processing subdirectory: ${subDir.getName}")
              val jsonFiles = subDir.listFiles().filter(_.getName.endsWith(".json"))
              jsonFiles.foreach { jsonFile =>
                println(s"    Reading JSON file: ${jsonFile.getName}")
                val jsonFileName = jsonFile.getAbsolutePath
                println(s"jsonFileName = $jsonFileName")

                val jsonOpt = parseJson(jsonFileName)
                jsonOpt match {
                  case Some(json) =>
                    val chartName = (json \ "questionCard" \ "name").asOpt[String].getOrElse("Unknown Chart")
                    println(s"  --- Started Processing For The Chart: $chartName")

                    if (validateJson(jsonFileName)) {
                      val requestBody = (json \ "questionCard").get

                      val updatedJson = updateJson(requestBody)

                      updatedJson match {
                        case JsSuccess(updated, _) =>
                          println("Original JSON:")
                          println(Json.prettyPrint(requestBody))

                          println("\nUpdated JSON:")
                          println(Json.prettyPrint(updated))

                          val response = metabaseUtil.createQuestionCard(updated.toString())

                          val cardIdOpt = Try(Json.parse(response)).toOption.flatMap(json => (json \ "id").asOpt[Int])
                          println(s"cardIdOpt = $cardIdOpt")

                          cardIdOpt match {
                            case Some(cardId) =>
                              println(s"   >> Successfully created question card with card_id: $cardId for $chartName")

                              val updatedJsonOpt  = updateJsonWithCardId(json,cardId)
                              println(s"updatedJsonOpt = $updatedJsonOpt")
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

                        case JsError(errors) =>
                          println(s"Error occurred while updating JSON: $errors")
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


    def updateJsonWithCardId(json: JsValue, cardId: Int): Option[JsValue] = {
      Try {
        // Convert the input JSON to JsObject
        val jsonObject = json.as[JsObject]

        // Get or create the "dashCards" node
        val dashCardsNode = (jsonObject \ "dashCards").asOpt[JsObject].getOrElse(Json.obj())

        // Update the "card_id" in the dashCardsNode
        val updatedDashCardsNode = dashCardsNode + ("card_id" -> JsNumber(cardId))

        // Update the "parameter_mappings" inside dashCards only
        val updatedParameterMappingsNode = (dashCardsNode \ "parameter_mappings").asOpt[JsArray].map { paramMappings =>
          JsArray(paramMappings.value.map {
            case JsObject(fields) =>
              JsObject(fields + ("card_id" -> JsNumber(cardId))) // Add or update the card_id
            case other => other
          })
        }.getOrElse(JsArray()) // Default to an empty array if "parameter_mappings" doesn't exist

        // Add the updated parameter_mappings back into dashCards
        val finalDashCardsNode = updatedDashCardsNode + ("parameter_mappings" -> updatedParameterMappingsNode)

        // Remove top-level "parameter_mappings" from the original JSON object (if exists)
        val updatedJsonObject = jsonObject - "parameter_mappings"

        // Add the final dashCards node back to the JSON object
        updatedJsonObject + ("dashCards" -> finalDashCardsNode)
      }.toOption
    }

    // Helper function to update JSON
    def updateJson(json: JsValue): JsResult[JsValue] = {
      // Log the structure of the JSON at the beginning
      println("Original JSON:")
      println(Json.prettyPrint(json))

      // Update "query" field
      json.transform {
        (__ \ "dataset_query" \ "native" \ "query").json.update(
          Reads[JsValue] {
            case JsString(query) =>
              JsSuccess(JsString(query.replace("[[AND {{program_param}}]]", s"AND programname = '$programname'")))
            case other =>
              JsError(s"Expected a string, but got: $other")
          }
        )
      }.flatMap { updatedJson =>
        // Prune "state_param" in "template-tags" if it exists
        updatedJson.transform((__ \ "dataset_query" \ "native" \ "template-tags" \ "program_param").json.prune) match {
          case JsSuccess(prunedJson, _) =>
            JsSuccess(prunedJson)
          case JsError(_) =>
            println("Warning: 'program_param' key does not exist, skipping prune.")
            JsSuccess(updatedJson) // Proceed with the original JSON if pruning fails
        }
      }.flatMap { updatedJson =>
        // Safely handle the "parameters" field update if it exists
        updatedJson.transform {
          (__ \ "parameters").json.update(
            Reads[JsValue] {
              case JsArray(parameters) =>
                JsSuccess(JsArray(parameters.filterNot {
                  case obj: JsObject =>
                    (obj \ "id").asOpt[String].contains("program_param")
                  case _ => false
                }))
              case other =>
                JsSuccess(other)
            }
          )
        } match {
          case JsSuccess(updatedParams, _) =>
            // Log the updated "parameters"
            println("Updated parameters:")
            println(Json.prettyPrint(updatedParams))
            JsSuccess(updatedParams)
          case JsError(_) =>
            println("Warning: 'parameters' key does not exist or could not be updated.")
            JsSuccess(updatedJson) // Proceed with the original JSON if parameters is missing
        }
      }.flatMap { updatedJson =>
        // Safely handle the "parameter_mappings" field update if it exists
        updatedJson.transform {
          (__ \ "dashCards" \ "parameter_mappings").json.update(
            Reads[JsValue] {
              case JsArray(mappings) =>
                JsSuccess(JsArray(mappings.filterNot {
                  case obj: JsObject =>
                    (obj \ "parameter_id").asOpt[String].contains("c32c8fc5")
                  case _ => false
                }))
              case other =>
                JsSuccess(other)
            }
          )
        } match {
          case JsSuccess(updatedMappings, _) =>
            // Log the updated "parameter_mappings"
            println("Updated parameter_mappings:")
            println(Json.prettyPrint(updatedMappings))
            JsSuccess(updatedMappings)
          case JsError(_) =>
            println("Warning: 'parameter_mappings' key does not exist or could not be updated.")
            JsSuccess(updatedJson) // Proceed with the original JSON if parameter_mappings is missing
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
