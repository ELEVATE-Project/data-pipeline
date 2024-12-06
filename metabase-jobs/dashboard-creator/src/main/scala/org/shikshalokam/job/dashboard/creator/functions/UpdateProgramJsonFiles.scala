package org.shikshalokam.job.dashboard.creator.functions

import org.shikshalokam.job.util.MetabaseUtil
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import scala.io.Source
import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object UpdateProgramJsonFiles {
  def ProcessAndUpdateJsonFiles(mainDir: String, collectionId: Int, databaseId: Int, dashboardId: Int, statenameId: Int, districtnameId: Int, programnameId: Int, metabaseUtil: MetabaseUtil, programname: String): ListBuffer[Int] = {
    println(s"---------------started processing ProcessAndUpdateJsonFiles function----------------")
    val questionCardId = ListBuffer[Int]()
    val objectMapper = new ObjectMapper()

    def processJsonFiles(mainDir: String, dashboardId: Int): Unit = {
      val mainDirectory = new File(mainDir)
      if (mainDirectory.exists() && mainDirectory.isDirectory) {
        val dirs = mainDirectory.listFiles().filter(_.isDirectory)

        dirs.foreach { dir =>
          println(s"Processing directory: ${dir.getName}")

          val jsonDir = new File(dir, "json")
          if (jsonDir.exists() && jsonDir.isDirectory) {
            val subDirs = jsonDir.listFiles().filter((subDir: File) => subDir.isDirectory && subDir.getName != "heading")

            subDirs.foreach { subDir =>
              println(s"Processing subdirectory: ${subDir.getName}")
              val jsonFiles = subDir.listFiles().filter(_.getName.endsWith(".json"))

              jsonFiles.foreach { jsonFile =>
                println(s"Reading JSON file: ${jsonFile.getName}")
                val jsonFileName = jsonFile.getAbsolutePath
                val jsonOpt = parseJson(jsonFile)

                jsonOpt match {
                  case Some(json) =>
                    val chartName = Option(json.at("/questionCard/name").asText()).getOrElse("Unknown Chart")
                    println(s" >>>>>>>>>>> Started Processing For The Chart: $chartName")

                    if (validateJson(jsonFile)) {
                      val mapper = new ObjectMapper()

                      val requestBody = json.get("questionCard").asInstanceOf[ObjectNode]

                      val updatedJson = updateQuery(requestBody, programname)
                      updatedJson match {
                        case Some(updated) =>
                          val jsonString = mapper.writeValueAsString(updated)
                          try {
                            val response = metabaseUtil.createQuestionCard(jsonString)
                            val cardIdOpt = extractCardId(response)
                            cardIdOpt match {
                              case Some(cardId) =>
                                println(s"Successfully created question card with card_id: $cardId for $chartName")
                                questionCardId.append(cardId)

                                // Update JSON with the card_id
                                val updatedJsonOpt = updateJsonWithCardId(json, cardId)
                                println(s"Updated JSON with Card ID: $updatedJsonOpt")

                                updatedJsonOpt match {
                                  case Some(finalUpdatedJson) =>
                                    writeToFile(jsonFile, finalUpdatedJson.toPrettyString)
                                  case None =>
                                    println("Failed to update JSON: updatedJsonOpt is None.")
                                }

                                AddQuestionCards.appendDashCardToDashboard(metabaseUtil, updatedJsonOpt, dashboardId)
                                println("Successfully updated the JSON file.")
                              case None =>
                                println("Failed to extract card ID from response.")
                            }
                          } catch {
                            case e: Exception =>
                              println(s"Error occurred while creating question card: ${e.getMessage}")
                          }
                        case None =>
                          println("Failed to update JSON: updateQuery returned None.")
                      }
                    }
                  case None => println(s"Warning: File '$jsonFileName' could not be parsed as JSON. Skipping...")
                }
              }
            }
          }
        }
      }
    }

    def updateQuery(json: JsonNode, programname: String): Option[JsonNode] = {
      Try {
        // Update the query
        val updatedQueryJson = Option(json.at("/dataset_query/native/query"))
          .filter(_.isTextual)
          .map { queryNode =>
            val updatedQuery = queryNode.asText().replace("[[AND {{program_param}}]]", s"AND programname = '$programname'")
            val datasetQuery = json.get("dataset_query").deepCopy().asInstanceOf[ObjectNode]
            val nativeNode = datasetQuery.get("native").deepCopy().asInstanceOf[ObjectNode]
            nativeNode.set("query", TextNode.valueOf(updatedQuery))
            datasetQuery.set("native", nativeNode)
            val updatedJson = json.deepCopy().asInstanceOf[ObjectNode]
            updatedJson.set("dataset_query", datasetQuery)
            updatedJson
          }.getOrElse(json)
        updatedQueryJson
      } match {
        case Success(updatedJson) => Some(updatedJson)
        case Failure(exception) =>
          println(s"Error updating JSON: ${exception.getMessage}")
          None
      }
    }


    def parseJson(file: File): Option[JsonNode] = {
      Try(objectMapper.readTree(file)) match {
        case Success(jsonNode) => Some(jsonNode)
        case Failure(exception) =>
          println(s"Error parsing JSON: ${exception.getMessage}")
          None
      }
    }

    def validateJson(file: File): Boolean = {
      Try(objectMapper.readTree(file)).isSuccess
    }

    def extractCardId(response: String): Option[Int] = {
      Try {
        val jsonResponse = objectMapper.readTree(response)
        jsonResponse.get("id").asInt()
      }.toOption
    }

    def updateJsonWithCardId(json: JsonNode, cardId: Int): Option[JsonNode] = {
      Try {
        val jsonObject = json.asInstanceOf[ObjectNode]

        val dashCardsNode = if (jsonObject.has("dashCards") && jsonObject.get("dashCards").isObject) {
          jsonObject.get("dashCards").asInstanceOf[ObjectNode]
        } else {
          val newDashCardsNode = JsonNodeFactory.instance.objectNode()
          jsonObject.set("dashCards", newDashCardsNode)
          newDashCardsNode
        }

        dashCardsNode.put("card_id", cardId)

        if (dashCardsNode.has("parameter_mappings") && dashCardsNode.get("parameter_mappings").isArray) {
          dashCardsNode.get("parameter_mappings").elements().forEachRemaining { paramMappingNode =>
            if (paramMappingNode.isObject) {
              paramMappingNode.asInstanceOf[ObjectNode].put("card_id", cardId)
            }
          }
        }

        jsonObject
      }.toOption
    }


    def writeToFile(file: File, content: String): Unit = {
      Try {
        val writer = new java.io.PrintWriter(file)
        try {
          writer.write(content)
        } finally {
          writer.close()
        }
      } match {
        case Success(_) => println(s"File '${file.getAbsolutePath}' updated successfully.")
        case Failure(exception) => println(s"Error writing to file: ${exception.getMessage}")
      }
    }

    def updateJsonFiles(mainDir: String, collectionId: Int, statenameId: Int, districtnameId: Int, programnameId: Int, databaseId: Int): Unit = {
      val mapper = new ObjectMapper()
      val mainDirectory = new File(mainDir)

      if (mainDirectory.exists() && mainDirectory.isDirectory) {
        val dirs = mainDirectory.listFiles().filter(_.isDirectory)

        dirs.foreach { dir =>
          println(s"Processing directory: ${dir.getName}")

          val jsonDir = new File(dir, "json")
          if (jsonDir.exists() && jsonDir.isDirectory) {
            val subDirs = jsonDir.listFiles().filter(_.isDirectory)

            subDirs.foreach { subDir =>
              println(s"  Processing subdirectory: ${subDir.getName}")

              val jsonFiles = subDir.listFiles().filter(_.getName.endsWith(".json"))
              jsonFiles.foreach { jsonFile =>
                println(s"    Reading JSON file: ${jsonFile.getName}")
                try {
                  val jsonStr = Source.fromFile(jsonFile).mkString
                  val rootNode = mapper.readTree(jsonStr).asInstanceOf[ObjectNode]
                  if (rootNode.has("questionCard")) {
                    val questionCard = rootNode.get("questionCard").asInstanceOf[ObjectNode]
                    questionCard.put("collection_id", collectionId)
                    if (questionCard.has("dataset_query")) {
                      val datasetQuery = questionCard.get("dataset_query").asInstanceOf[ObjectNode]
                      datasetQuery.put("database", databaseId)
                      if (datasetQuery.has("native")) {
                        val nativeNode = datasetQuery.get("native").asInstanceOf[ObjectNode]
                        if (nativeNode.has("template-tags")) {
                          val templateTags = nativeNode.get("template-tags").asInstanceOf[ObjectNode]
                          if (templateTags.has("state_param")) {
                            updateDimension(templateTags.get("state_param").asInstanceOf[ObjectNode], statenameId)
                          }
                          if (templateTags.has("district_param")) {
                            updateDimension(templateTags.get("district_param").asInstanceOf[ObjectNode], districtnameId)
                          }
                          if (templateTags.has("program_param")) {
                            updateDimension(templateTags.get("program_param").asInstanceOf[ObjectNode], programnameId)
                          }
                        }
                      }
                    }
                  }

                  val writer = new PrintWriter(jsonFile)
                  try {
                    writer.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode))
                    println(s"    Updated ${jsonFile.getName} successfully.")
                  } finally {
                    writer.close()
                  }
                } catch {
                  case e: Exception =>
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


    def updateDimension(node: ObjectNode, newId: Int): Unit = {
      if (node.has("dimension") && node.get("dimension").isArray) {
        val dimensionNode = node.get("dimension").asInstanceOf[ArrayNode]
        if (dimensionNode.size() >= 2) {
          dimensionNode.set(1, dimensionNode.numberNode(newId))
        } else {
          println(s"Warning: 'dimension' array does not have enough elements to update.")
        }
      } else {
        println(s"Warning: 'dimension' node is missing or not an array.")
      }
    }

    updateJsonFiles(mainDir, collectionId = collectionId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, databaseId = databaseId)
    processJsonFiles(mainDir, dashboardId)
    println(s"---------------processed ProcessAndUpdateJsonFiles function----------------")
    questionCardId
  }
}
