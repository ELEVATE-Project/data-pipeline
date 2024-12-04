package org.shikshalokam.job.dashboard.creator.functions

import org.shikshalokam.job.util.MetabaseUtil
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import scala.io.Source
import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object UpdateStateJsonFiles {
  def ProcessAndUpdateJsonFiles(mainDir: String, collectionId: Int, databaseId: Int, dashboardId : Int , statenameId: Int, districtnameId: Int, programnameId: Int, metabaseUtil: MetabaseUtil,statename:String): ListBuffer[Int] = {
    println(s"---------------started processing ProcessAndUpdateJsonFiles function----------------")
    val questionCardId = ListBuffer[Int]()
    val objectMapper = new ObjectMapper()
    def processJsonFiles(mainDir: String,dashboardId:Int): Unit = {
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

                      val updatedJson = updateQuery(requestBody, statename)
                      updatedJson match {
                        case Some(updated) =>
                          val jsonString = mapper.writeValueAsString(updated)
                          try {
                            val response = metabaseUtil.createQuestionCard(jsonString)
                            val cardIdOpt = extractCardId(response)
                            println(s"Card ID Option: $cardIdOpt")

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

                                AddQuestionCards.appendDashCardToDashboard(metabaseUtil,updatedJsonOpt, dashboardId)
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

//    def appendDashCardToDashboard(jsonFile:Option[JsonNode], dashboardId: Int): Unit = {
//
//      val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
//
//      val dashboardJson = objectMapper.readTree(dashboardResponse)
//      val existingDashcards = dashboardJson.path("dashcards") match {
//        case array: ArrayNode => array
//        case _ => objectMapper.createArrayNode()
//      }
//      val dashCardsNode = readJsonFile(jsonFile)
//      dashCardsNode.foreach { value =>
//        existingDashcards.add(value)
//      }
//      val finalDashboardJson = objectMapper.createObjectNode()
//      finalDashboardJson.set("dashcards", existingDashcards)
//      val dashcardsString = objectMapper.writeValueAsString(finalDashboardJson)
//      val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, dashcardsString)
//      println(s"********************* Successfully updated Dashcards *********************")
//    }
//
//    def readJsonFile(jsonContent: Option[JsonNode]): Option[JsonNode] = {
//      jsonContent.flatMap { content =>
//        Try {
//          val dashCardsNode = content.path("dashCards")
//
//          if (!dashCardsNode.isMissingNode) {
//            println(s"Successfully extracted 'dashCards' key: $dashCardsNode")
//            Some(dashCardsNode)
//          } else {
//            println(s"'dashCards' key not found in JSON content.")
//            None
//          }
//        } match {
//          case Success(value) => value // Return the result if successful
//          case Failure(exception) =>
//            println(s"Error processing JSON content: ${exception.getMessage}")
//            None // Handle exceptions gracefully
//        }
//      }
//    }

    def updateQuery(json: JsonNode, stateName: String): Option[JsonNode] = {
      Try {
        // Update the query
        val updatedQueryJson = Option(json.at("/dataset_query/native/query"))
          .filter(_.isTextual)
          .map { queryNode =>
            val updatedQuery = queryNode.asText().replace("[[AND {{state_param}}]]", s"AND statename = '$stateName'")
            val datasetQuery = json.get("dataset_query").deepCopy().asInstanceOf[ObjectNode]
            val nativeNode = datasetQuery.get("native").deepCopy().asInstanceOf[ObjectNode]
            nativeNode.set("query", TextNode.valueOf(updatedQuery))
            datasetQuery.set("native", nativeNode)
            val updatedJson = json.deepCopy().asInstanceOf[ObjectNode]
            updatedJson.set("dataset_query", datasetQuery)
            updatedJson
          }.getOrElse(json)

        // Remove "state_param" from template-tags
//        val prunedTemplateTagsJson = Option(updatedQueryJson.at("/dataset_query/native/template-tags/state_param"))
//          .map { _ =>
//            val datasetQuery = updatedQueryJson.get("dataset_query").deepCopy().asInstanceOf[ObjectNode]
//            val nativeNode = datasetQuery.get("native").deepCopy().asInstanceOf[ObjectNode]
//            nativeNode.remove("template-tags")
//            datasetQuery.set("native", nativeNode)
//            val updatedJson = updatedQueryJson.deepCopy().asInstanceOf[ObjectNode]
//            updatedJson.set("dataset_query", datasetQuery)
//            updatedJson
//          }.getOrElse(updatedQueryJson)

        // Remove "state_param" from parameters
//        val updatedParametersJson = Option(updatedQueryJson.at("/parameters"))
//          .filter(_.isArray)
//          .map { parametersNode =>
//            val filteredParams = parametersNode.elements().asScala.collect {
//              case objNode: ObjectNode if Option(objNode.get("id")).exists(_.asText() != "state_param") => objNode
//            }
//            val newParamsArray = mapper.createArrayNode()
//            filteredParams.foreach(newParamsArray.add)
//            val updatedJson = updatedQueryJson.deepCopy().asInstanceOf[ObjectNode]
//            updatedJson.set("parameters", newParamsArray)
//            updatedJson
//          }.getOrElse(updatedQueryJson)

        // Remove parameter mappings
//        val updatedMappingsJson = Option(updatedQueryJson.at("/dashCards/parameter_mappings"))
//          .filter(_.isArray)
//          .map { mappingsNode =>
//            val filteredMappings = mappingsNode.elements().asScala.collect {
//              case objNode: ObjectNode if Option(objNode.get("parameter_id")).exists(_.asText() != "a7e82951") => objNode
//            }
//            val newMappingsArray = mapper.createArrayNode()
//            filteredMappings.foreach(newMappingsArray.add)
//            val dashCards = updatedQueryJson.get("dashCards").deepCopy().asInstanceOf[ObjectNode]
//            dashCards.set("parameter_mappings", newMappingsArray)
//            val updatedJson = updatedQueryJson.deepCopy().asInstanceOf[ObjectNode]
//            updatedJson.set("dashCards", dashCards)
//            updatedJson
//          }.getOrElse(updatedQueryJson)

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

                  // Update "collection_id"
                  if (rootNode.has("questionCard")) {
                    val questionCard = rootNode.get("questionCard").asInstanceOf[ObjectNode]
                    questionCard.put("collection_id", collectionId)

                    // Update "dataset_query"
                    if (questionCard.has("dataset_query")) {
                      val datasetQuery = questionCard.get("dataset_query").asInstanceOf[ObjectNode]
                      datasetQuery.put("database", databaseId)

                      // Update "native" -> "template-tags"
                      if (datasetQuery.has("native")) {
                        val nativeNode = datasetQuery.get("native").asInstanceOf[ObjectNode]
                        if (nativeNode.has("template-tags")) {
                          val templateTags = nativeNode.get("template-tags").asInstanceOf[ObjectNode]

                          // Update "state_param"
                          if (templateTags.has("state_param")) {
                            updateDimension(templateTags.get("state_param").asInstanceOf[ObjectNode], statenameId)
                          }

                          // Update "district_param"
                          if (templateTags.has("district_param")) {
                            updateDimension(templateTags.get("district_param").asInstanceOf[ObjectNode], districtnameId)
                          }

                          // Update "program_param"
                          if (templateTags.has("program_param")) {
                            updateDimension(templateTags.get("program_param").asInstanceOf[ObjectNode], programnameId)
                          }
                        }
                      }
                    }
                  }

                  // Update "dashCards" -> "id"

                  // Write updated JSON back to the file
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
    processJsonFiles(mainDir,dashboardId)
    println(s"---------------processed ProcessAndUpdateJsonFiles function----------------")
    questionCardId
  }
}
