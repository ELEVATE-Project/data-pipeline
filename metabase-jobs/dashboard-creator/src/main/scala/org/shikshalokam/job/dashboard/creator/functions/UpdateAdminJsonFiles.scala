package org.shikshalokam.job.dashboard.creator.functions

import org.json4s.jackson.JsonMethods.{mapper, _}
import org.shikshalokam.job.util.MetabaseUtil
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

import scala.collection.JavaConverters._
import scala.io.Source

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}



object UpdateAdminJsonFiles {
  def ProcessAndUpdateJsonFiles(mainDir: String, dashboardId: Int, collectionId: Int, databaseId: Int, statenameId: Int, districtnameId: Int, programnameId: Int, metabaseUtil: MetabaseUtil): ListBuffer[Int] = {
    println(s"---------------started processing ProcessAndUpdateJsonFiles function----------------")
    val questionCardId = ListBuffer[Int]()

    val objectMapper = new ObjectMapper()
    val questionCardIds = scala.collection.mutable.ListBuffer[Int]()
    def processJsonFiles(mainDir: String): Unit = {
      val mainDirectory = new File(mainDir)
      if (mainDirectory.exists() && mainDirectory.isDirectory) {
        val dirs = mainDirectory.listFiles().filter(_.isDirectory)

        dirs.foreach { dir =>
          println(s"Processing directory: ${dir.getName}")

          val jsonDir = new File(dir, "json")
          if (jsonDir.exists() && jsonDir.isDirectory) {
            val subDirs = jsonDir.listFiles().filter((subDir: File) => subDir.isDirectory && subDir.getName != "heading")

            subDirs.foreach { subDir =>
              println(s"  Processing subdirectory: ${subDir.getName}")
              val jsonFiles = subDir.listFiles().filter(_.getName.endsWith(".json"))

              jsonFiles.foreach { jsonFile =>
                println(s"    Reading JSON file: ${jsonFile.getName}")
                val jsonFileName = jsonFile.getAbsolutePath
                val jsonOpt = parseJson(jsonFile)

                jsonOpt match {
                  case Some(json) =>
                    val chartName = Option(json.at("/questionCard/name").asText()).getOrElse("Unknown Chart")
                    println(s"  --- Started Processing For The Chart: $chartName")

                    if (validateJson(jsonFile)) {
                      val requestBody = json.get("questionCard").asInstanceOf[ObjectNode]
//                      val datasetQuery = requestBody.path("dataset_query").asInstanceOf[ObjectNode]
//                      //                      val nativeNode = datasetQuery.path("native").asInstanceOf[ObjectNode]
//                      val nativeNode = if (datasetQuery.has("native") && datasetQuery.get("native").isObject) {
//                        datasetQuery.get("native").asInstanceOf[ObjectNode]
//                      } else {
//                        throw new IllegalArgumentException("Missing or invalid 'native' node in dataset_query.")
//                      }
//
//                      val query = nativeNode.path("query").asText()
//                      nativeNode.put("query", query.replace("[[AND {{state_param}}]]", "AND statename = 'Karnataka'"))
//
//                      val templateTags = if (nativeNode.has("template-tags") && nativeNode.get("template-tags").isObject) {
//                        nativeNode.get("template-tags").asInstanceOf[ObjectNode]
//                      } else {
//                        throw new IllegalArgumentException("Missing or invalid 'template-tags' node in nativeNode.")
//                      }
//
//                      val parameters = requestBody.path("parameters").asInstanceOf[ArrayNode]
//                      val updatedParameters = parameters.elements().asScala
//                        .filterNot(param => param.path("id").asText() == "state_param")
//                        .toSeq
//                      requestBody.set("parameters", mapper.valueToTree(updatedParameters))

                      val response = metabaseUtil.createQuestionCard(requestBody.toString)
                      val cardIdOpt = extractCardId(response)
                      println(s"cardIdOpt = $cardIdOpt")
                      cardIdOpt match {
                        case Some(cardId) =>
                          println(s"   >> Successfully created question card with card_id: $cardId for $chartName")
                          questionCardIds.append(cardId)

                          // Update JSON with the card_id
                          val updatedJsonOpt = updateJsonWithCardId(json, cardId)
                          updatedJsonOpt match {
                            case Some(updatedJson) =>
                              writeToFile(jsonFile, updatedJson.toPrettyString)
                            case None =>
                              println("Failed to update JSON: jsonOpt is None.")
                          }
                          println(s"--------Successfully updated the json file---------")
                      }}
                  case None => println(s"Warning: File '$jsonFileName' could not be parsed as JSON. Skipping...")
                }
              }
            }
          }
        }
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
    processJsonFiles(mainDir)
    println(s"---------------processed ProcessAndUpdateJsonFiles function----------------")
    questionCardId
  }
}
