package org.shikshalokam.job.dashboard.creator.miDashboard

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.postgresql.util.PGobject
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object HomePage {

  def ProcessAndUpdateJsonFiles(reportConfigQuery: String, collectionId: Int, databaseId: Int, dashboardId: Int, projects: String, solutions: String, reportConfig: String, metaDataTable: String, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil): ListBuffer[Int] = {
    println(s"---------------Started processing Mi dashboard Home page function for Admin----------------")
    val questionCardId = ListBuffer[Int]()
    val objectMapper = new ObjectMapper()

    def processJsonFiles(reportConfigQuery: String, collectionId: Int, databaseId: Int): Unit = {
      val queryResult = postgresUtil.fetchData(reportConfigQuery)

      queryResult.foreach { row =>
        if (row.get("question_type").map(_.toString).getOrElse("") != "heading") {
          row.get("config") match {
            case Some(queryValue: PGobject) =>
              val jsonString = queryValue.getValue
              val rootNode = objectMapper.readTree(jsonString)
              if (rootNode != null) {
                val questionCardNode = rootNode.path("questionCard")
                val chartName = Option(questionCardNode.path("name").asText()).getOrElse("Unknown Chart")
                println(s" >>>>>>>>>> Processing The Chart: $chartName")
                val updatedJson = updateJsonFiles(rootNode, collectionId, databaseId, reportConfig)
                val updatedJsonWithQuery = updateQuery(updatedJson.path("questionCard"), projects, solutions, metaDataTable)
                val requestBody = updatedJsonWithQuery.asInstanceOf[ObjectNode]
                val response = metabaseUtil.createQuestionCard(requestBody.toString)
                val cardIdOpt = extractCardId(response)

                cardIdOpt match {
                  case Some(cardId) =>
                    println(s"~~ Successfully created question card with card_id: $cardId for $chartName")
                    questionCardId.append(cardId)
                    val updatedJsonOpt = updateJsonWithCardId(updatedJson, cardId)
                    appendDashCardToDashboard(metabaseUtil, updatedJsonOpt, dashboardId)
                  case None =>
                    println(s"Error: Unable to extract card ID for $chartName. Skipping...")
                }
              } else {
                println("Warning: File could not be parsed as JSON. Skipping...")
              }

            case Some(_) =>
              println("Unexpected type for 'query' key value.")

            case None =>
              println("Key 'query' not found in the result row.")
          }
        } else {
          row.get("config") match {
            case Some(queryValue: PGobject) =>
              val jsonString = queryValue.getValue
              val rootNode = objectMapper.readTree(jsonString)
              if (rootNode != null) {
                val optJsonNode = toOption(rootNode)
                appendDashCardToDashboard(metabaseUtil, optJsonNode, dashboardId)
              }
          }
        }
      }
    }

    def toOption(jsonNode: JsonNode): Option[JsonNode] = {
      if (jsonNode == null || jsonNode.isMissingNode) None else Some(jsonNode)
    }

    def updateQuery(json: JsonNode, projectsTable: String, solutionsTable: String, metaDataTable: String): JsonNode = {
      Try {

        val queryPath = "/dataset_query/native/query"
        val queryNode = json.at(queryPath)
        if (queryNode.isMissingNode || !queryNode.isTextual) {
          throw new IllegalArgumentException(s"Query node at path $queryPath is missing or not textual.")
        }

        val updateTableFilter = queryNode.asText()
        val updatedTableName = updateTableFilter
          .replace("${config.projects}", projectsTable)
          .replace("${config.solutions}", solutionsTable)
          .replace("${config.dashboard_metadata}", metaDataTable)

        val datasetQuery = json.get("dataset_query").deepCopy().asInstanceOf[ObjectNode]
        val nativeNode = datasetQuery.get("native").deepCopy().asInstanceOf[ObjectNode]
        nativeNode.set("query", TextNode.valueOf(updatedTableName))
        datasetQuery.set("native", nativeNode)

        val updatedJson = json.deepCopy().asInstanceOf[ObjectNode]
        updatedJson.set("dataset_query", datasetQuery)
        updatedJson
      } match {
        case Success(updatedQueryJson) => updatedQueryJson
        case Failure(exception) =>
          throw new IllegalArgumentException("Failed to update query in JSON", exception)
      }
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

    def updateJsonFiles(jsonNode: JsonNode, collectionId: Int, databaseId: Int, reportConfig: String): JsonNode = {
      try {
        val rootNode = jsonNode.deepCopy().asInstanceOf[ObjectNode]

        if (rootNode.has("questionCard")) {
          val questionCard = rootNode.get("questionCard").asInstanceOf[ObjectNode]
          questionCard.put("collection_id", collectionId)

          if (questionCard.has("dataset_query")) {
            val datasetQuery = questionCard.get("dataset_query").asInstanceOf[ObjectNode]
            datasetQuery.put("database", databaseId)
          }

          if (questionCard.has("display") && questionCard.get("display").asText() == "map") {
            if (questionCard.has("visualization_settings")) {
              val visualizationSettings = questionCard.get("visualization_settings").asInstanceOf[ObjectNode]
              val map_unique_id_query = s"SELECT config_item->>'id' AS state_map_id FROM $reportConfig,LATERAL json_array_elements(config) AS config_item WHERE config_item->>'state_name' = 'India'  AND report_name = 'Mi-Dashboard-Location-Mapping'  AND question_type = 'state';"
              val queryResult = postgresUtil.fetchData(map_unique_id_query)
              val stateMapId = queryResult.headOption.flatMap(_.get("state_map_id")).getOrElse("NotFound").toString
              visualizationSettings.put("map.region", stateMapId)
            }
          }
        }
        rootNode
      } catch {
        case e: Exception =>
          println(s"Warning: JSON node could not be updated. Error: ${e.getMessage}")
          jsonNode
      }
    }

    processJsonFiles(reportConfigQuery, collectionId, databaseId)
    println(s"---------------Completed processing Mi dashboard Home page function for Admin----------------")
    questionCardId
  }

  val objectMapper = new ObjectMapper()

  def appendDashCardToDashboard(metabaseUtil: MetabaseUtil, jsonFile: Option[JsonNode], dashboardId: Int): Unit = {

    val dashboardResponse = objectMapper.readTree(metabaseUtil.getDashboardDetailsById(dashboardId))
    val existingDashcards = dashboardResponse.path("dashcards") match {
      case array: ArrayNode => array
      case _ => objectMapper.createArrayNode()
    }
    val dashCardsNode = readJsonFile(jsonFile)
    dashCardsNode.foreach { value =>
      existingDashcards.add(value)
    }
    val finalDashboardJson = objectMapper.createObjectNode()
    finalDashboardJson.set("dashcards", existingDashcards)
    val dashcardsString = objectMapper.writeValueAsString(finalDashboardJson)
    metabaseUtil.addQuestionCardToDashboard(dashboardId, dashcardsString)
    println(s"~~ Successfully added the card to Dashcards")
  }

  def readJsonFile(jsonContent: Option[JsonNode]): Option[JsonNode] = {
    jsonContent.flatMap { content =>
      Try {
        val dashCardsNode = content.path("dashCards")

        if (!dashCardsNode.isMissingNode) {
          Some(dashCardsNode)
        } else {
          println(s"'dashCards' key not found in JSON content.")
          None
        }
      } match {
        case Success(value) => value // Return the result if successful
        case Failure(exception) =>
          println(s"Error processing JSON content: ${exception.getMessage}")
          None
      }
    }
  }

  def updateAndAddFilter(metabaseUtil: MetabaseUtil, queryResult: JsonNode, collectionId: Int, databaseId: Int, projectTable: String, solutionTable: String): Int = {
    val objectMapper = new ObjectMapper()

    def replaceStateName(json: JsonNode, projectTable: String, solutionTable: String): JsonNode = {
      def processNode(node: JsonNode): JsonNode = {
        node match {
          case obj: ObjectNode =>
            obj.fieldNames().forEachRemaining { fieldName =>
              val childNode = obj.get(fieldName)
              if (childNode.isTextual) {
                var updatedText = childNode.asText()
                if (updatedText.contains("${config.projects}")) {
                  updatedText = updatedText.replace("${config.projects}", projectTable)
                }
                if (updatedText.contains("${config.solutions}")) {
                  updatedText = updatedText.replace("${config.solutions}", solutionTable)
                }
                obj.put(fieldName, updatedText)
              } else {
                obj.set(fieldName, processNode(childNode))
              }
            }
            obj

          case array: ArrayNode =>
            val newArray = array.deepCopy()
            newArray.removeAll()
            array.elements().forEachRemaining { child =>
              newArray.add(processNode(child))
            }
            newArray

          case _ => node
        }
      }

      val updatedJson = processNode(json.deepCopy())
      updatedJson
    }

    def updateCollectionIdAndDatabaseId(jsonFile: JsonNode, collectionId: Int, databaseId: Int): JsonNode = {
      try {
        val questionCardNode = jsonFile.get("questionCard").asInstanceOf[ObjectNode]
        if (questionCardNode == null) {
          throw new IllegalArgumentException("'questionCard' node not found.")
        }
        questionCardNode.put("collection_id", collectionId)
        val datasetQueryNode = questionCardNode.get("dataset_query").asInstanceOf[ObjectNode]
        if (datasetQueryNode == null) {
          throw new IllegalArgumentException("'dataset_query' node not found.")
        }
        datasetQueryNode.put("database", databaseId)
        jsonFile
      } catch {
        case ex: Exception =>
          throw new RuntimeException(s"Error updating JSON: ${ex.getMessage}", ex)
      }
    }

    def getTheQuestionId(json: JsonNode): Int = {
      try {
        val requestBody = json.get("questionCard")
        val questionCardResponse = metabaseUtil.createQuestionCard(requestBody.toString)
        val responseJson = objectMapper.readTree(questionCardResponse)
        Option(responseJson.get("id")).map(_.asInt()).getOrElse {
          println("Error: 'id' field not found in the response.")
          -1
        }
      } catch {
        case ex: Exception =>
          println(s"Error fetching 'id' from response: ${ex.getMessage}")
          -1
      }
    }

    val ReplacedStateNameJson = replaceStateName(queryResult, projectTable, solutionTable)
    val updatedJson = updateCollectionIdAndDatabaseId(ReplacedStateNameJson, collectionId, databaseId)
    val questionId = getTheQuestionId(updatedJson)
    questionId
  }

  def updateParameterFunction(metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, parametersQuery: String, slugNameToStateIdMap: Map[String, Int], dashboardId: Int): Unit = {
    val objectMapper = new ObjectMapper()
    val parameterData: List[Any] = postgresUtil.fetchData(parametersQuery).flatMap(_.get("config"))
    val parameterJsonString: String = parameterData.headOption match {
      case Some(value: String) if value.nonEmpty => value
      case Some(value) if value.toString.nonEmpty => value.toString
      case _ => throw new Exception("Invalid or empty parameter data found")
    }

    val parameterJson: ArrayNode = objectMapper.readTree(parameterJsonString) match {
      case array: ArrayNode => array
      case _ => throw new Exception("Expected parameter data to be an ArrayNode")
    }

    val updatedParameterJson = try {
      parameterJson.elements().asScala.flatMap { param =>
        try {
          if (param.isInstanceOf[ObjectNode]) {
            val updatedParam = param.asInstanceOf[ObjectNode]
            val slug = updatedParam.path("slug").asText()

            slugNameToStateIdMap.get(slug) match {
              case Some(newCardId) =>
                val valuesSourceConfigNode = updatedParam.path("values_source_config")
                if (valuesSourceConfigNode.isObject) {
                  val valuesSourceConfig = valuesSourceConfigNode.asInstanceOf[ObjectNode]
                  valuesSourceConfig.put("card_id", newCardId)
                  updatedParam.set("values_source_config", valuesSourceConfig)
                }
                Some(updatedParam)
              case None =>
                println(s"No card_id found for slug '$slug', skipping update")
                None
            }
          } else {
            println(s"Skipping param as it is not of type ObjectNode: ${param.toString}")
            None
          }
        } catch {
          case e: Exception =>
            println(s"Error processing param: ${param.toString}")
            println(s"Error: ${e.getMessage}")
            None
        }
      }.toList
    } catch {
      case e: Exception =>
        println(s"Error during JSON processing: ${e.getMessage}")
        e.printStackTrace()
        List.empty
    }
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson = dashboardJson.path("parameters").asInstanceOf[ArrayNode]

    val finalParametersJson = (currentParametersJson.elements().asScala.filterNot { param =>
      val slug = param.path("slug").asText()
      slugNameToStateIdMap.contains(slug)
    }.toList ++ updatedParameterJson).distinct

    val finalParametersArray = objectMapper.createArrayNode()
    finalParametersJson.foreach(finalParametersArray.add)
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersArray)

    metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"---------------Successfully updated parameters for Mi dashboard Home page---------------\n")
  }

}