package org.shikshalokam.job.dashboard.creator.miDashboard


import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.postgresql.util.PGobject
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object ComparePage {

  def ProcessAndUpdateJsonFiles(reportConfigQuery: String, collectionId: Int, databaseId: Int, dashboardId: Int, statenameId: Int, districtnameId: Int, projects: String, solutions: String, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil): ListBuffer[Int] = {
    println(s"---------------Started processing Mi dashboard Compare page function for Admin----------------")
    val questionCardId = ListBuffer[Int]()
    val objectMapper = new ObjectMapper()

    def processJsonFiles(reportConfigQuery: String, collectionId: Int, databaseId: Int, dashboardId: Int, statenameId: Int, districtnameId: Int): Unit = {
      val queryResult = postgresUtil.fetchData(reportConfigQuery)
      queryResult.foreach { row =>
        if (row.get("question_type").map(_.toString).getOrElse("") != "heading") {
          row.get("config") match {
            case Some(queryValue: PGobject) =>
              val configJson = objectMapper.readTree(queryValue.getValue)
              if (configJson != null) {
                val originalQuestionCard = configJson.path("questionCard")
                val chartName = Option(originalQuestionCard.path("name").asText()).getOrElse("Unknown Chart")
                println(s" >>>>>>>>>> Processing The Chart: $chartName")
                val updatedQuestionCard = updateQuestionCardJsonValues(configJson, collectionId, statenameId, districtnameId, databaseId)
                val finalQuestionCard = updatePostgresDatabaseQuery(updatedQuestionCard, projects, solutions)
                val requestBody = finalQuestionCard.asInstanceOf[ObjectNode]
                val cardId = mapper.readTree(metabaseUtil.createQuestionCard(requestBody.toString)).path("id").asInt()
                println(s"~~ Successfully created question card with card_id: $cardId for $chartName")
                questionCardId.append(cardId)
                val updatedQuestionIdInDashCard = updateQuestionIdInDashCard(configJson, cardId)
                appendDashCardToDashboard(metabaseUtil, updatedQuestionIdInDashCard, dashboardId)
              }
            case None =>
              println("Key 'config' not found in the result row.")
          }
        }
        else {
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

    def updateQuestionIdInDashCard(json: JsonNode, cardId: Int): Option[JsonNode] = {
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

    def updateQuestionCardJsonValues(configJson: JsonNode, collectionId: Int, statenameId: Int, districtnameId: Int, databaseId: Int): JsonNode = {
      try {
        val configObjectNode = configJson.deepCopy().asInstanceOf[ObjectNode]
        Option(configObjectNode.get("questionCard")).foreach { questionCard =>
          questionCard.asInstanceOf[ObjectNode].put("collection_id", collectionId)

          Option(questionCard.get("dataset_query")).foreach { datasetQuery =>
            datasetQuery.asInstanceOf[ObjectNode].put("database", databaseId)

            Option(datasetQuery.get("native")).foreach { nativeNode =>
              Option(nativeNode.get("template-tags")).foreach { templateTags =>
                val params = Map(
                  "state_name" -> statenameId,
                  "district_name" -> districtnameId
                )
                params.foreach { case (paramName, paramId) =>
                  Option(templateTags.get(paramName)).foreach { paramNode =>
                    updateDimension(paramNode.asInstanceOf[ObjectNode], paramId)
                  }
                }
              }
            }
          }
        }
        configObjectNode.get("questionCard")
      } catch {
        case e: Exception =>
          println(s"Warning: JSON node could not be updated. Error: ${e.getMessage}")
          configJson
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

    def updatePostgresDatabaseQuery(json: JsonNode, projectsTable: String, solutionsTable: String): JsonNode = {
      Try {
        val queryNode = json.at("/dataset_query/native/query")
        if (queryNode.isMissingNode || !queryNode.isTextual) {
          throw new IllegalArgumentException("Query node is missing or not a valid string.")
        }

        val updatedQuery = queryNode.asText()
          .replace("${config.projects}", projectsTable)
          .replace("${config.solutions}", solutionsTable)

        val updatedJson = json.deepCopy().asInstanceOf[ObjectNode]
        updatedJson.at("/dataset_query/native")
          .asInstanceOf[ObjectNode]
          .set("query", TextNode.valueOf(updatedQuery))

        updatedJson
      } match {
        case Success(updatedQueryJson) => updatedQueryJson
        case Failure(exception) =>
          throw new IllegalArgumentException("Failed to update query in JSON", exception)
      }
    }

    processJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, statenameId, districtnameId)
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

  def UpdateAdminParameterFunction(metabaseUtil: MetabaseUtil, parametersQuery: String, dashboardId: Int, postgresUtil: PostgresUtil): Unit = {
    val objectMapper = new ObjectMapper()
    val parameterData: List[Any] = postgresUtil.fetchData(parametersQuery).flatMap(_.get("config"))
    val parameterJsonString: String = parameterData.headOption match {
      case Some(value: String) => value
      case Some(value) => value.toString
      case None => throw new Exception("No parameter data found")
    }

    val parameterJson: ArrayNode = objectMapper.readTree(parameterJsonString) match {
      case array: ArrayNode => array
      case _ => throw new Exception("Expected parameter data to be an ArrayNode")
    }

    val updatedParameterJson: List[ObjectNode] = parameterJson.elements().asScala.map {
      case param: ObjectNode => param
      case _ => throw new Exception("Expected all parameters to be ObjectNodes")
    }.toList

    val dashboardResponse: String = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson: ArrayNode = dashboardJson.path("parameters") match {
      case array: ArrayNode => array
      case _ => objectMapper.createArrayNode()
    }

    val finalParametersJson = objectMapper.createArrayNode()
    currentParametersJson.elements().asScala.foreach(finalParametersJson.add)
    updatedParameterJson.foreach(finalParametersJson.add)

    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersJson)

    metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"---------------Successfully updated parameters for Mi dashboard Compare page---------------\n")
  }

}