package org.shikshalokam.job.survey.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.postgresql.util.PGobject
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object UpdateCsvDownloadJsonFiles {
  def ProcessAndUpdateJsonFiles(reportConfigQuery: String, collectionId: Int, databaseId: Int, dashboardId: Int, tabId: Int, stateNameId: Int, districtNameId: Int, blockNameId: Int, clusterNameId: Int, schoolNameId: Int, orgNameId: Int, replacements: Map[String, String], metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil): ListBuffer[Int] = {
    println(s"---------------started processing ProcessAndUpdateJsonFiles function----------------")
    val questionCardId = ListBuffer[Int]()
    val objectMapper = new ObjectMapper()

    def processCsvJsonFiles(reportConfigQuery: String, collectionId: Int, databaseId: Int, dashboardId: Int, tabId: Int, stateNameId: Int, districtNameId: Int, blockNameId: Int, clusterNameId: Int, schoolNameId: Int, orgNameId: Int, replacements: Map[String, String]): Unit = {
      val dashcardsArray = objectMapper.createArrayNode()
      val queryResult = postgresUtil.fetchData(reportConfigQuery)
      queryResult.foreach { row =>
        if (row.get("question_type").map(_.toString).getOrElse("") != "heading") {
          row.get("config") match {
            case Some(queryValue: PGobject) =>
              val configJson = objectMapper.readTree(queryValue.getValue)
              if (configJson != null) {
                val originalQuestionCard = configJson.path("questionCard")
                val chartName = Option(originalQuestionCard.path("name").asText()).getOrElse("Unknown Chart")
                val updatedQuestionCard = updateQuestionCardJsonValues(configJson, collectionId, stateNameId, districtNameId, blockNameId, clusterNameId, schoolNameId, orgNameId, databaseId)
                val finalQuestionCard = updatePostgresDatabaseQuery(updatedQuestionCard, replacements)
                val requestBody = finalQuestionCard.asInstanceOf[ObjectNode]
                val cardId = mapper.readTree(metabaseUtil.createQuestionCard(requestBody.toString)).path("id").asInt()
                println(s">>>>>>>>> Successfully created question card with card_id: $cardId for $chartName")
                questionCardId.append(cardId)
                val updatedQuestionIdInDashCard = updateQuestionIdInDashCard(configJson, cardId, dashboardId, tabId)
                updatedQuestionIdInDashCard.foreach { node =>
                  val dashCardsNode = node.path("dashCards")
                  if (!dashCardsNode.isMissingNode && !dashCardsNode.isNull) {
                    dashcardsArray.add(dashCardsNode)
                  } else {
                    println("No 'dashCards' key found in the JSON.")
                  }
                }
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
                optJsonNode.foreach { node =>
                  val dashCardsNode = node.path("dashCards")
                  if (!dashCardsNode.isMissingNode && !dashCardsNode.isNull) {
                    dashcardsArray.add(dashCardsNode)
                  } else {
                    println("No 'dashCards' key found in the JSON.")
                  }
                }
              }
          }
        }
      }
      Utils.appendDashCardToDashboard(metabaseUtil, dashcardsArray, dashboardId)
    }

    def toOption(jsonNode: JsonNode): Option[JsonNode] = {
      if (jsonNode == null || jsonNode.isMissingNode) None else Some(jsonNode)
    }

    def updateQuestionIdInDashCard(json: JsonNode, cardId: Int, dashboardId: Int, tabId: Int): Option[JsonNode] = {
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
        dashCardsNode.put("dashboard_id", dashboardId)
        dashCardsNode.put("dashboard_tab_id", tabId)
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

    def updateQuestionCardJsonValues(configJson: JsonNode, collectionId: Int, stateNameId: Int, districtNameId: Int, blockNameId: Int, clusterNameId: Int, schoolNameId: Int, orgNameId: Int, databaseId: Int): JsonNode = {
      try {
        val configObjectNode = configJson.deepCopy().asInstanceOf[ObjectNode]
        Option(configObjectNode.get("questionCard")).foreach { questionCard =>
          questionCard.asInstanceOf[ObjectNode].put("collection_id", collectionId)

          Option(questionCard.get("dataset_query")).foreach { datasetQuery =>
            datasetQuery.asInstanceOf[ObjectNode].put("database", databaseId)

            Option(datasetQuery.get("native")).foreach { nativeNode =>
              Option(nativeNode.get("template-tags")).foreach { templateTags =>
                val params = Map(
                  "state_param" -> stateNameId,
                  "district_param" -> districtNameId,
                  "block_param" -> blockNameId,
                  "school_param" -> schoolNameId,
                  "cluster_param" -> clusterNameId,
                  "org_param" -> orgNameId
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

    def updateDimension(node: JsonNode, newId: Int): Unit = {
      if (node.has("dimension") && node.get("dimension").isArray) {
        val dimensionNode = node.get("dimension").asInstanceOf[ArrayNode]
        if (dimensionNode.size() >= 2) {
          dimensionNode.set(1, JsonNodeFactory.instance.numberNode(newId))
        } else {
          println(s"Warning: 'dimension' array does not have enough elements to update.")
        }
      } else {
        println(s"Warning: 'dimension' node is missing or not an array.")
      }
    }

    def updatePostgresDatabaseQuery(json: JsonNode, replacements: Map[String, String]): JsonNode = {
      Try {
        val queryNode = json.at("/dataset_query/native/query")
        if (queryNode.isMissingNode || !queryNode.isTextual) {
          throw new IllegalArgumentException("Query node is missing or not a valid string.")
        }

        var updatedQuery = queryNode.asText()
        replacements.foreach { case (placeholder, value) =>
          updatedQuery = updatedQuery.replace(placeholder, value)
        }

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

    processCsvJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, tabId, stateNameId, districtNameId, blockNameId, clusterNameId, schoolNameId, orgNameId, replacements)
    println(s"---------------processed ProcessAndUpdateJsonFiles function----------------")
    questionCardId
  }
}
