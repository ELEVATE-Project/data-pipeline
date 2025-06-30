package org.shikshalokam.job.observation.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.postgresql.util.PGobject
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}


object UpdateQuestionDomainJsonFiles {
  def ProcessAndUpdateJsonFiles(reportConfigQuery: String, collectionId: Int, databaseId: Int, dashboardId: Int, domainTable: String, QuestionTable: String, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, params: Map[String, Int], paramsToRemove: ListMap[String, String], entityColumnName: String, entityColumnId: String, entityType: String): ListBuffer[Int] = {
    println(s"---------------started processing ProcessAndUpdateJsonFiles function----------------")
    val questionCardId = ListBuffer[Int]()
    val objectMapper = new ObjectMapper()

    def processJsonFiles(reportConfigQuery: String, collectionId: Int, databaseId: Int, dashboardId: Int, domainTable: String, QuestionTable: String, params: Map[String, Int], paramsToRemove: ListMap[String, String], entityColumnName: String, entityColumnId: String, entityType: String): Unit = {
      val queryResult = postgresUtil.fetchData(reportConfigQuery)
      queryResult.foreach { row =>
        if (row.get("question_type").map(_.toString).getOrElse("") != "heading") {
          row.get("config") match {
            case Some(queryValue: PGobject) =>
              val configJson = objectMapper.readTree(queryValue.getValue)
              val cleanedJson: JsonNode = objectMapper.readTree(cleanDashboardJson(configJson.toString, paramsToRemove, true))
              if (cleanedJson != null) {
                val originalQuestionCard = cleanedJson.path("questionCard")
                val chartName = Option(originalQuestionCard.path("name").asText())
                  .map(name => name.replace("${entityType}", entityType))
                  .getOrElse("Unknown Chart")
                val nameNode = originalQuestionCard.asInstanceOf[ObjectNode].get("name")
                if (nameNode != null && nameNode.isTextual) {
                  val updatedName = nameNode.asText().replace("${entityType}", entityType)
                  originalQuestionCard.asInstanceOf[ObjectNode].put("name", updatedName)
                }
                val updatedQuestionCard = updateQuestionCardJsonValues(cleanedJson, collectionId, databaseId, params)
                val finalQuestionCard = updatePostgresDatabaseQuery(updatedQuestionCard, domainTable, QuestionTable, entityColumnName, entityColumnId, entityType)
                val requestBody = finalQuestionCard.asInstanceOf[ObjectNode]
                val cardId = mapper.readTree(metabaseUtil.createQuestionCard(requestBody.toString)).path("id").asInt()
                println(s">>>>>>>>> Successfully created question card with card_id: $cardId for $chartName")
                questionCardId.append(cardId)
                val updatedQuestionIdInDashCard = updateQuestionIdInDashCard(configJson, cardId)
                AddQuestionCards.appendDashCardToDashboard(metabaseUtil, updatedQuestionIdInDashCard, dashboardId)
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
                AddQuestionCards.appendDashCardToDashboard(metabaseUtil, optJsonNode, dashboardId)
              }
          }
        }
      }
    }

    def cleanDashboardJson(jsonStr: String, paramsToRemove: Map[String, String], removeByColumnName: Boolean = false): String = {
      val mapper = new ObjectMapper()
      val root = mapper.readTree(jsonStr).asInstanceOf[ObjectNode]

      // Remove template-tags
      val templateTags = root
        .path("questionCard")
        .path("dataset_query")
        .path("native")
        .path("template-tags")
        .asInstanceOf[ObjectNode]
      paramsToRemove.keys.foreach(templateTags.remove)

      // Remove parameters
      val parametersPath = root
        .path("questionCard")
        .path("parameters")
        .asInstanceOf[ArrayNode]
      val filteredParams = mapper.createArrayNode()
      parametersPath.elements().asScala.foreach { param =>
        if (!paramsToRemove.contains(param.path("slug").asText())) {
          filteredParams.add(param)
        }
      }
      root.path("questionCard").asInstanceOf[ObjectNode].set("parameters", filteredParams)

      // Remove parameter_mappings
      val dashCards = root.path("dashCards").asInstanceOf[ObjectNode]
      val paramMappings = dashCards.path("parameter_mappings").asInstanceOf[ArrayNode]
      val filteredMappings = mapper.createArrayNode()
      paramMappings.elements().asScala.foreach { mapping =>
        val target = mapping.path("target")
        if (
          target.isArray &&
            target.size() > 1 &&
            target.get(1).isArray &&
            target.get(1).size() > 1 &&
            !paramsToRemove.contains(target.get(1).get(1).asText())
        ) {
          filteredMappings.add(mapping)
        }
      }
      dashCards.set("parameter_mappings", filteredMappings)

      // Remove filter parameters from query string
      val questionCard = root.path("questionCard").asInstanceOf[ObjectNode]
      val datasetQuery = questionCard.path("dataset_query").asInstanceOf[ObjectNode]
      val nativeNode = datasetQuery.path("native").asInstanceOf[ObjectNode]
      val queryNode = nativeNode.path("query")
      if (queryNode != null && queryNode.isTextual) {
        var queryStr = queryNode.asText()
        val items = if (removeByColumnName) paramsToRemove.values else paramsToRemove.keys
        items.foreach { item =>
          if (removeByColumnName) {
            // Remove entire AND <column_name> = ( ... )
            val regex = ("""(?is)\s*AND\s+""" + java.util.regex.Pattern.quote(item) + """\s*=\s*\((?:[^()]*|\((?:[^()]*|\([^()]*\))*\))*\)""").r
            queryStr = regex.replaceAllIn(queryStr, "")
          } else {
            // Remove [[AND {{key}}]]
            val regex = raw"""(?i)\[\[\s*AND\s*\{\{\s*${java.util.regex.Pattern.quote(item)}\s*\}\}\s*\]\]""".r
            queryStr = regex.replaceAllIn(queryStr, "")
          }
        }
        nativeNode.put("query", queryStr)
      }

      mapper.writeValueAsString(root)
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

    def updateQuestionCardJsonValues(configJson: JsonNode, collectionId: Int, databaseId: Int, params: Map[String, Int]): JsonNode = {
      try {
        val configObjectNode = configJson.deepCopy().asInstanceOf[ObjectNode]
        Option(configObjectNode.get("questionCard")).foreach { questionCard =>
          questionCard.asInstanceOf[ObjectNode].put("collection_id", collectionId)
          Option(questionCard.get("dataset_query")).foreach { datasetQuery =>
            datasetQuery.asInstanceOf[ObjectNode].put("database", databaseId)
            Option(datasetQuery.get("native")).foreach { nativeNode =>
              Option(nativeNode.get("template-tags")).foreach { templateTags =>
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

    def updatePostgresDatabaseQuery(json: JsonNode, domainTable: String, questionTable: String, entityColumnName: String, entityColumnId: String, entityType: String): JsonNode = {
      Try {
        val queryNode = json.at("/dataset_query/native/query")
        if (queryNode.isMissingNode || !queryNode.isTextual) {
          throw new IllegalArgumentException("Query node is missing or not a valid string.")
        }

        val updatedQuery = queryNode.asText()
          .replace("${questionTable}", s""""$questionTable"""")
          .replace("${domainTable}", s""""$domainTable"""")
          .replace("${entityColumnName}", s"""$entityColumnName""" )
          .replace("${entityColumnId}", s"""$entityColumnId""")
          .replace("${entityType}", s"""$entityType""")
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

    processJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, domainTable, QuestionTable, params, paramsToRemove, entityColumnName, entityColumnId, entityType)
    println(s"---------------processed ProcessAndUpdateJsonFiles function----------------")
    questionCardId
  }
}