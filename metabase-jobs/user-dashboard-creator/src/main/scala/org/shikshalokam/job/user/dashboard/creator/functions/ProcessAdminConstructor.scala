package org.shikshalokam.job.user.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.postgresql.util.PGobject
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import scala.collection.mutable.ListBuffer
import scala.util.Try


object ProcessAdminConstructor {
  def processAdminJsonFiles(reportConfigQuery: String, collectionId: Int, databaseId: Int, dashboardId: Int, userMetrics: String, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil): ListBuffer[Int] = {
    val questionCardIds = ListBuffer[Int]()
    val objectMapper = new ObjectMapper()

    def processJson(): Unit = {
      val queryResult = postgresUtil.fetchData(reportConfigQuery)
      queryResult.foreach { row =>
        row.get("config") match {
          case Some(pgObject: PGobject) =>
            val configJson = objectMapper.readTree(pgObject.getValue)
            if (row.get("question_type").map(_.toString).getOrElse("") != "heading") {
              val originalQuestionCard = configJson.path("questionCard")
              val chartName = Option(originalQuestionCard.path("name").asText()).getOrElse("Unknown Chart")
              val updatedCard = updateAdminQuestion(configJson, collectionId, databaseId, userMetrics)
              val requestBody = updatedCard.asInstanceOf[ObjectNode]
              val cardId = new ObjectMapper().readTree(metabaseUtil.createQuestionCard(requestBody.toString)).path("id").asInt()
              println(s"Successfully created question card with card_id: $cardId for $chartName")
              questionCardIds += cardId
              val updatedDashCard = updateCardId(configJson, cardId)
              AddQuestionCards.appendDashCardToDashboard(metabaseUtil, updatedDashCard, dashboardId)
            } else {
              AddQuestionCards.appendDashCardToDashboard(metabaseUtil, Some(configJson), dashboardId)
            }
          case _ => println("No config found in row.")
        }
      }
    }

    def updateAdminQuestion(json: JsonNode, collectionId: Int, databaseId: Int, userMetrics: String): JsonNode = {
      val copy = json.deepCopy().asInstanceOf[ObjectNode]
      Option(copy.get("questionCard")).foreach { questionCard =>
        questionCard.asInstanceOf[ObjectNode].put("collection_id", collectionId)
        questionCard.at("/dataset_query").asInstanceOf[ObjectNode].put("database", databaseId)
        val queryNode = questionCard.at("/dataset_query/native/query")
        if (queryNode.isTextual) {
          val updatedQuery = queryNode.asText()
            .replace("${userMetrics}", s""""$userMetrics"""")
          questionCard.at("/dataset_query/native").asInstanceOf[ObjectNode]
            .put("query", updatedQuery)
        }
      }
      copy.get("questionCard")
    }

    def updateCardId(json: JsonNode, cardId: Int): Option[JsonNode] = {
      Try {
        val dashNode = json.deepCopy().asInstanceOf[ObjectNode]

        // Check if dashCards exists and is an object
        if (dashNode.has("dashCards") && dashNode.get("dashCards").isObject) {
          val dashCardsNode = dashNode.get("dashCards").asInstanceOf[ObjectNode]
          dashCardsNode.put("card_id", cardId)

          // If parameter_mappings exists, update card_id inside it
          if (dashCardsNode.has("parameter_mappings") && dashCardsNode.get("parameter_mappings").isArray) {
            val paramMappings = dashCardsNode.get("parameter_mappings").asInstanceOf[ArrayNode]
            paramMappings.elements().forEachRemaining { param =>
              if (param.isObject) {
                param.asInstanceOf[ObjectNode].put("card_id", cardId)
              }
            }
          }
        }

        dashNode
      }.toOption
    }

    processJson()
    questionCardIds
  }
}