package org.shikshalokam.job.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.postgresql.util.PGobject
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}


object UpdateAdminJsonFiles {
  def ProcessAndUpdateJsonFiles(report_config_query: String, collectionId: Int, databaseId: Int, dashboardId: Int, statenameId: Int, districtnameId: Int, programnameId: Int, projects : String, solutions: String, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil): ListBuffer[Int] = {
    println(s"---------------started processing ProcessAndUpdateJsonFiles function----------------")
    val questionCardId = ListBuffer[Int]()
    val objectMapper = new ObjectMapper()

    def processJsonFiles(report_config_query: String, collectionId: Int, databaseId: Int, dashboardId: Int, statenameId: Int, districtnameId: Int, programnameId: Int): Unit = {
      val adminIdStatus = postgresUtil.fetchData(report_config_query)
      adminIdStatus.foreach { row =>
        if (row.get("question_type").map(_.toString).getOrElse("") != "heading") {
          row.get("query") match {
            case Some(queryValue: PGobject) =>
              val jsonString = queryValue.getValue
              val rootNode = objectMapper.readTree(jsonString)
              if (rootNode != null) {
                val questionCardNode = rootNode.path("questionCard")
                println(s"questionCardNode = $questionCardNode")
                val chartName = Option(questionCardNode.path("name").asText()).getOrElse("Unknown Chart")
                println(s" >>>>>>>>>>> Started Processing For The Chart: $chartName")
                val updatedJson = updateJsonFiles(rootNode, collectionId = collectionId, statenameId = statenameId, districtnameId = districtnameId, programnameId = programnameId, databaseId = databaseId)
                println(s"updateJson = $updatedJson")
                val updatedJsonWithQuery = updateQuery(json = updatedJson.path("questionCard"), projectsTable = projects, solutionsTable = solutions)
                println(s"updatedJsonWithQuery = $updatedJsonWithQuery")
                val requestBody = updatedJsonWithQuery.asInstanceOf[ObjectNode]
                val response = metabaseUtil.createQuestionCard(requestBody.toString)
                println(s"response = $response")
                val cardIdOpt = extractCardId(response)
                println(s"cardIdOpt = $cardIdOpt")

                cardIdOpt match {
                  case Some(cardId) =>
                    println(s">>>>>>>>> Successfully created question card with card_id: $cardId for $chartName")
                    questionCardId.append(cardId)
                    val updatedJsonOpt = updateJsonWithCardId(updatedJson, cardId)
                    println(s"updatedJsonOpt = $updatedJsonOpt")
                    println(s"--------Successfully updated the json file---------")
                    AddQuestionCards.appendDashCardToDashboard(metabaseUtil, updatedJsonOpt, dashboardId)
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
          row.get("query") match {
            case Some(queryValue: PGobject) =>
              val jsonString = queryValue.getValue
              val rootNode = objectMapper.readTree(jsonString)
              println(s"rootNodeAtElse = $rootNode")
              if (rootNode != null) {
                  val optJsonNode = toOption(rootNode)
                  println(s"optJsonNodeAtElse = $optJsonNode")
                  AddQuestionCards.appendDashCardToDashboard(metabaseUtil, optJsonNode, dashboardId)
              }
          }
        }
      }
    }

    def toOption(jsonNode: JsonNode): Option[JsonNode] = {
      if (jsonNode == null || jsonNode.isMissingNode) None else Some(jsonNode)
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


    def updateJsonFiles(jsonNode: JsonNode, collectionId: Int, statenameId: Int, districtnameId: Int, programnameId: Int, databaseId: Int): JsonNode = {
      try {
        val rootNode = jsonNode.deepCopy().asInstanceOf[ObjectNode]

        if (rootNode.has("questionCard")) {
          val questionCard = rootNode.get("questionCard").asInstanceOf[ObjectNode]
          questionCard.put("collection_id", collectionId)
          println(s"Updated questionCard: $questionCard")

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

        println(s"Updated rootNode: $rootNode")
        rootNode
      } catch {
        case e: Exception =>
          println(s"Warning: JSON node could not be updated. Error: ${e.getMessage}")
          jsonNode
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

    def updateQuery(json: JsonNode, projectsTable: String, solutionsTable: String): JsonNode = {
      Try {

        val queryPath = "/dataset_query/native/query"
        val queryNode = json.at(queryPath)
        if (queryNode.isMissingNode || !queryNode.isTextual) {
          throw new IllegalArgumentException(s"Query node at path $queryPath is missing or not textual.")
        }

        val updatedQuery = queryNode.asText()
          .replace("${config.projects}", projectsTable)
          .replace("${config.solutions}", solutionsTable)
        val datasetQuery = json.get("dataset_query").deepCopy().asInstanceOf[ObjectNode]
        val nativeNode = datasetQuery.get("native").deepCopy().asInstanceOf[ObjectNode]
        nativeNode.set("query", TextNode.valueOf(updatedQuery))
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

    processJsonFiles(report_config_query, collectionId, databaseId, dashboardId, statenameId, districtnameId, programnameId)
    println(s"---------------processed ProcessAndUpdateJsonFiles function----------------")
    questionCardId
  }
}
