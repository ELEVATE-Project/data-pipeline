package org.shikshalokam.job.mentoring.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.postgresql.util.PGobject
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object ProcessTenantConstructor {
  def ProcessAndUpdateJsonFiles(reportConfigQuery: String, collectionId: Int, databaseId: Int, dashboardId: Int, orgIdSession: Int, orgIdMentor: Int, orgIdRating: Int,
                                tenantUserTable: String, tenantSessionTable: String, tenantSessionAttendanceTable: String, tenantConnectionsTable: String, tenantOrgMentorRatingTable: String, tenantOrgRolesTable: String,
                                immutableSlugNameToFilterMap: Map[String, Int] = Map.empty, orgId: Int, tabId: Int, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil): ListBuffer[Int] = {
    println(s"---------------started processing ProcessAndUpdateJsonFiles function----------------")
    val questionCardId = ListBuffer[Int]()
    val objectMapper = new ObjectMapper()

    def processJsonFiles(reportConfigQuery: String, collectionId: Int, databaseId: Int, dashboardId: Int, orgIdSession: Int, orgIdMentor: Int, orgIdRating: Int, tabId: Int): Unit = {
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
                val updatedQuestionCard = updateQuestionCardJsonValues(configJson, collectionId, orgIdSession, orgIdMentor, orgIdRating, databaseId)
                val finalQuestionCard = updatePostgresDatabaseQuery(updatedQuestionCard, tenantUserTable, tenantSessionTable, tenantSessionAttendanceTable, tenantConnectionsTable, tenantOrgMentorRatingTable, tenantOrgRolesTable, orgId)
                val requestBody = finalQuestionCard.asInstanceOf[ObjectNode]
                val cardId = mapper.readTree(metabaseUtil.createQuestionCard(requestBody.toString)).path("id").asInt()
                println(s">>>>>>>>> Successfully created question card with card_id: $cardId for $chartName")
                questionCardId.append(cardId)
                val updatedQuestionIdInDashCard = updateQuestionIdInDashCard(configJson, dashboardId, cardId, tabId)
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

    def updateQuestionIdInDashCard(json: JsonNode, dashboardId: Int, cardId: Int, tabId: Int): Option[JsonNode] = {
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

    def updateQuestionCardJsonValues(configJson: JsonNode, collectionId: Int, orgIdSession: Int, orgIdMentor: Int, orgIdRating: Int, databaseId: Int): JsonNode = {
      try {
        val configObjectNode = configJson.deepCopy().asInstanceOf[ObjectNode]

        Option(configObjectNode.get("questionCard")).foreach { questionCard =>
          questionCard.asInstanceOf[ObjectNode].put("collection_id", collectionId)

          Option(questionCard.get("dataset_query")).foreach { datasetQuery =>
            datasetQuery.asInstanceOf[ObjectNode].put("database", databaseId)

            Option(datasetQuery.get("native")).foreach { nativeNode =>
              Option(nativeNode.get("query")).foreach { queryNode =>
                val queryText = queryNode.asText()

                // 👇 Detect table from query
                val isSessionTable = queryText.contains("FROM ${tenantSessionTable}")
                val isMentorTable = queryText.contains("${tenantOrgRolesTable}")
                val isRatingTable = queryText.contains("FROM ${tenantOrgMentorRatingTable}")

                // Choose correct field id
                val orgFieldId =
                  if (isSessionTable) orgIdSession
                  else if (isMentorTable) orgIdMentor
                  else if (isRatingTable) orgIdRating // assuming orgIdMentor is used for ratings as well
                  else orgIdMentor // default fallback

                // Update template-tags
                Option(nativeNode.get("template-tags")).foreach { templateTags =>
                  val params = Map(
                    "select_organization" -> orgFieldId,
                    "select_org_a" -> orgFieldId,
                    "select_org_b" -> orgFieldId
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
        }

        configObjectNode.get("questionCard")
      } catch {
        case e: Exception =>
          println(s"Warning: JSON node could not be updated. Error: ${e.getMessage}")
          configJson.path("questionCard")
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

    def updatePostgresDatabaseQuery(json: JsonNode, tenantUserTable: String, tenantSessionTable: String, tenantSessionAttendanceTable: String,
                                    tenantConnectionsTable: String, tenantOrgMentorRatingTable: String, tenantOrgRolesTable: String, orgId: Int): JsonNode = {
      Try {
        val queryNode = json.at("/dataset_query/native/query")
        if (queryNode.isMissingNode || !queryNode.isTextual) {
          throw new IllegalArgumentException("Query node is missing or not a valid string.")
        }

        val updatedQuery = queryNode.asText()
          .replace("${tenantUserTable}", tenantUserTable)
          .replace("${tenantSessionTable}", tenantSessionTable)
          .replace("${tenantSessionAttendanceTable}", tenantSessionAttendanceTable)
          .replace("${tenantConnectionsTable}", tenantConnectionsTable)
          .replace("${tenantOrgMentorRatingTable}", tenantOrgMentorRatingTable)
          .replace("${tenantOrgRolesTable}", tenantOrgRolesTable)
          .replace("${orgId}", orgId.toString)

        val updatedJson = json.deepCopy().asInstanceOf[ObjectNode]
        val dq = updatedJson.path("dataset_query")
        val native = dq.path("native")
        native match {
          case obj: ObjectNode => obj.put("query", updatedQuery)
          case _ => throw new IllegalArgumentException("Missing or invalid dataset_query.native object")
        }

        updatedJson
      } match {
        case Success(updatedQueryJson) => updatedQueryJson
        case Failure(exception) =>
          throw new IllegalArgumentException("Failed to update query in JSON", exception)
      }
    }

    processJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, orgIdSession, orgIdMentor, orgIdRating, tabId)
    println(s"---------------processed ProcessAndUpdateJsonFiles function----------------")
    questionCardId
  }
}