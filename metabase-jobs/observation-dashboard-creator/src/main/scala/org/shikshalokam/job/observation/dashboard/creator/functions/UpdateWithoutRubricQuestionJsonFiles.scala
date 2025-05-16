package org.shikshalokam.job.observation.dashboard.creator.functions

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import org.postgresql.util.PGobject
import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object UpdateWithoutRubricQuestionJsonFiles {
  def ProcessAndUpdateJsonFiles(collectionId: Int, databaseId: Int, dashboardId: Int, statenameId: Int, districtnameId: Int, schoolId: Int, clusterId: Int, question: String, metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, report_config: String): ListBuffer[Int] = {
    println(s"---------------started processing ProcessAndUpdateJsonFiles function----------------")
    val questionCardId = ListBuffer[Int]()
    val objectMapper = new ObjectMapper()

    val csvConfigQuery = s"SELECT * FROM $report_config WHERE dashboard_name = 'Observation-Question-Without-Rubric' AND question_type = 'table';"

    def processCsvJsonFiles(collectionId: Int, databaseId: Int, dashboardId: Int, statenameId: Int, districtnameId: Int, schoolId: Int, clusterId: Int, questionTable: String, newRow : Int, newCol: Int): Unit = {
      val queryResult = postgresUtil.fetchData(csvConfigQuery)
      queryResult.foreach { row =>
        if (row.get("question_type").map(_.toString).getOrElse("") != "heading") {
          row.get("config") match {
            case Some(queryValue: PGobject) =>
              val configJson = objectMapper.readTree(queryValue.getValue)
              if (configJson != null) {
                val originalQuestionCard = configJson.path("questionCard")
                val chartName = Option(originalQuestionCard.path("name").asText()).getOrElse("Unknown Chart")
                val updatedQuestionCard = updateQuestionCardJsonValues(configJson, collectionId, statenameId, districtnameId, schoolId, clusterId, databaseId)
                val finalQuestionCard = updatePostgresDatabaseQuery(updatedQuestionCard, questionTable, null)
                val requestBody = finalQuestionCard.asInstanceOf[ObjectNode]
                val cardId = mapper.readTree(metabaseUtil.createQuestionCard(requestBody.toString)).path("id").asInt()
                println(s">>>>>>>>> Successfully created question card with card_id: $cardId for $chartName")
                questionCardId.append(cardId)
                val updatedQuestionIdInDashCard = updateQuestionIdInDashCard(configJson, cardId, newRow , newCol )
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

    def processJsonFiles(collectionId: Int, databaseId: Int, dashboardId: Int, statenameId: Int, districtnameId: Int, schoolId: Int, clusterId: Int, question: String, report_config: String): Unit = {
      val queries = Map(
        "nonMatrix" -> s"""SELECT distinct(question_id),question_text,question_type FROM "$question" WHERE has_parent_question = 'false'""",
        "matrix" -> s"""SELECT distinct(question_id),question_type,question_text, parent_question_text FROM "$question" WHERE has_parent_question = 'true'""",
        "slider" -> s"SELECT * FROM $report_config WHERE dashboard_name = 'Observation-Question-Without-Rubric' AND question_type = 'slider-chart';",
        "radio" -> s"SELECT * FROM $report_config WHERE dashboard_name = 'Observation-Question-Without-Rubric' AND question_type = 'radio-chart';",
        "multiselect" -> s"SELECT * FROM $report_config WHERE dashboard_name = 'Observation-Question-Without-Rubric' AND question_type = 'multiselect-chart';",
        "numbers" -> s"SELECT * FROM $report_config WHERE dashboard_name = 'Observation-Question-Without-Rubric' AND question_type = 'number-chart';",
        "text" -> s"SELECT * FROM $report_config WHERE dashboard_name = 'Observation-Question-Without-Rubric' AND question_type = 'text-chart';",
        "heading" -> s"SELECT * FROM $report_config WHERE dashboard_name = 'Observation-Question-Without-Rubric' AND question_type = 'heading';",
        "date" -> s"SELECT * FROM $report_config WHERE dashboard_name = 'Observation-Question-Without-Rubric' AND question_type = 'date-chart';"
      )

      val results = queries.map { case (key, query) => key -> postgresUtil.fetchData(query) }
      var newRow = 0
      var newCol = 0

      def processHeading(questionText: String): Unit = {
        results("heading").foreach { headingRow =>
          headingRow.get("config") match {
            case Some(queryValue: PGobject) =>
              val jsonString = queryValue.getValue
              val rootNode = objectMapper.readTree(jsonString)
              Option(rootNode).foreach { node =>
                val dashCardsNode = node.path("dashCards")
                val visualizationSettingsNode = dashCardsNode.path("visualization_settings")
                if (visualizationSettingsNode.has("text")) {
                  visualizationSettingsNode.asInstanceOf[ObjectNode].put("text", questionText)
                }
                newRow += 3
                dashCardsNode.asInstanceOf[ObjectNode].put("col", newCol)
                dashCardsNode.asInstanceOf[ObjectNode].put("row", newRow)
                AddQuestionCards.appendDashCardToDashboard(metabaseUtil, toOption(node), dashboardId)
              }
            case None => println("Key 'config' not found in the heading result row.")
          }
        }
      }

      def processQuestionType(questionType: String, questionId: String, questionText: String): Unit = {
        val resultKey = questionType match {
          case "slider" => "slider"
          case "radio" => "radio"
          case "multiselect" => "multiselect"
          case "number" => "numbers"
          case "text" => "text"
          case "date" => "date"
          case _ => return
        }

        results(resultKey).foreach { row =>
          row.get("config") match {
            case Some(queryValue: PGobject) =>
              val configJson = objectMapper.readTree(queryValue.getValue)
              Option(configJson).foreach { json =>
                val updatedQuestionCard = updateQuestionCardJsonValues(json, collectionId, statenameId, districtnameId, schoolId, clusterId, databaseId)
                val finalQuestionCard = updatePostgresDatabaseQuery(updatedQuestionCard, question, questionId)
                val requestBody = finalQuestionCard.asInstanceOf[ObjectNode]
                val cardId = mapper.readTree(metabaseUtil.createQuestionCard(requestBody.toString)).path("id").asInt()
                println(s">>>>>>>>> Successfully created question card with card_id: $cardId")
                questionCardId.append(cardId)
                val originalDashcard = json.path("dashCards")
                val existingSizeY = originalDashcard.path("size_y").asInt()
                val updatedDashCard = updateQuestionIdInDashCard(json, cardId, newRow, newCol)
                newRow += existingSizeY + 1
                AddQuestionCards.appendDashCardToDashboard(metabaseUtil, updatedDashCard, dashboardId)
              }
            case None => println(s"Key 'config' not found in the $resultKey result row.")
          }
        }
      }

      results("nonMatrix").foreach { row =>
        val questionId = row.get("question_id").map(_.toString).getOrElse("")
        val questionText = row.get("question_text").map(_.toString).getOrElse("")
        val questionType = row.get("question_type").map(_.toString).getOrElse("")
        processHeading(questionText)
        processQuestionType(questionType, questionId, questionText)
      }

      val processedParentQuestions = scala.collection.mutable.Set[String]()
      var questionCounter = 1

      results("matrix").foreach { row =>
        val questionId = row.get("question_id").map(_.toString).getOrElse("")
        val questionType = row.get("question_type").map(_.toString).getOrElse("")
        val questionText = row.get("question_text").map(_.toString).getOrElse("")
        val parentQuestionText = row.get("parent_question_text").map(_.toString).getOrElse("")

        if (!processedParentQuestions.contains(parentQuestionText)) {
          processHeading(parentQuestionText)
          processedParentQuestions.add(parentQuestionText)
        }

        val numberedQuestionText = s"$questionCounter. $questionText"
        questionCounter += 1
        processHeading(numberedQuestionText)
        processQuestionType(questionType, questionId, questionText)
      }

      processCsvJsonFiles(collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolId, clusterId, question, newRow, newCol)
    }


    def toOption(jsonNode: JsonNode): Option[JsonNode] = {
      if (jsonNode == null || jsonNode.isMissingNode) None else Some(jsonNode)
    }

    def updateQuestionIdInDashCard(json: JsonNode, cardId: Int, newRow: Int, newCol: Int): Option[JsonNode] = {
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
        dashCardsNode.put("row", newRow)
        dashCardsNode.put("col", newCol)

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

    def updateQuestionCardJsonValues(configJson: JsonNode, collectionId: Int, statenameId: Int, districtnameId: Int, schoolId: Int, clusterId: Int, databaseId: Int): JsonNode = {
      try {
        val configObjectNode = configJson.deepCopy().asInstanceOf[ObjectNode]
        Option(configObjectNode.get("questionCard")).foreach { questionCard =>
          questionCard.asInstanceOf[ObjectNode].put("collection_id", collectionId)

          Option(questionCard.get("dataset_query")).foreach { datasetQuery =>
            datasetQuery.asInstanceOf[ObjectNode].put("database", databaseId)

            Option(datasetQuery.get("native")).foreach { nativeNode =>
              Option(nativeNode.get("template-tags")).foreach { templateTags =>
                val params = Map(
                  "state_param" -> statenameId,
                  "district_param" -> districtnameId,
                  "school_param" -> schoolId,
                  "cluster_param" -> clusterId
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

    def updatePostgresDatabaseQuery(json: JsonNode, questionTable: String, questionId: String): JsonNode = {
      Try {
        val queryNode = json.at("/dataset_query/native/query")
        if (queryNode.isMissingNode || !queryNode.isTextual) {
          throw new IllegalArgumentException("Query node is missing or not a valid string.")
        }

        val updatedQuery = queryNode.asText()
          .replace("${questionTable}", s""""$questionTable"""")
          .replace("${questionId}", s"""'$questionId'""")
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

    processJsonFiles(collectionId, databaseId, dashboardId, statenameId, districtnameId, schoolId, clusterId, question, report_config)
    println(s"---------------processed ProcessAndUpdateJsonFiles function----------------")
    questionCardId
  }
}
