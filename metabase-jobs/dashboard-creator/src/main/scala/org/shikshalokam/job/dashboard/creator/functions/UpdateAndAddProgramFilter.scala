package org.shikshalokam.job.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import scala.util.Try

object UpdateAndAddProgramFilter {
  val objectMapper = new ObjectMapper()

  def updateAndAddFilter(metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, filterQuery: String, targetedProgramId: String, collectionId: Int, databaseId: Int, projectTable: String, solutionTable: String): Int = {
    println(s"---------------- started processing updateAndAddFilter Function -------------------")

    def readJsonFromQuery(filterQuery: String): Option[JsonNode] = {
      try {
        // Fetching data from the database or any external source
        val queryResult = postgresUtil.fetchData(filterQuery).flatMap(_.get("config")) // Assuming this fetches JSON data

        // Convert the fetched result to a String (parameterString)
        val filterString: String = queryResult.headOption match {
          case Some(value: String) => value
          case Some(value) => value.toString
          case None => throw new Exception("No parameter data found")
        }

        // Convert parameterString to JsonNode using Jackson's ObjectMapper
        Try(objectMapper.readTree(filterString)).toOption match {
          case Some(jsonNode) => Some(jsonNode) // Successfully parsed JSON
          case None =>
            println(s"Error: Invalid JSON format in parameterString: $filterString")
            None
        }

      } catch {
        case ex: Exception =>
          println(s"Error reading or parsing the query result: ${ex.getMessage}")
          None
      }
    }

    def replaceProgramName(json: JsonNode, targatedProgramId: String, projectTable: String, solutionTable: String): JsonNode = {
      def processNode(node: JsonNode): JsonNode = {
        node match {
          case obj: ObjectNode =>
            obj.fieldNames().forEachRemaining { fieldName =>
              val childNode = obj.get(fieldName)
              if (childNode.isTextual) {
                var updatedText = childNode.asText()
                if (updatedText.contains("PROGRAMID")) {
                  updatedText = updatedText.replace("PROGRAMID", targatedProgramId)
                }

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
            val newArray = array.deepCopy().asInstanceOf[ArrayNode]
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

    readJsonFromQuery(filterQuery) match {
      case Some(json) =>
        val ReplacedProgramNameJson = replaceProgramName(json, targetedProgramId, projectTable, solutionTable)
        val updatedJson = updateCollectionIdAndDatabaseId(ReplacedProgramNameJson, collectionId, databaseId)
        val questionId = getTheQuestionId(updatedJson)
        questionId
      case None =>
        println("Failed to process JSON file.")
        -1
    }
  }
}