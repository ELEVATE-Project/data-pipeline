package org.shikshalokam.job.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.shikshalokam.job.util.MetabaseUtil

import java.io.File

// This script will read the state-filter.json file, update the query by replacing 'STATENAME'
// with a dynamic state name, use the createQuestionCard API to create a question card, retrieve the question card ID, and update the card ID in the state-parameter.json file.

object UpdateAndAddStateFilter {
  val objectMapper = new ObjectMapper()

  def updateAndAddFilter(metabaseUtil: MetabaseUtil, filterFilePath: String, statename: String, districtname: String, programname: String,collectionId:Int,databaseId:Int): Int = {
    println(s"---------------- started processing updateAndAddFilter Function -------------------")
    // Read JSON from file
    def readJsonFile(filePath: String): Option[JsonNode] = {
      try {
        val file = new File(filePath)
        if (file.exists() && file.isFile) {
          val jsonNode = objectMapper.readTree(file)
          Some(jsonNode)
        } else {
          println(s"Error: File '$filePath' does not exist or is not a valid file.")
          None
        }
      } catch {
        case ex: Exception =>
          println(s"Error reading JSON file: ${ex.getMessage}")
          None
      }
    }

    // Replace "STATENAME" in the JSON with the provided state name
    def replaceStateName(json: JsonNode, stateName: String): JsonNode = {
      def processNode(node: JsonNode): JsonNode = {
        node match {
          case obj: ObjectNode =>
            obj.fieldNames().forEachRemaining { fieldName =>
              val childNode = obj.get(fieldName)
              if (childNode.isTextual && childNode.asText().contains("STATENAME")) {
                // Replace "STATENAME" in text fields
                obj.put(fieldName, childNode.asText().replace("STATENAME", stateName))
              } else {
                // Recursively process child nodes
                obj.set(fieldName, processNode(childNode))
              }
            }
            obj

          case array: ArrayNode =>
            // Process each element in the array
            val updatedArray = array.elements()
            val newArray = array.deepCopy().asInstanceOf[ArrayNode]
            newArray.removeAll()
            while (updatedArray.hasNext) {
              newArray.add(processNode(updatedArray.next()))
            }
            newArray

          case _ => node
        }
      }

      processNode(json)
    }

    def updateCollectionIdAndDatabaseId(jsonFile: JsonNode, collectionId: Int, databaseId: Int): JsonNode = {
      try {
        // Ensure the root node contains the "questionCard" field
        val questionCardNode = jsonFile.get("questionCard").asInstanceOf[ObjectNode]
        if (questionCardNode == null) {
          throw new IllegalArgumentException("'questionCard' node not found.")
        }
        // Update "collection_id"
        questionCardNode.put("collection_id", collectionId)

        // Access and update "database" inside "dataset_query"
        val datasetQueryNode = questionCardNode.get("dataset_query").asInstanceOf[ObjectNode]
        if (datasetQueryNode == null) {
          throw new IllegalArgumentException("'dataset_query' node not found.")
        }
        datasetQueryNode.put("database", databaseId)

        // Return the updated JSON
        jsonFile
      } catch {
        case ex: Exception =>
          throw new RuntimeException(s"Error updating JSON: ${ex.getMessage}", ex)
      }
    }

    // Get 'id' from the response JSON
    def getTheQuestionId(json: JsonNode): Int = {
      println(s"Request JSON: ${json.toString}") // Log the request

      try {
        val requestBody = json.get("questionCard")
        val questionCardResponse = metabaseUtil.createQuestionCard(requestBody.toString)

        // Log the response for debugging
        println(s"API Response: $questionCardResponse")

        // Parse the JSON response
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


    // Read and process JSON file
    readJsonFile(filterFilePath) match {
      case Some(json) =>
        // Replace the state name and get the question ID
        val ReplacedStateNameJson = replaceStateName(json, statename)
        val updatedJson = updateCollectionIdAndDatabaseId(ReplacedStateNameJson,collectionId, databaseId)
        val questionId = getTheQuestionId(updatedJson)
        questionId
      case None =>
        println("Failed to process JSON file.")
        -1
    }
  }
}