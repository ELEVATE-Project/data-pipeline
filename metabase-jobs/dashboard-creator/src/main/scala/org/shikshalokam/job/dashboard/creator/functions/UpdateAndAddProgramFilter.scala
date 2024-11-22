package org.shikshalokam.job.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.shikshalokam.job.util.MetabaseUtil

import java.io.File

object UpdateAndAddProgramFilter {
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
    def replaceProgramName(json: JsonNode, ProgramName: String): JsonNode = {
      def processNode(node: JsonNode): JsonNode = {
        node match {
          case obj: ObjectNode =>
            // Process each field in the object
            obj.fieldNames().forEachRemaining { fieldName =>
              val childNode = obj.get(fieldName)
              if (childNode.isTextual && childNode.asText().contains("PROGRAMNAME")) {
                obj.put(fieldName, childNode.asText().replace("PROGRAMNAME", ProgramName))
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

          case _ => node // Return other types of nodes unchanged
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
//        println(s"API Response: $questionCardResponse")

        // Parse the JSON response
        val responseJson = objectMapper.readTree(questionCardResponse)
        Option(responseJson.get("id")).map(_.asInt()).getOrElse {
          println("Error: 'id' field not found in the response.")
          -1 // Return a default value
        }
      } catch {
        case ex: Exception =>
          println(s"Error fetching 'id' from response: ${ex.getMessage}")
          -1 // Return default value in case of error
      }
    }


    // Read and process JSON file
    readJsonFile(filterFilePath) match {
      case Some(json) =>
        // Replace the state name and get the question ID
        val ReplacedProgramNameJson = replaceProgramName(json, programname)
        val updatedJson = updateCollectionIdAndDatabaseId(ReplacedProgramNameJson,collectionId, databaseId)
        val questionId = getTheQuestionId(updatedJson)
        questionId
      case None =>
        println("Failed to process JSON file.")
        -1 // Return -1 if JSON file reading fails
    }
  }
}