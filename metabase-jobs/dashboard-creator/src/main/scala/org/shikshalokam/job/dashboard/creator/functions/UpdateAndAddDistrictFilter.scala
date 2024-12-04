package org.shikshalokam.job.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.shikshalokam.job.util.MetabaseUtil

import java.io.File

object UpdateAndAddDistrictFilter {
  val objectMapper = new ObjectMapper()

  def updateAndAddFilter(metabaseUtil: MetabaseUtil, filterFilePath: String, statename: String, districtname: String, programname: String, collectionId: Int, databaseId: Int): Int = {
    println(s"---------------- started processing updateAndAddFilter Function -------------------")

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

    def replaceDistrictName(json: JsonNode, statename: String, districtname: String): JsonNode = {
      def processNode(node: JsonNode): JsonNode = {
        node match {
          case obj: ObjectNode =>
            obj.fieldNames().forEachRemaining { fieldName =>
              val childNode = obj.get(fieldName)
              if (childNode.isTextual) {
                var updatedValue = childNode.asText()
                if (updatedValue.contains("DISTRICTNAME")) {
                  updatedValue = updatedValue.replace("DISTRICTNAME", districtname)
                  println(s"Updated value for DISTRICTNAME: $updatedValue")
                }
                if (updatedValue.contains("STATENAME")) {
                  updatedValue = updatedValue.replace("STATENAME", statename)
                  println(s"Updated value for STATENAME: $updatedValue")
                }
                obj.put(fieldName, updatedValue)
              } else {
                obj.set(fieldName, processNode(childNode))
              }
            }
            obj

          case array: ArrayNode =>
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

    readJsonFile(filterFilePath) match {
      case Some(json) =>
        val ReplacedDistrictNameJson = replaceDistrictName(json, statename, districtname)
        val updatedJson = updateCollectionIdAndDatabaseId(ReplacedDistrictNameJson, collectionId, databaseId)
        val questionId = getTheQuestionId(updatedJson)
        questionId
      case None =>
        println("Failed to process JSON file.")
        -1
    }
  }
}