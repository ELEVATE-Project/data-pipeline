package org.shikshalokam.job.observation.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.shikshalokam.job.util.MetabaseUtil

import scala.util.{Failure, Success, Try}

object AddQuestionCards {

  val objectMapper = new ObjectMapper()

  def appendDashCardToDashboard(metabaseUtil: MetabaseUtil, jsonFile: Option[JsonNode], dashboardId: Int): Unit = {

    val dashboardResponse = objectMapper.readTree(metabaseUtil.getDashboardDetailsById(dashboardId))
    val existingDashcards = dashboardResponse.path("dashcards") match {
      case array: ArrayNode => array
      case _ => objectMapper.createArrayNode()
    }
    val dashCardsNode = readJsonFile(jsonFile)
    dashCardsNode.foreach { value =>
      existingDashcards.add(value)
    }
    val finalDashboardJson = objectMapper.createObjectNode()
    finalDashboardJson.set("dashcards", existingDashcards)
    val dashcardsString = objectMapper.writeValueAsString(finalDashboardJson)
    metabaseUtil.addQuestionCardToDashboard(dashboardId, dashcardsString)
    println(s"********************* Successfully updated Dashcards  *************************")
  }

  def readJsonFile(jsonContent: Option[JsonNode]): Option[JsonNode] = {
    jsonContent.flatMap { content =>
      Try {
        val dashCardsNode = content.path("dashCards")

        if (!dashCardsNode.isMissingNode) {
          Some(dashCardsNode)
        } else {
          println(s"'dashCards' key not found in JSON content.")
          None
        }
      } match {
        case Success(value) => value // Return the result if successful
        case Failure(exception) =>
          println(s"Error processing JSON content: ${exception.getMessage}")
          None
      }
    }
  }
}