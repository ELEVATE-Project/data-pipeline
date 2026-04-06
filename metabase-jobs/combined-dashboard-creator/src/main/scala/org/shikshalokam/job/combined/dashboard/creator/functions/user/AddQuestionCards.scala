package org.shikshalokam.job.combined.dashboard.creator.functions.user

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.shikshalokam.job.util.MetabaseUtil

import scala.util.{Failure, Success, Try}

import org.slf4j.LoggerFactory

object AddQuestionCards {
  private val logger = LoggerFactory.getLogger(AddQuestionCards.getClass)

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
    logger.info(s"********************* Successfully updated Dashcards  *************************")
  }

  def readJsonFile(jsonContent: Option[JsonNode]): Option[JsonNode] = {
    jsonContent.flatMap { content =>
      Try {
        val dashCardsNode = content.path("dashCards")

        if (!dashCardsNode.isMissingNode) {
          Some(dashCardsNode)
        } else {
          logger.info(s"'dashCards' key not found in JSON content.")
          None
        }
      } match {
        case Success(value) => value // Return the result if successful
        case Failure(exception) =>
          logger.info(s"Error processing JSON content: ${exception.getMessage}")
          None
      }
    }
  }
}