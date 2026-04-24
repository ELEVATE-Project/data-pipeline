package org.shikshalokam.job.combined.dashboard.creator.functions.user

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory

object UpdateParameters {
  private val logger = LoggerFactory.getLogger(UpdateParameters.getClass)
  def updateDashboardParameters(metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, parametersQuery: String, dashboardId: Int, slugNameToCardIdMap: Map[String, Int] = Map.empty): Unit = {
    logger.info("-----------Started Processing dashboard parameters ------------")
    val objectMapper = new ObjectMapper()
    val parameterData: List[Any] = postgresUtil.fetchData(parametersQuery).flatMap(_.get("config"))
    val parameterJsonString: String = parameterData.headOption match {
      case Some(value: String) if value.nonEmpty => value
      case Some(value) if value.toString.nonEmpty => value.toString
      case _ => throw new Exception("Invalid or empty parameter data found")
    }
    val parameterJson: ArrayNode = objectMapper.readTree(parameterJsonString) match {
      case array: ArrayNode => array
      case _ => throw new Exception("Expected parameter data to be an ArrayNode")
    }
    val updatedParametersArray = objectMapper.createArrayNode()
    parameterJson.elements().asScala.foreach { param =>
      val updatedParam = param.deepCopy().asInstanceOf[ObjectNode]
      val slug = Option(updatedParam.path("slug").asText()).getOrElse("")
      if (slugNameToCardIdMap.contains(slug)) {
        val newCardId = slugNameToCardIdMap(slug)
        val valuesSourceConfigNode = updatedParam.path("values_source_config")
        if (valuesSourceConfigNode.isObject) {
          val valuesSourceConfig = valuesSourceConfigNode.asInstanceOf[ObjectNode]
          valuesSourceConfig.put("card_id", newCardId)
          updatedParam.set("values_source_config", valuesSourceConfig)
          logger.info(s" Filter '$slug' updated with card_id: $newCardId")
        } else {
          logger.warn(s" 'values_source_config' not found or invalid for slug: '$slug'")
        }
      } else {
        logger.info(s" Skipping slug '$slug' — no card_id mapping found.")
      }
      updatedParametersArray.add(updatedParam)
    }
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", updatedParametersArray)
    metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    logger.info("-----------Successfully updated dashboard parameters ------------")
  }
}
