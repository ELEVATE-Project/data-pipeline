package org.shikshalokam.job.mentoring.dashboard.creator.functions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import scala.collection.JavaConverters._
import org.slf4j.{Logger, LoggerFactory}

object UpdateParameters {
  private val logger: Logger = LoggerFactory.getLogger(UpdateParameters.getClass)

  def updateParameterFunction(metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, parametersQuery: String, slugNameToStateIdMap: Map[String, Int], dashboardId: Int): Unit = {
    logger.info("Started processing dashboard parameter")

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

    val updatedParameterJson = try {
      parameterJson.elements().asScala.flatMap { param =>
        try {
          if (param.isInstanceOf[ObjectNode]) {
            val updatedParam = param.asInstanceOf[ObjectNode]
            val slug = updatedParam.path("slug").asText()

            slugNameToStateIdMap.get(slug) match {
              case Some(newCardId) =>
                val valuesSourceConfigNode = updatedParam.path("values_source_config")
                if (valuesSourceConfigNode.isObject) {
                  val valuesSourceConfig = valuesSourceConfigNode.asInstanceOf[ObjectNode]
                  valuesSourceConfig.put("card_id", newCardId)
                  updatedParam.set("values_source_config", valuesSourceConfig)
                }
                Some(updatedParam)
              case None =>
                logger.debug(s"No card_id found for slug '$slug', skipping update")
                None
            }
          } else {
            println(s"Skipping param as it is not of type ObjectNode: ${param.toString}")
            None
          }
        } catch {
          case e: Exception =>
            logger.error(s"Error processing param: $param", e)
            None
        }
      }.toList
    } catch {
      case e: Exception =>
        logger.error("Error during JSON processing", e)
        List.empty
    }
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson = dashboardJson.path("parameters").asInstanceOf[ArrayNode]

    val finalParametersJson = (currentParametersJson.elements().asScala.filterNot { param =>
      val slug = param.path("slug").asText()
      slugNameToStateIdMap.contains(slug)
    }.toList ++ updatedParameterJson).distinct

    val finalParametersArray = objectMapper.createArrayNode()
    finalParametersJson.foreach(finalParametersArray.add)
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersArray)

    metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    logger.info("Successfully updated dashboard parameter")
  }

  def updateAdminParameterFunction(metabaseUtil: MetabaseUtil, parametersQuery: String, dashboardId: Int, postgresUtil: PostgresUtil): Unit = {
    logger.info("Started processing Admin dashboard parameter")

    val objectMapper = new ObjectMapper()
    val parameterData: List[Any] = postgresUtil.fetchData(parametersQuery).flatMap(_.get("config"))
    val parameterJsonString: String = parameterData.headOption match {
      case Some(value: String) => value
      case Some(value) => value.toString
      case None => throw new Exception("No parameter data found")
    }

    val parameterJson: ArrayNode = objectMapper.readTree(parameterJsonString) match {
      case array: ArrayNode => array
      case _ => throw new Exception("Expected parameter data to be an ArrayNode")
    }

    val updatedParameterJson: List[ObjectNode] = parameterJson.elements().asScala.map {
      case param: ObjectNode => param
      case _ => throw new Exception("Expected all parameters to be ObjectNodes")
    }.toList

    val dashboardResponse: String = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson: ArrayNode = dashboardJson.path("parameters") match {
      case array: ArrayNode => array
      case _ => objectMapper.createArrayNode()
    }

    val finalParametersJson = objectMapper.createArrayNode()
    currentParametersJson.elements().asScala.foreach(finalParametersJson.add)
    updatedParameterJson.foreach(finalParametersJson.add)

    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersJson)

    metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    logger.info("Successfully updated Admin dashboard parameter")
  }
}
