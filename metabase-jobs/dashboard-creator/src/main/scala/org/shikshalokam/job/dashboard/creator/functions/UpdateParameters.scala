package org.shikshalokam.job.dashboard.creator.functions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}

import scala.collection.JavaConverters._

object UpdateParameters {
  def updateStateParameterFunction(metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, parametersQuery: String, dashboardId: Int, stateId: Int): Unit = {
    println(s"-----------Started Processing State dashboard parameter ------------")

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
    val updatedParameterJson = parameterJson.elements().asScala.map { param =>
      val updatedParam = param.asInstanceOf[ObjectNode]
      val slug = updatedParam.path("slug").asText()

      if (slug == "select_district") {
        val valuesSourceConfig = updatedParam.path("values_source_config").asInstanceOf[ObjectNode]
        valuesSourceConfig.put("card_id", stateId)

        updatedParam.set("values_source_config", valuesSourceConfig)
      }
      updatedParam
    }.toList
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson = dashboardJson.path("parameters").asInstanceOf[ArrayNode]
    val finalParametersJson = currentParametersJson.elements().asScala.filterNot { param =>
      param.path("slug").asText() == "select_district"
    }.toList ++ updatedParameterJson
    val finalParametersArray = objectMapper.createArrayNode()
    finalParametersJson.foreach(finalParametersArray.add)
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersArray)
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"----------------Successfully updated State dashboard parameter----------------")
  }


  def UpdateAdminParameterFunction(metabaseUtil: MetabaseUtil, parametersQuery: String, dashboardId: Int, postgresUtil: PostgresUtil): Unit = {
    println(s"-----------Started Processing Admin dashboard parameter ------------")

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

    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"updateResponse = $updateResponse")
    println(s"----------------Successfully updated Admin dashboard parameter ----------------")
  }


  def UpdateProgramParameterFunction(metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, parametersQuery: String, dashboardId: Int, programId: Int): Unit = {
    println(s"-----------Started Processing Program dashboard parameter ------------")

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
    val updatedParameterJson = parameterJson.elements().asScala.map { param =>
      val updatedParam = param.asInstanceOf[ObjectNode]
      val slug = updatedParam.path("slug").asText()

      if (slug == "select_state") {
        val valuesSourceConfig = updatedParam.path("values_source_config").asInstanceOf[ObjectNode]
        valuesSourceConfig.put("card_id", programId)

        updatedParam.set("values_source_config", valuesSourceConfig)
      }
      updatedParam
    }.toList
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson = dashboardJson.path("parameters").asInstanceOf[ArrayNode]
    val finalParametersJson = currentParametersJson.elements().asScala.filterNot { param =>
      param.path("slug").asText() == "select_state"
    }.toList ++ updatedParameterJson

    val finalParametersArray = objectMapper.createArrayNode()
    finalParametersJson.foreach(finalParametersArray.add)
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersArray)
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"----------------Successfully updated Program dashboard parameter ----------------")
  }

  def UpdateDistrictParameterFunction(metabaseUtil: MetabaseUtil, postgresUtil: PostgresUtil, parametersQuery: String, dashboardId: Int, districtId: Int): Unit = {
    println(s"-----------Started Processing District dashboard parameter ------------")
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
    val updatedParameterJson = parameterJson.elements().asScala.map { param =>
      val updatedParam = param.asInstanceOf[ObjectNode]
      val slug = updatedParam.path("slug").asText()

      if (slug == "select_program_name") {
        val valuesSourceConfig = updatedParam.path("values_source_config").asInstanceOf[ObjectNode]
        valuesSourceConfig.put("card_id", districtId)

        updatedParam.set("values_source_config", valuesSourceConfig)
      }
      updatedParam
    }.toList
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson = dashboardJson.path("parameters").asInstanceOf[ArrayNode]
    val finalParametersJson = currentParametersJson.elements().asScala.filterNot { param =>
      param.path("slug").asText() == "select_program_name"
    }.toList ++ updatedParameterJson

    val finalParametersArray = objectMapper.createArrayNode()
    finalParametersJson.foreach(finalParametersArray.add)
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersArray)
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"----------------Successfully updated District dashboard parameter ----------------------")
  }

}
