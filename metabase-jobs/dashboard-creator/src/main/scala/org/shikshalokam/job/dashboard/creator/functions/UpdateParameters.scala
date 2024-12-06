package org.shikshalokam.job.dashboard.creator.functions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.shikshalokam.job.util.MetabaseUtil

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.io.Source

object UpdateParameters {
  def updateStateParameterFunction(metabaseUtil: MetabaseUtil, parameterFilePath: String, dashboardId: Int, stateId: Int): Unit = {
    println(s"-----------Started Processing State dashboard parameter ------------")

    val objectMapper = new ObjectMapper()
    val fileContent = Source.fromFile(parameterFilePath).getLines().mkString
    val parameterJson = objectMapper.readTree(fileContent).asInstanceOf[ArrayNode]
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


  def UpdateAdminParameterFunction(metabaseUtil: MetabaseUtil, parameterFilePath: String, dashboardId: Int): Unit = {
    println(s"-----------Started Processing Admin dashboard parameter ------------")

    val objectMapper = new ObjectMapper()
    val fileContent = new String(Files.readAllBytes(Paths.get(parameterFilePath)), "UTF-8")
    val parameterJson = objectMapper.readTree(fileContent).asInstanceOf[ArrayNode]
    val updatedParameterJson = parameterJson.elements().asScala.map { param =>
      param.asInstanceOf[ObjectNode]
    }.toList
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson = dashboardJson.path("parameters").asInstanceOf[ArrayNode]
    val finalParametersJson = objectMapper.createArrayNode()
    currentParametersJson.elements().asScala.foreach(finalParametersJson.add)
    updatedParameterJson.foreach(finalParametersJson.add)
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersJson)
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"----------------Successfully updated Admin dashboard parameter ----------------")
  }

  def UpdateProgramParameterFunction(metabaseUtil: MetabaseUtil, parameterFilePath: String, dashboardId: Int, programId: Int): Unit = {
    println(s"-----------Started Processing Program dashboard parameter ------------")

    val objectMapper = new ObjectMapper()
    val fileContent = Source.fromFile(parameterFilePath).getLines().mkString
    val parameterJson = objectMapper.readTree(fileContent).asInstanceOf[ArrayNode]
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
    val finalParametersJson = objectMapper.createArrayNode()
    currentParametersJson.elements().asScala.filterNot { param =>
      param.path("slug").asText() == "select_state"
    }.foreach(finalParametersJson.add)
    updatedParameterJson.foreach(finalParametersJson.add)
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersJson)
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"----------------Successfully updated Program dashboard parameter ----------------")
  }

  def UpdateDistrictParameterFunction(metabaseUtil: MetabaseUtil, parameterFilePath: String, dashboardId: Int, districtId: Int): Unit = {
    println(s"-----------Started Processing District dashboard parameter ------------")

    val objectMapper = new ObjectMapper()
    val fileContent = Source.fromFile(parameterFilePath).getLines().mkString
    val parameterJson = objectMapper.readTree(fileContent).asInstanceOf[ArrayNode]
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
    val finalParametersJson = objectMapper.createArrayNode()
    currentParametersJson.elements().asScala.filterNot { param =>
      param.path("slug").asText() == "select_program_name"
    }.foreach(finalParametersJson.add)
    updatedParameterJson.foreach(finalParametersJson.add)
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersJson)
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"----------------Successfully updated District dashboard parameter ----------------------")
  }

}
