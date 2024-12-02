package org.shikshalokam.job.dashboard.creator.functions

import org.shikshalokam.job.util.MetabaseUtil
//import play.api.libs.json._
import java.nio.file.{Files, Paths}
import scala.io.Source
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import scala.collection.JavaConverters._

object UpdateParameters {
  def updateStateParameterFunction(metabaseUtil: MetabaseUtil, parameterFilePath: String, dashboardId: Int, stateId: Int): Unit = {
    println(s"-----------Started Processing State dashboard parameter ------------")

    val objectMapper = new ObjectMapper()

    // Read and parse JSON file
    val fileContent = Source.fromFile(parameterFilePath).getLines().mkString
    val parameterJson = objectMapper.readTree(fileContent).asInstanceOf[ArrayNode]

    // Update parameters
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

    println(s"updatedParameterJson = $updatedParameterJson")

    // Fetch dashboard details
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson = dashboardJson.path("parameters").asInstanceOf[ArrayNode]

    // Filter out existing "select_district" parameters and add updated ones
    val finalParametersJson = currentParametersJson.elements().asScala.filterNot { param =>
      param.path("slug").asText() == "select_district"
    }.toList ++ updatedParameterJson

    val finalParametersArray = objectMapper.createArrayNode()
    finalParametersJson.foreach(finalParametersArray.add)

    // Create the payload
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersArray)

    // Update the dashboard
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"----------------Successfully updated State dashboard parameter: $updateResponse----------------")
  }


  def UpdateAdminParameterFunction(metabaseUtil: MetabaseUtil, parameterFilePath: String, dashboardId: Int): Unit = {
    println(s"-----------Started Processing Admin dashboard parameter ------------")

    val objectMapper = new ObjectMapper()

    // Read and parse JSON file
    val fileContent = new String(Files.readAllBytes(Paths.get(parameterFilePath)), "UTF-8")
    val parameterJson = objectMapper.readTree(fileContent).asInstanceOf[ArrayNode]

    // Update parameters (in this case, no specific update logic is provided, so it just passes through)
    val updatedParameterJson = parameterJson.elements().asScala.map { param =>
      param.asInstanceOf[ObjectNode]
    }.toList

    // Fetch dashboard details
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson = dashboardJson.path("parameters").asInstanceOf[ArrayNode]

    // Combine current parameters and updated parameters
    val finalParametersJson = objectMapper.createArrayNode()
    currentParametersJson.elements().asScala.foreach(finalParametersJson.add)
    updatedParameterJson.foreach(finalParametersJson.add)

    // Create the payload
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersJson)

    // Update the dashboard
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"----------------Successfully updated Admin dashboard parameter: $updateResponse----------------")
  }

  def UpdateProgramParameterFunction(metabaseUtil: MetabaseUtil, parameterFilePath: String, dashboardId: Int, programId: Int): Unit = {
    println(s"-----------Started Processing Program dashboard parameter ------------")

    val objectMapper = new ObjectMapper()

    // Read and parse JSON file
    val fileContent = Source.fromFile(parameterFilePath).getLines().mkString
    val parameterJson = objectMapper.readTree(fileContent).asInstanceOf[ArrayNode]

    // Update parameters
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

    // Fetch dashboard details
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson = dashboardJson.path("parameters").asInstanceOf[ArrayNode]

    // Filter out existing "select_state" parameters and add updated ones
    val finalParametersJson = objectMapper.createArrayNode()
    currentParametersJson.elements().asScala.filterNot { param =>
      param.path("slug").asText() == "select_state"
    }.foreach(finalParametersJson.add)
    updatedParameterJson.foreach(finalParametersJson.add)

    // Create the payload
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersJson)

    // Update the dashboard
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"----------------Successfully updated Program dashboard parameter: $updateResponse----------------")
  }

  def UpdateDistrictParameterFunction(metabaseUtil: MetabaseUtil, parameterFilePath: String, dashboardId: Int, districtId: Int): Unit = {
    println(s"-----------Started Processing District dashboard parameter ------------")

    val objectMapper = new ObjectMapper()

    // Read and parse JSON file
    val fileContent = Source.fromFile(parameterFilePath).getLines().mkString
    val parameterJson = objectMapper.readTree(fileContent).asInstanceOf[ArrayNode]

    // Update parameters id
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

    // Fetch dashboard details
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)
    val dashboardJson = objectMapper.readTree(dashboardResponse)
    val currentParametersJson = dashboardJson.path("parameters").asInstanceOf[ArrayNode]

    // Filter out existing "select_program_name" parameters and add updated ones
    val finalParametersJson = objectMapper.createArrayNode()
    currentParametersJson.elements().asScala.filterNot { param =>
      param.path("slug").asText() == "select_program_name"
    }.foreach(finalParametersJson.add)
    updatedParameterJson.foreach(finalParametersJson.add)

    // Create the payload
    val updatePayload = objectMapper.createObjectNode()
    updatePayload.set("parameters", finalParametersJson)

    // Update the dashboard
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString)
    println(s"----------------Successfully updated District dashboard parameter: $updateResponse----------------")
  }

}
