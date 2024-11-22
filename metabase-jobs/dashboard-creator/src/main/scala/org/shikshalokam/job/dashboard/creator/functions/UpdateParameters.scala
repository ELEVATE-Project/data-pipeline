package org.shikshalokam.job.dashboard.creator.functions

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.shikshalokam.job.dashboard.creator.task.MetabaseDashboardConfig
import org.shikshalokam.job.util.MetabaseUtil
import play.api.libs.json._

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.collection.immutable._
import scala.io.Source
import scala.sys.process._
import scala.util.{Failure, Success, Try}

object UpdateParameters {
  def updateStateParameterFunction(metabaseUtil: MetabaseUtil, parameterFilePath: String, dashboardId: Int, stateId: Int): Unit = {
    // Read the content of the JSON file
    println(s"-----------Started Processing State dashboard parameter ------------")
    val fileContent = Source.fromFile(parameterFilePath).getLines().mkString
    val parameterJson = Json.parse(fileContent).as[JsArray]

    // Update parameters with new 'card_id' for "select_district"
    val updatedParameterJson = parameterJson.value.map { param =>
      val updatedParam = param.as[JsObject]
      if ((updatedParam \ "slug").as[String] == "select_district") {
        // Update only the card_id in values_source_config
        val updatedValuesSourceConfig = (updatedParam \ "values_source_config").as[JsObject] + (
          "card_id" -> JsNumber(stateId)
          )

        // Update the parameter object for "select_district"
        updatedParam ++ Json.obj(
          "values_source_config" -> updatedValuesSourceConfig
        )
      } else {
        // Return the parameter as-is for other slugs
        updatedParam
      }
    }

    println(s"updatedParameterJson = $updatedParameterJson")

    // Fetch existing dashboard details using the dashboardId
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)

    // Parse the current parameters of the dashboard
    val currentParametersJson = (Json.parse(dashboardResponse) \ "parameters").as[JsArray]

    // Combine current parameters with the updated "select_district" parameter
    val finalParametersJson = Json.toJson(
      currentParametersJson.value.filterNot(param =>
        (param \ "slug").asOpt[String].contains("select_district")
      ) ++ updatedParameterJson
    )

    // Prepare the update payload with the new parameters
    val updatePayload = Json.obj("parameters" -> finalParametersJson)

    // Update the dashboard with the new parameters
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString())

    // Log the successful update response
    println(s"----------------Successfully updated State dashboard parameter: $updateResponse----------------")
  }

  def UpdateAdminParameterFunction(metabaseUtil:MetabaseUtil,parameterFilePath:String,dashboardId:Int): Unit = {
    println(s"-----------Started Processing Admin dashboard parameter ------------")
    val fileContent = new String(Files.readAllBytes(Paths.get(parameterFilePath)), "UTF-8")
    val parameterJson = Json.parse(fileContent).as[JsArray]

    val updatedParameterJson = parameterJson.value.map { param =>
      val updatedParam = param.as[JsObject]
      updatedParam
    }

    // Fetch existing dashboard details
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)

    // Parse current parameters
    val currentParametersJson = (Json.parse(dashboardResponse) \ "parameters").as[JsArray]

    // Combine current parameters with the updated ones
    val finalParametersJson = Json.toJson(currentParametersJson.value ++ updatedParameterJson)

    // Update the dashboard with new parameters
    val updatePayload = Json.obj("parameters" -> finalParametersJson)

    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId , updatePayload.toString())

    println(s"----------------successfullly updated Admin dashboard parameter $updateResponse----------------")
  }

  def UpdateProgramParameterFunction(metabaseUtil:MetabaseUtil,parameterFilePath:String,dashboardId:Int,programName:String,programId:Int): Unit = {
    println(s"-----------Started Processing Program dashboard parameter ------------")
    val fileContent = Source.fromFile(parameterFilePath).getLines().mkString
    val parameterJson = Json.parse(fileContent).as[JsArray]

    // Update parameters with new 'card_id' for "select_district"
    val updatedParameterJson = parameterJson.value.map { param =>
      val updatedParam = param.as[JsObject]
      if ((updatedParam \ "slug").as[String] == "select_state") {
        // Update only the card_id in values_source_config
        val updatedValuesSourceConfig = (updatedParam \ "values_source_config").as[JsObject] + (
          "card_id" -> JsNumber(programId)
          )

        // Update the parameter object for "select_district"
        updatedParam ++ Json.obj(
          "values_source_config" -> updatedValuesSourceConfig
        )
      } else {
        // Return the parameter as-is for other slugs
        updatedParam
      }
    }

//    println(s"updatedParameterJson = $updatedParameterJson")

    // Fetch existing dashboard details using the dashboardId
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)

    // Parse the current parameters of the dashboard
    val currentParametersJson = (Json.parse(dashboardResponse) \ "parameters").as[JsArray]

    // Combine current parameters with the updated "select_district" parameter
    val finalParametersJson = Json.toJson(
      currentParametersJson.value.filterNot(param =>
        (param \ "slug").asOpt[String].contains("select_state")
      ) ++ updatedParameterJson
    )

    // Prepare the update payload with the new parameters
    val updatePayload = Json.obj("parameters" -> finalParametersJson)

    // Update the dashboard with the new parameters
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString())

    println(s"----------------successfullly updated Program dashboard parameter $updateResponse----------------")
  }

  def UpdateDistrictParameterFunction(metabaseUtil:MetabaseUtil,parameterFilePath:String,dashboardId:Int,stateName:String,districtName:String,districtId:Int): Unit = {
    println(s"-----------Started Processing District dashboard parameter ------------")
    val fileContent = Source.fromFile(parameterFilePath).getLines().mkString
    val parameterJson = Json.parse(fileContent).as[JsArray]

    // Update parameters with new 'card_id' for "select_district"
    val updatedParameterJson = parameterJson.value.map { param =>
      val updatedParam = param.as[JsObject]
      if ((updatedParam \ "slug").as[String] == "select_program_name") {
        // Update only the card_id in values_source_config
        val updatedValuesSourceConfig = (updatedParam \ "values_source_config").as[JsObject] + (
          "card_id" -> JsNumber(districtId)
          )

        // Update the parameter object for "select_district"
        updatedParam ++ Json.obj(
          "values_source_config" -> updatedValuesSourceConfig
        )
      } else {
        // Return the parameter as-is for other slugs
        updatedParam
      }
    }

//    println(s"updatedParameterJson = $updatedParameterJson")

    // Fetch existing dashboard details using the dashboardId
    val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)

    // Parse the current parameters of the dashboard
    val currentParametersJson = (Json.parse(dashboardResponse) \ "parameters").as[JsArray]

    // Combine current parameters with the updated "select_district" parameter
    val finalParametersJson = Json.toJson(
      currentParametersJson.value.filterNot(param =>
        (param \ "slug").asOpt[String].contains("select_program_name")
      ) ++ updatedParameterJson
    )

    // Prepare the update payload with the new parameters
    val updatePayload = Json.obj("parameters" -> finalParametersJson)

    // Update the dashboard with the new parameters
    val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, updatePayload.toString())

    println(s"----------------successfullly updated District dashboard parameter $updateResponse----------------")
  }

}
