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
  def UpdateStateParameterFunction(metabaseUtil:MetabaseUtil,parameterFilePath:String,dashboardId:Int,stateName:String): Unit = {
    val fileContent = new String(Files.readAllBytes(Paths.get(parameterFilePath)), "UTF-8")
    val parameterJson = Json.parse(fileContent).as[JsArray]

    val updatedParameterJson = parameterJson.value.map { param =>
      val updatedParam = param.as[JsObject]
      if ((updatedParam \ "slug").as[String] == "select_state") {
        updatedParam ++ Json.obj(
          "default" -> Json.arr(stateName),
          "values_source_config" -> Json.obj("values" -> Json.arr(stateName))
        )
      } else {
        updatedParam
      }
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

    println(s"----------------successfullly updated State dashboard parameter $updateResponse----------------")
  }

  def UpdateAdminParameterFunction(metabaseUtil:MetabaseUtil,parameterFilePath:String,dashboardId:Int): Unit = {
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

  def UpdateProgramParameterFunction(metabaseUtil:MetabaseUtil,parameterFilePath:String,dashboardId:Int,programName:String): Unit = {
    val fileContent = new String(Files.readAllBytes(Paths.get(parameterFilePath)), "UTF-8")
    val parameterJson = Json.parse(fileContent).as[JsArray]

    val updatedParameterJson = parameterJson.value.map { param =>
      val updatedParam = param.as[JsObject]
      if ((updatedParam \ "slug").as[String] == "select_program_name") {
        updatedParam ++ Json.obj(
          "default" -> Json.arr(programName),
          "values_source_config" -> Json.obj("values" -> Json.arr(programName))
        )
      } else {
        updatedParam
      }
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

    println(s"----------------successfullly updated Program dashboard parameter $updateResponse----------------")
  }

  def UpdateDistrictParameterFunction(metabaseUtil:MetabaseUtil,parameterFilePath:String,dashboardId:Int,stateName:String,districtName:String): Unit = {
    val fileContent = new String(Files.readAllBytes(Paths.get(parameterFilePath)), "UTF-8")
    val parameterJson = Json.parse(fileContent).as[JsArray]

    val updatedParameterJson = parameterJson.value.map { param =>
      val updatedParam = param.as[JsObject]

      // Update state parameter
      val updatedStateParam = if ((updatedParam \ "slug").as[String] == "select_state") {
        updatedParam ++ Json.obj(
          "default" -> Json.arr(stateName),
          "values_source_config" -> Json.obj("values" -> Json.arr(stateName))
        )
      } else {
        updatedParam
      }

      // Update district parameter
      val updatedDistrictParam = if ((updatedStateParam \ "slug").as[String] == "select_district") {
        updatedStateParam ++ Json.obj(
          "default" -> Json.arr(districtName),
          "values_source_config" -> Json.obj("values" -> Json.arr(districtName))
        )
      } else {
        updatedStateParam
      }

      updatedDistrictParam
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

    println(s"----------------successfullly updated Program dashboard parameter $updateResponse----------------")
  }

}
