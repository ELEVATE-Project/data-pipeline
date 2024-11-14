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
import scala.collection.mutable.ListBuffer


object addQuestionCards {
  def AddQuestionCardsFunction(metabaseUtil:MetabaseUtil,mainDir:String,dashboardId:Int): Unit = {
    println(s"---------------Started processing AddQuestionCardsFunction function---------------")
    def appendDashCardToDashboard(mainDir: String, dashboardId: Int): Unit = {
      val mainDirectory = new File(mainDir)
      // Step 7: Get the Dashboard response
      val DashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)

      // Step 8: Extract the existing dashcards
      val DashboardJson = Json.parse(DashboardResponse)
      var existingDashcards = (DashboardJson \ "dashcards").asOpt[JsArray] match {
        case Some(dashcards) => dashcards
        case None => JsArray()
      }
      println(s"existingDashcards = $existingDashcards")
      // Step 1: Check if the main directory exists and is a directory
      if (mainDirectory.exists() && mainDirectory.isDirectory) {
        // Step 2: Get all directories inside the main directory
        val dirs = mainDirectory.listFiles().filter(_.isDirectory)

        // Loop through each directory inside the main directory
        dirs.foreach { dir =>
          println(s"Processing directory: ${dir.getName}")

          // Step 3: Look for "json" sub-directory
          val jsonDir = new File(dir, "json")
          if (jsonDir.exists() && jsonDir.isDirectory) {
            // Step 4: Get all directories inside the "json" directory
            val subDirs = jsonDir.listFiles().filter(_.isDirectory)

            // Loop through each subdirectory
            subDirs.foreach { subDir =>
              println(s"  Processing subdirectory: ${subDir.getName}")

              // Step 5: Loop through each JSON file inside the subdirectory
              val jsonFiles = subDir.listFiles().filter(_.getName.endsWith(".json"))
              jsonFiles.foreach { jsonFile =>
                println(s"Reading JSON file: ${jsonFile.getName}")

                // Step 6: Read JSON file and fetch the value of the "dashboard" key
                val dashboardValue = readJsonFile(jsonFile)
                println(s"dashboardValue : $dashboardValue")
                dashboardValue match {
                  case Some(value) =>
                    // If a valid dashboard value exists, append it to the existingDashcards (as JsObject or JsArray)
                    val newCard = Json.obj("dashCards" -> value)
                    val dashCards: JsValue = (newCard \ "dashCards").get
                    println(s"dashCards = $dashCards")
                    existingDashcards = existingDashcards :+ dashCards
                    println(s"existingDashcards = $existingDashcards")
                  case None =>
                    println("dashCards key not found in the JSON.")
                }
              }
            }
          }
        }
        // Convert JsArray to String
        val finalDashcards: JsObject = Json.obj("dashcards" -> existingDashcards)
        println(s"finalDashcards = $finalDashcards")
        val DashcardString: String = Json.stringify(finalDashcards)
        val UpdateDashcards = metabaseUtil.addQuestionCardToDashboard(dashboardId,DashcardString)
        println(s"********************* successfully updated Dashcard : $UpdateDashcards  *********************")
      } else {
        println(s"$mainDir is not a valid directory.")
      }

      def readJsonFile(file: File): Option[JsValue] = {
        Try {
          val source = Source.fromFile(file)
          val content = try source.mkString finally source.close()
          //              println(s"File content from ${file.getName}: $content")
          Json.parse(content) \ "dashCards" // Extract the "dashboard" key
        } match {
          case Success(JsDefined(value)) =>
            println(s"Successfully extracted 'dashCards' key: $value")
            Some(value)
          case Success(JsUndefined()) =>
            println(s"'dashCards' key not found in file: ${file.getName}")
            None
          case Failure(exception) =>
            println(s"Error reading or parsing JSON file ${file.getName}: ${exception.getMessage}")
            None
        }
      }
    }
    appendDashCardToDashboard(mainDir,dashboardId)
    println(s"---------------Processed AddQuestionCardsFunction function---------------")
  }
}
