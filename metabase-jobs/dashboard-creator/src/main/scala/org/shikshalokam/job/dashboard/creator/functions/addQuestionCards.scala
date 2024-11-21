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
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import java.io.{File, IOException}
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

object addQuestionCards {

  def addQuestionCardsFunction(metabaseUtil: MetabaseUtil, mainDir: String, dashboardId: Int): Unit = {
    println(s"---------------Started processing AddQuestionCardsFunction---------------")
    val objectMapper = new ObjectMapper()

    def appendDashCardToDashboard(mainDir: String, dashboardId: Int): Unit = {
      val mainDirectory = new File(mainDir)

      // Step 7: Get the Dashboard response
      val dashboardResponse = metabaseUtil.getDashboardDetailsById(dashboardId)

      // Step 8: Parse existing dashcards
      val dashboardJson = objectMapper.readTree(dashboardResponse)
      val existingDashcards = dashboardJson.path("dashcards") match {
        case array: ArrayNode => array
        case _ => objectMapper.createArrayNode()
      }
      println(s"existingDashcards: $existingDashcards")

      // Step 1: Validate main directory
      if (mainDirectory.exists() && mainDirectory.isDirectory) {
        val dirs = mainDirectory.listFiles().filter(_.isDirectory)

        dirs.foreach { dir =>
          println(s"Processing directory: ${dir.getName}")

          // Step 3: Check for "json" subdirectory
          val jsonDir = new File(dir, "json")
          if (jsonDir.exists() && jsonDir.isDirectory) {
            val subDirs = jsonDir.listFiles().filter(_.isDirectory)

            subDirs.foreach { subDir =>
              println(s"Processing subdirectory: ${subDir.getName}")

              val jsonFiles = subDir.listFiles().filter(_.getName.endsWith(".json"))
              jsonFiles.foreach { jsonFile =>
                println(s"Reading JSON file: ${jsonFile.getName}")

                // Step 6: Extract "dashCards" key from the JSON file
                val dashCardsNode = readJsonFile(jsonFile)
                dashCardsNode.foreach { value =>
                  println(s"Extracted dashCards: $value")
                  existingDashcards.add(value)
                }
              }
            }
          }
        }

        // Step 9: Update the dashboard with the new dashcards
        val finalDashboardJson = objectMapper.createObjectNode()
        finalDashboardJson.set("dashcards", existingDashcards)
        val dashcardsString = objectMapper.writeValueAsString(finalDashboardJson)
        println(s"finalDashcards: $dashcardsString")

        val updateResponse = metabaseUtil.addQuestionCardToDashboard(dashboardId, dashcardsString)
        println(s"********************* Successfully updated Dashcard: $updateResponse *********************")
      } else {
        println(s"$mainDir is not a valid directory.")
      }
    }

    // Helper method to read JSON file and extract "dashCards"
    def readJsonFile(file: File): Option[JsonNode] = {
      Try {
        val rootNode = objectMapper.readTree(file)
        rootNode.path("dashCards") match {
          case value if !value.isMissingNode =>
            println(s"Successfully extracted 'dashCards' key: $value")
            Some(value)
          case _ =>
            println(s"'dashCards' key not found in file: ${file.getName}")
            None
        }
      } match {
        case Success(value) => value
        case Failure(exception) =>
          println(s"Error reading or parsing JSON file ${file.getName}: ${exception.getMessage}")
          None
      }
    }

    appendDashCardToDashboard(mainDir, dashboardId)
    println(s"---------------Processed AddQuestionCardsFunction---------------")
  }
}
