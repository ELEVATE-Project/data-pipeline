package org.shikshalokam.job.mitra.stream.processor.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.shikshalokam.job.mitra.stream.processor.domain.DiscussionEvent
import org.shikshalokam.job.mitra.stream.processor.task.MitraStreamConfig
import org.shikshalokam.job.mitra.stream.processor.utils.{ClassifiedItem, ThematicAnalyzer}
import org.shikshalokam.job.util.PostgresUtil
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.collection.immutable._
import java.sql.SQLException

class DiscussionStreamFunction(config: MitraStreamConfig) extends RichAsyncFunction[DiscussionEvent, DiscussionEvent] {

  private[this] val logger = LoggerFactory.getLogger(classOf[DiscussionStreamFunction])
  private var thematicAnalyzer: ThematicAnalyzer = _
  private var thematicPrompt: String = _

  // Configuration: How long before retrying stuck "processing" status (in seconds)
  private val PROCESSING_TIMEOUT_SECONDS = 3600 // 1 hour

  @transient implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(30))
  @transient var postgresUtil: PostgresUtil = _

  override def open(parameters: Configuration): Unit = {
    val connectionUrl = s"jdbc:postgresql://${config.pgHost}:${config.pgPort}/${config.pgDataBase}"
    postgresUtil = new PostgresUtil(connectionUrl, config.pgUsername, config.pgPassword)
    thematicAnalyzer = new ThematicAnalyzer(config)
    thematicPrompt = postgresUtil.fetchData(config.thematicAnalyzerQuery).collectFirst { case map: Map[_, _] => map.get("content").map(_.toString).orNull }.orNull
  }

  override def asyncInvoke(event: DiscussionEvent, resultFuture: ResultFuture[DiscussionEvent]): Unit = {

    val logPrefix = s"[DiscussionID: ${event.id}]"
    println(s"$logPrefix Starting asynchronous thematic analysis.") //logger.info

    val id = event.id
    val title = event.title
    val discussionDate = event.discussionDate
    val role = event.role
    val district = event.district
    val state = event.state

    // SCENARIO 1: Check if already completed OR currently being processed
    val discussionStatus = checkDiscussionStatusWithTimeout(id)

    if (discussionStatus == "completed") {
      println(s"$logPrefix This discussion is already processed successfully. Stopping.") //logger.warn
      resultFuture.complete(Iterable.empty)
      return
    }

    if (discussionStatus == "processing") {
      println(s"$logPrefix This discussion is currently being processed by another thread. Stopping.") //logger.warn
      resultFuture.complete(Iterable.empty)
      return
    }

    if (event.challenges == null || event.challenges.trim.isEmpty) {
      println(s"$logPrefix Skipping: No challenges found.") //logger.warn
      resultFuture.complete(Iterable.empty)
      return
    }

    // SCENARIO 2: Atomic insert with proper conflict handling
    val insertSuccessful = try {
      val insertQuery =
        s"""INSERT INTO ${config.discussionsTable}
           |(id, title, discussion_date, role, district, state, status, updated_at)
           |VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
           |ON CONFLICT (id)
           |DO UPDATE SET
           |  title = EXCLUDED.title,
           |  discussion_date = EXCLUDED.discussion_date,
           |  role = EXCLUDED.role,
           |  district = EXCLUDED.district,
           |  state = EXCLUDED.state,
           |  status = CASE
           |    WHEN ${config.discussionsTable}.status IN ('completed', 'processing') THEN ${config.discussionsTable}.status
           |    ELSE EXCLUDED.status
           |  END,
           |  updated_at = NOW()""".stripMargin

      val params = Seq(id, title, discussionDate, role, district, state, "processing")

      postgresUtil.executePreparedUpdate(insertQuery, params, config.discussionsTable, id.toString)

      // Verify we actually got the processing status with a separate query
      val currentStatus = checkDiscussionStatus(id)
      if (currentStatus == "processing") {
        println(s"$logPrefix Successfully acquired processing lock") //logger.info
        true
      } else {
        println(s"$logPrefix Another thread acquired the lock, status is: $currentStatus") //logger.warn
        false
      }
    } catch {
      case e: Exception =>
        println(s"$logPrefix Failed to insert discussion: ${e.getMessage}") //logger.error
        e.printStackTrace()
        false
    }

    if (!insertSuccessful) {
      println(s"$logPrefix Unable to acquire processing lock. Stopping.") //logger.warn
      resultFuture.complete(Iterable.empty)
      return
    }

    val future: Future[DiscussionEvent] = Future {
      try {
        val challenges = cleanChallenges(event.challenges)
        println(s"$logPrefix Cleaned challenges count: ${challenges.size}") //logger.debug
        val modelResponse = thematicAnalyzer.analyzeThematicChallenge(challenges, thematicPrompt)

        modelResponse match {
          case Right(response) =>
            println(s"$logPrefix LLM Call Success | Total Challenge Sentences: ${challenges.size}  | Total Classified Object: ${response.classified_data.size}") //logger.info
            println(s"$logPrefix LLM Classified Response: ${response}") //logger.debug

            event.thematicResult = response

            // SCENARIO 3: Insert voices with proper error handling and rollback
            try {
              val voiceInsertCount = insertVoicesDataWithValidation(id, response.classified_data, logPrefix)

              // SCENARIO 4: Critical status update - if this fails, we need to know
              val statusUpdateSuccess = updateDiscussionStatusWithVerification(id, "completed", None)

              if (!statusUpdateSuccess) {
                println(s"$logPrefix CRITICAL: Voices inserted but status update failed. Manual intervention may be required.") //logger.warn
                // Don't throw - voices are inserted successfully
              } else {
                println(s"$logPrefix Successfully completed processing with $voiceInsertCount voices") //logger.info
              }
            } catch {
              case e: SQLException if e.getMessage.contains("foreign key") =>
                println(s"$logPrefix Foreign key constraint violation - invalid theme_id: ${e.getMessage}") //logger.error
                updateDiscussionStatusWithVerification(id, "failed", Some(s"Invalid theme_id in LLM response: ${e.getMessage}"))
                throw e
              case e: Exception =>
                println(s"$logPrefix Voice insertion failed: ${e.getMessage}") //logger.error
                e.printStackTrace()

                // SCENARIO 5: Clean up partial inserts
                cleanupPartialVoiceInserts(id, logPrefix)

                updateDiscussionStatusWithVerification(id, "failed", Some(s"Voice insertion failed: ${e.getMessage}"))
                throw e
            }

            event

          case Left(err) =>
            println(s"$logPrefix LLM Failure: ${err.error}") //logger.error
            updateDiscussionStatusWithVerification(id, "failed", Some(err.error))
            event
        }
      } catch {
        case e: Exception =>
          println(s"$logPrefix Unexpected error in Future: ${e.getMessage}") //logger.error
          e.printStackTrace()
          updateDiscussionStatusWithVerification(id, "failed", Some(s"Unexpected error: ${e.getMessage}"))
          throw e
      }
    }

    future.onComplete {
      case Success(res) =>
        try {
          resultFuture.complete(List(res))
        } catch {
          case _: java.util.concurrent.RejectedExecutionException =>
            println(s"$logPrefix Result delivered (mailbox closed during shutdown)")
          case e: Exception =>
            println(s"$logPrefix Error completing result: ${e.getMessage}")
            e.printStackTrace()
        }

      case Failure(ex) =>
        println(s"$logPrefix Processing failed in onComplete: ${ex.getMessage}")
        ex.printStackTrace()
        updateDiscussionStatusWithVerification(id, "failed", Some(ex.getMessage))

        try {
          resultFuture.completeExceptionally(ex)
        } catch {
          case _: java.util.concurrent.RejectedExecutionException =>
            println(s"$logPrefix Failed to report error (mailbox closed)")
          case e: Exception =>
            println(s"$logPrefix Error reporting failure: ${e.getMessage}")
        }
    }

  }

  /**
   * SCENARIO 3: Check status with timeout for stuck "processing" records
   */
  private def checkDiscussionStatusWithTimeout(discussionId: Int): String = {
    try {
      val result = postgresUtil.fetchData(
        s"""SELECT status, EXTRACT(EPOCH FROM (NOW() - updated_at)) as seconds_since_update
           |FROM ${config.discussionsTable}
           |WHERE id = $discussionId""".stripMargin
      )

      result.headOption match {
        case Some(map: Map[_, _]) =>
          val status = map.getOrElse("status", "not_found").toString
          val secondsSinceUpdate = map.get("seconds_since_update") match {
            case Some(seconds: Number) => seconds.doubleValue()
            case _ => 0.0
          }

          // If stuck in "processing" for too long, treat as retriable
          if (status == "processing" && secondsSinceUpdate > PROCESSING_TIMEOUT_SECONDS) {
            println(s"[DiscussionID: $discussionId] Found stale processing status (${secondsSinceUpdate}s old). Marking for retry.") //logger.warn
            "failed" // Treat as failed so it can be retried
          } else {
            status
          }
        case _ => "not_found"
      }
    } catch {
      case e: Exception =>
        println(s"[DiscussionID: $discussionId] Error checking status: ${e.getMessage}") //logger.error
        "not_found"
    }
  }

  private def checkDiscussionStatus(discussionId: Int): String = {
    try {
      val result = postgresUtil.fetchData(s"SELECT status FROM ${config.discussionsTable} WHERE id = $discussionId")
      result.headOption match {
        case Some(map: Map[_, _]) => map.getOrElse("status", "not_found").toString
        case _ => "not_found"
      }
    } catch {
      case e: Exception =>
        println(s"[DiscussionID: $discussionId] Error checking status: ${e.getMessage}") //logger.error
        "not_found"
    }
  }

  /**
   * SCENARIO 4: Update status with verification
   */
  private def updateDiscussionStatusWithVerification(discussionId: Int, status: String, errorMessage: Option[String]): Boolean = {
    try {
      val updateQuery = errorMessage match {
        case Some(_) =>
          s"""UPDATE ${config.discussionsTable}
             |SET status = ?, error_message = ?, updated_at = NOW()
             |WHERE id = ?""".stripMargin
        case None =>
          s"""UPDATE ${config.discussionsTable}
             |SET status = ?, updated_at = NOW()
             |WHERE id = ?""".stripMargin
      }

      val params = errorMessage match {
        case Some(msg) => Seq(status, msg, discussionId)
        case None => Seq(status, discussionId)
      }

      postgresUtil.executePreparedUpdate(updateQuery, params, config.discussionsTable, discussionId.toString)

      // Verify the update
      val verifyStatus = checkDiscussionStatus(discussionId)
      val success = verifyStatus == status

      if (success) {
        println(s"[DiscussionID: $discussionId] Status successfully updated to: $status") //logger.info
      } else {
        println(s"[DiscussionID: $discussionId] Status update verification failed. Expected: $status, Got: $verifyStatus") //logger.warn
      }

      success
    } catch {
      case e: Exception =>
        println(s"[DiscussionID: $discussionId] Failed to update status: ${e.getMessage}") //logger.error
        e.printStackTrace()
        false
    }
  }

  private def cleanChallenges(challenges: String): List[String] = {
    val withoutBrackets = challenges.trim.stripPrefix("[").stripSuffix("]")
    val sentences = withoutBrackets.split("\\|")
    sentences.flatMap { sentence =>
      val trimmed = sentence.trim
      val cleaned = trimmed.replaceAll("^\\d+\\.\\s*", "")
      if (cleaned.nonEmpty) Some(cleaned) else None
    }.toList
  }

  /**
   * SCENARIO 5: Insert voices with validation and proper error handling
   */
  private def insertVoicesDataWithValidation(discussionId: Int, classifiedData: List[ClassifiedItem], logPrefix: String): Int = {
    if (classifiedData.isEmpty) {
      println(s"$logPrefix No classified data to insert")
      return 0
    }

    var insertedCount = 0
    try {
      classifiedData.foreach { item =>
        // Validate theme_id exists (optional, depends on your setup)
        // val themeExists = validateThemeId(item.theme_id)
        // if (!themeExists) throw new SQLException(s"Invalid theme_id: ${item.theme_id}")

        val insertVoiceQuery =
          s"""INSERT INTO ${config.voicesTable}
             |(discussion_id, theme_id, challenge, pii_flag, confidence_score, justification, multi_theme_mapped)
             |VALUES (?, ?, ?, ?, ?, ?, ?)""".stripMargin

        val voiceParams = Seq(
          discussionId,
          item.theme_id,
          item.challenge,
          item.pii_flag,
          BigDecimal(item.confidence_score),
          item.justification,
          item.multi_theme_mapped
        )

        postgresUtil.executePreparedUpdate(insertVoiceQuery, voiceParams, config.voicesTable, discussionId.toString)
        insertedCount += 1
      }
      println(s"$logPrefix Successfully inserted $insertedCount records into voices table") //logger.info
      insertedCount
    } catch {
      case e: Exception =>
        println(s"$logPrefix Error inserting voices data after $insertedCount successful inserts: ${e.getMessage}") //logger.error
        e.printStackTrace()
        throw e
    }
  }

  /**
   * SCENARIO 5: Cleanup partial voice inserts on failure
   */
  private def cleanupPartialVoiceInserts(discussionId: Int, logPrefix: String): Unit = {
    try {
      val deleteQuery = s"DELETE FROM ${config.voicesTable} WHERE discussion_id = ?"
      postgresUtil.executePreparedDelete(deleteQuery, Seq(discussionId), config.voicesTable, discussionId.toString)
      println(s"$logPrefix Cleaned up partial voice inserts for discussion") //logger.info
    } catch {
      case e: Exception =>
        println(s"$logPrefix WARNING: Failed to cleanup partial voice inserts: ${e.getMessage}") //logger.error
      // Don't throw - this is cleanup, not critical
    }
  }

  override def close(): Unit = {
    super.close()
    if (thematicAnalyzer != null) thematicAnalyzer.close()
  }
}
