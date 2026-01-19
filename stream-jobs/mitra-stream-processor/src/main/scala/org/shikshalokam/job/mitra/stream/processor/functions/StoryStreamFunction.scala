package org.shikshalokam.job.mitra.stream.processor.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.shikshalokam.job.mitra.stream.processor.domain.StoryEvent
import org.shikshalokam.job.mitra.stream.processor.task.MitraStreamConfig
import org.shikshalokam.job.mitra.stream.processor.utils._
import org.shikshalokam.job.util.PostgresUtil
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class StoryStreamFunction(config: MitraStreamConfig) extends RichAsyncFunction[StoryEvent, StoryEvent] {

  private[this] val logger = LoggerFactory.getLogger(classOf[StoryStreamFunction])
  private var piiAnalyzer: PIIAnalyzer = _
  private var storyAnalyzer: StoryAnalyzer = _
  // private var imageProcessor: ImageProcessor = _
  // private var cloudUploader: CloudStorageUploader = _
  private var piiPrompt: String = _
  private var storyPrompt: String = _

  // Configuration: How long before retrying stuck "processing" status (in seconds)
  private val PROCESSING_TIMEOUT_SECONDS = 3600 // 1 hour

  @transient implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(
    java.util.concurrent.Executors.newFixedThreadPool(10)
  )
  @transient var postgresUtil: PostgresUtil = _

  override def open(parameters: Configuration): Unit = {
    val connectionUrl = s"jdbc:postgresql://${config.pgHost}:${config.pgPort}/${config.pgDataBase}"
    postgresUtil = new PostgresUtil(connectionUrl, config.pgUsername, config.pgPassword)

    // Initialize analyzers
    piiAnalyzer = new PIIAnalyzer(config)
    storyAnalyzer = new StoryAnalyzer(config)
    // imageProcessor = new ImageProcessor()
    // cloudUploader = new CloudStorageUploader(config)

    // Fetch prompts from database
    piiPrompt = postgresUtil.fetchData(config.piiAnalyzerQuery).collectFirst { case map: Map[_, _] => map.get("content").map(_.toString).orNull }.orNull
    storyPrompt = postgresUtil.fetchData(config.storyAnalyzerQuery).collectFirst { case map: Map[_, _] => map.get("content").map(_.toString).orNull }.orNull
  }

  override def asyncInvoke(event: StoryEvent, resultFuture: ResultFuture[StoryEvent]): Unit = {
    val logPrefix = s"[StoryID: ${event.id}]"
    println(s"$logPrefix Starting asynchronous story processing.")

    val id = event.id
    val title = event.title
    val content = event.content
    val pdfLink = event.pdfLink
    val imageLinks = event.imageLinks
    val role = event.role
    val district = event.district
    val state = event.state
    val actionSteps = event.actionSteps
    val impact = event.impact
    val challenges = event.challenges
    val maskedContent = event.maskedContent

    // PARALLEL PROCESSING: Launch both PII and Story analysis independently
    val piiFuture = Future {
      processPIIAnalysis(id, title, actionSteps, impact, role, district, state, logPrefix)
    }

    val storyFuture = Future {
      processStoryAnalysis(id, title, challenges, actionSteps, impact, maskedContent, pdfLink, imageLinks, role, district, state, logPrefix)
    }

    // Combine both futures
    val combinedFuture = for {
      piiResult <- piiFuture.recover { case e =>
        println(s"$logPrefix PII analysis failed: ${e.getMessage}")
        false
      }
      storyResult <- storyFuture.recover { case e =>
        println(s"$logPrefix Story analysis failed: ${e.getMessage}")
        false
      }
    } yield (piiResult, storyResult)

    combinedFuture.onComplete {
      case Success((piiSuccess, storySuccess)) =>
        println(s"$logPrefix Processing completed - PII: $piiSuccess, Story: $storySuccess")
        try {
          resultFuture.complete(List(event))
        } catch {
          case _: java.util.concurrent.RejectedExecutionException =>
            println(s"$logPrefix Result delivered (mailbox closed during shutdown)")
          case e: Exception =>
            println(s"$logPrefix Error completing result: ${e.getMessage}")
            e.printStackTrace()
        }
      case Failure(ex) =>
        println(s"$logPrefix Processing failed: ${ex.getMessage}")
        ex.printStackTrace()
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
   * Process PII Analysis for action_steps and impact fields
   */
  private def processPIIAnalysis(storyId: Int, title: String, actionSteps: String, impact: String, role: String, district: String, state: String, logPrefix: String): Boolean = {
    try {
      println(s"$logPrefix Starting PII analysis...")

      // SCENARIO 1: Check if already completed OR currently being processed
      val feedStatus = checkFeedStatusWithTimeout(storyId)

      if (feedStatus == "completed") {
        println(s"$logPrefix PII analysis already completed. Skipping.")
        return true
      }

      if (feedStatus == "processing") {
        println(s"$logPrefix PII analysis currently being processed. Skipping.")
        return false
      }

      // Validate input fields
      if ((actionSteps == null || actionSteps.trim.isEmpty) && (impact == null || impact.trim.isEmpty)) {
        println(s"$logPrefix Skipping PII analysis: Both action_steps and impact are empty.")
        return true // Not an error, just nothing to process
      }

      // SCENARIO 2: Atomic insert with proper conflict handling
      val insertSuccessful = insertStoryMetaIfNotExists(storyId, title, role, district, state, "feed", logPrefix)

      if (!insertSuccessful) {
        println(s"$logPrefix Unable to acquire PII processing lock. Skipping.")
        return false
      }

      // Combine action_steps and impact for analysis
      val combinedText = List(
        Option(actionSteps).filter(_.trim.nonEmpty).map(s => s"Action Steps: $s"),
        Option(impact).filter(_.trim.nonEmpty).map(s => s"Impact: $s")
      ).flatten.mkString("\n\n")

      if (combinedText.trim.isEmpty) {
        println(s"$logPrefix No content to analyze for PII.")
        updateFeedStatusWithVerification(storyId, "completed", None)
        return true
      }

      // Call LLM for PII analysis
      val piiResponse = piiAnalyzer.analyzePII(combinedText, piiPrompt)

      piiResponse match {
        case Right(result) =>
          println(s"$logPrefix PII Analysis Success - PII Flag: ${result.pii_flag}")

          // Insert into feeds table
          insertFeedData(storyId, actionSteps, impact, result, logPrefix)

          // Update status to completed
          updateFeedStatusWithVerification(storyId, "completed", None)
          true

        case Left(err) =>
          println(s"$logPrefix PII Analysis Failed: ${err.error}")
          updateFeedStatusWithVerification(storyId, "failed", Some(err.error))
          false
      }
    } catch {
      case e: Exception =>
        println(s"$logPrefix PII analysis error: ${e.getMessage}")
        e.printStackTrace()
        updateFeedStatusWithVerification(storyId, "failed", Some(e.getMessage))
        false
    }
  }

  /**
   * Process Story Analysis (PDF ranking) and Image Blurring
   */
  private def processStoryAnalysis(storyId: Int, title: String, challenges: String, actionSteps: String, impact: String, maskedContent: String, pdfLink: String, imageLinks: String, role: String, district: String, state: String, logPrefix: String): Boolean = {

    try {
      println(s"$logPrefix Starting story analysis...")

      val extractedPdfPath = extractPaths(pdfLink, config.baseUrl, logPrefix)
      val extractedImagePath = extractPaths(imageLinks, config.baseUrl, logPrefix)

      // SCENARIO 1: Check if already completed OR currently being processed
      val storyStatus = checkStoryStatusWithTimeout(storyId)

      if (storyStatus == "completed") {
        println(s"$logPrefix Story analysis already completed. Skipping.")
        return true
      }

      if (storyStatus == "processing") {
        println(s"$logPrefix Story analysis currently being processed. Skipping.")
        return false
      }

      // Validate PDF link
      if (pdfLink == null || pdfLink.trim.isEmpty) {
        println(s"$logPrefix Skipping story analysis: PDF link is missing.")
        return true // Not an error, just nothing to process
      }

      // SCENARIO 2: Atomic insert with proper conflict handling
      val insertSuccessful = insertStoryMetaIfNotExists(storyId, title, role, district, state, "story", logPrefix)

      if (!insertSuccessful) {
        println(s"$logPrefix Unable to acquire story processing lock. Skipping.")
        return false
      }

      // Call LLM for story ranking
      val storyResponse = storyAnalyzer.analyzeStory(pdfLink, challenges, actionSteps, impact, storyPrompt, logPrefix)

      storyResponse match {
        case Right(result) =>
          println(s"$logPrefix Story Analysis Success - Tier: ${result.tier}, Composite Score: ${result.composite_score}")

          // Insert into stories table
          insertStoryData(storyId, maskedContent, result, extractedPdfPath, extractedImagePath, logPrefix)

          //          // PARALLEL: Process and blur images
          //          if (imageLinks != null && imageLinks.trim.nonEmpty) {
          //            Future {
          //              processAndUploadBlurredImages(storyId, imageLinks, logPrefix)
          //            }.recover {
          //              case e: Exception =>
          //                println(s"$logPrefix Image processing failed (non-critical): ${e.getMessage}")
          //              // Don't fail the entire story processing due to image issues
          //            }
          //          }

          // Update status to completed
          updateStoryStatusWithVerification(storyId, "completed", None)
          true

        case Left(err) =>
          println(s"$logPrefix Story Analysis Failed: ${err.error}")
          updateStoryStatusWithVerification(storyId, "failed", Some(err.error))
          false
      }
    } catch {
      case e: Exception =>
        println(s"$logPrefix Story analysis error: ${e.getMessage}")
        e.printStackTrace()
        updateStoryStatusWithVerification(storyId, "failed", Some(e.getMessage))
        false
    }
  }

  /**
   * Process and upload blurred images to S3
   */
  //  private def processAndUploadBlurredImages(storyId: Int, imageLinks: String, logPrefix: String): Unit = {
  //    try {
  //      // Parse image links (assuming comma-separated URLs)
  //      val imageUrls = imageLinks.split(",").map(_.trim).filter(_.nonEmpty).toList
  //
  //      if (imageUrls.isEmpty) {
  //        println(s"$logPrefix No valid image URLs found")
  //        return
  //      }
  //
  //      println(s"$logPrefix Processing ${imageUrls.size} images for blurring...")
  //
  //      // Process images in parallel
  //      val uploadedUrls = imageUrls.zipWithIndex.par.flatMap { case (url, index) =>
  //        try {
  //          println(s"$logPrefix Processing image ${index + 1}/${imageUrls.size}: $url")
  //
  //          // Download image
  //          val imageBytes = imageProcessor.downloadImage(url)
  //
  //          // Blur faces using deface
  //          val blurredBytes = imageProcessor.blurFaces(imageBytes, index)
  //
  //          // Upload to S3
  //          val s3Key = s"stories/${storyId}/blurred_${index}_${System.currentTimeMillis()}.jpg"
  //          val s3Url = cloudUploader.upload(s3Key, blurredBytes)
  //
  //          println(s"$logPrefix Image ${index + 1} uploaded successfully to: $s3Url")
  //          Some(s3Url)
  //        } catch {
  //          case e: Exception =>
  //            println(s"$logPrefix Failed to process image ${index + 1}: ${e.getMessage}")
  //            None
  //        }
  //      }.toList.flatten
  //
  //      // Update stories table with blurred image URLs
  //      if (uploadedUrls.nonEmpty) {
  //        val blurredImageLinks = uploadedUrls.mkString(",")
  //        updateStoryImageLinks(storyId, blurredImageLinks, logPrefix)
  //        println(s"$logPrefix Successfully processed ${uploadedUrls.size}/${imageUrls.size} images")
  //      }
  //    } catch {
  //      case e: Exception =>
  //        println(s"$logPrefix Image processing error: ${e.getMessage}")
  //        e.printStackTrace()
  //    }
  //  }

  /**
   * Insert or update stories_meta table with proper conflict handling
   */
  private def insertStoryMetaIfNotExists(storyId: Int, title: String, role: String, district: String, state: String, processingType: String, logPrefix: String): Boolean = {
    try {
      val (statusField, otherStatusField) = if (processingType == "feed") {
        ("feed_status", "story_status")
      } else {
        ("story_status", "feed_status")
      }

      val insertQuery =
        s"""INSERT INTO ${config.storiesMetaTable}
           |(id, title, role, district, state, $statusField, updated_at)
           |VALUES (?, ?, ?, ?, ?, ?, NOW())
           |ON CONFLICT (id)
           |DO UPDATE SET
           |  title = EXCLUDED.title,
           |  role = EXCLUDED.role,
           |  district = EXCLUDED.district,
           |  state = EXCLUDED.state,
           |  $statusField = CASE
           |    WHEN ${config.storiesMetaTable}.$statusField IN ('completed', 'processing') 
           |    THEN ${config.storiesMetaTable}.$statusField
           |    ELSE EXCLUDED.$statusField
           |  END,
           |  updated_at = NOW()""".stripMargin

      val params = Seq(storyId, title, role, district, state, "processing")
      postgresUtil.executePreparedUpdate(insertQuery, params, config.storiesMetaTable, storyId.toString)

      // Verify we actually got the processing status
      val currentStatus = if (processingType == "feed") {
        checkFeedStatus(storyId)
      } else {
        checkStoryStatus(storyId)
      }

      if (currentStatus == "processing") {
        println(s"$logPrefix Successfully acquired $processingType processing lock")
        true
      } else {
        println(s"$logPrefix Another thread acquired the $processingType lock, status is: $currentStatus")
        false
      }
    } catch {
      case e: Exception =>
        println(s"$logPrefix Failed to insert story meta: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Insert feed data (PII analysis results)
   */
  private def insertFeedData(storyId: Int, actionSteps: String, impact: String, result: PIIAnalysisResult, logPrefix: String): Unit = {
    try {
      val insertQuery =
        s"""INSERT INTO ${config.feedsTable}
           |(story_id, action_steps, impact, pii_flag, justification, confidence_score)
           |VALUES (?, ?, ?, ?, ?, ?)
           |ON CONFLICT (story_id) 
           |DO UPDATE SET
           |  action_steps = EXCLUDED.action_steps,
           |  impact = EXCLUDED.impact,
           |  pii_flag = EXCLUDED.pii_flag,
           |  justification = EXCLUDED.justification,
           |  confidence_score = EXCLUDED.confidence_score""".stripMargin

      val params = Seq(
        storyId,
        actionSteps,
        impact,
        result.pii_flag,
        result.justification,
        result.confidence_score
      )

      postgresUtil.executePreparedUpdate(insertQuery, params, config.feedsTable, storyId.toString)
      println(s"$logPrefix Feed data inserted successfully")
    } catch {
      case e: Exception =>
        println(s"$logPrefix Error inserting feed data: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }

  /**
   * Insert story data (ranking results)
   */
  private def insertStoryData(storyId: Int, maskedContent: String, result: StoryRatingResult, extractedPdfPath: String, extractedImagePath: String, logPrefix: String): Unit = {
    try {
      //TODO: make the first letter of document_language caps
      val insertQuery =
        s"""INSERT INTO ${config.storiesTable}
           |(story_id, content, pdf_link, image_link,
            impact_and_outcome_score, impact_justification,
            issue_and_challenge_score, issue_justification,
            action_steps_score, action_justification,
            composite_score, document_language, tier, overall_summary)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |ON CONFLICT (story_id)
           |DO UPDATE SET
           |  content = EXCLUDED.content,
           |  pdf_link = EXCLUDED.pdf_link,
           |  image_link = EXCLUDED.image_link,
           |  impact_and_outcome_score = EXCLUDED.impact_and_outcome_score,
           |  impact_justification = EXCLUDED.impact_justification,
           |  issue_and_challenge_score = EXCLUDED.issue_and_challenge_score,
           |  issue_justification = EXCLUDED.issue_justification,
           |  action_steps_score = EXCLUDED.action_steps_score,
           |  action_justification = EXCLUDED.action_justification,
           |  composite_score = EXCLUDED.composite_score,
           |  document_language = EXCLUDED.document_language,
           |  tier = EXCLUDED.tier,
           |  overall_summary = EXCLUDED.overall_summary""".stripMargin

      val params = Seq(
        storyId,
        maskedContent,
        extractedPdfPath,
        extractedImagePath,
        BigDecimal(result.impact_and_outcome_score),
        result.impact_justification,
        BigDecimal(result.issue_and_challenge_score),
        result.issue_justification,
        BigDecimal(result.action_steps_score),
        result.action_justification,
        BigDecimal(result.composite_score),
        result.document_language,
        result.tier,
        result.overall_summary
      )

      postgresUtil.executePreparedUpdate(insertQuery, params, config.storiesTable, storyId.toString)
      println(s"$logPrefix Story data inserted successfully")
    } catch {
      case e: Exception =>
        println(s"$logPrefix Error inserting story data: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }

  /**
   * Update story table with blurred image URLs
   */
  private def updateStoryImageLinks(storyId: Int, blurredImageLinks: String, logPrefix: String): Unit = {
    try {
      val updateQuery =
        s"""UPDATE ${config.storiesTable}
           |SET image_link = ?
           |WHERE story_id = ?""".stripMargin

      val params = Seq(blurredImageLinks, storyId)
      postgresUtil.executePreparedUpdate(updateQuery, params, config.storiesTable, storyId.toString)
      println(s"$logPrefix Blurred image links updated successfully")
    } catch {
      case e: Exception =>
        println(s"$logPrefix Error updating image links: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  /**
   * Check feed_status with timeout for stuck "processing" records
   */
  private def checkFeedStatusWithTimeout(storyId: Int): String = {
    try {
      val result = postgresUtil.fetchData(
        s"""SELECT feed_status, EXTRACT(EPOCH FROM (NOW() - updated_at)) as seconds_since_update
           |FROM ${config.storiesMetaTable}
           |WHERE id = $storyId""".stripMargin
      )

      result.headOption match {
        case Some(map: Map[_, _]) =>
          val status = map.getOrElse("feed_status", "not_found").toString
          val secondsSinceUpdate = map.get("seconds_since_update") match {
            case Some(seconds: Number) => seconds.doubleValue()
            case _ => 0.0
          }

          if (status == "processing" && secondsSinceUpdate > PROCESSING_TIMEOUT_SECONDS) {
            println(s"[StoryID: $storyId] Found stale feed processing status (${secondsSinceUpdate}s old). Marking for retry.")
            "failed"
          } else {
            status
          }
        case _ => "not_found"
      }
    } catch {
      case e: Exception =>
        println(s"[StoryID: $storyId] Error checking feed status: ${e.getMessage}")
        "not_found"
    }
  }

  /**
   * Check story_status with timeout for stuck "processing" records
   */
  private def checkStoryStatusWithTimeout(storyId: Int): String = {
    try {
      val result = postgresUtil.fetchData(
        s"""SELECT story_status, EXTRACT(EPOCH FROM (NOW() - updated_at)) as seconds_since_update
           |FROM ${config.storiesMetaTable}
           |WHERE id = $storyId""".stripMargin
      )

      result.headOption match {
        case Some(map: Map[_, _]) =>
          val status = map.getOrElse("story_status", "not_found").toString
          val secondsSinceUpdate = map.get("seconds_since_update") match {
            case Some(seconds: Number) => seconds.doubleValue()
            case _ => 0.0
          }

          if (status == "processing" && secondsSinceUpdate > PROCESSING_TIMEOUT_SECONDS) {
            println(s"[StoryID: $storyId] Found stale story processing status (${secondsSinceUpdate}s old). Marking for retry.")
            "failed"
          } else {
            status
          }
        case _ => "not_found"
      }
    } catch {
      case e: Exception =>
        println(s"[StoryID: $storyId] Error checking story status: ${e.getMessage}")
        "not_found"
    }
  }

  private def checkFeedStatus(storyId: Int): String = {
    try {
      val result = postgresUtil.fetchData(s"SELECT feed_status FROM ${config.storiesMetaTable} WHERE id = $storyId")
      result.headOption match {
        case Some(map: Map[_, _]) => map.getOrElse("feed_status", "not_found").toString
        case _ => "not_found"
      }
    } catch {
      case e: Exception =>
        println(s"[StoryID: $storyId] Error checking feed status: ${e.getMessage}")
        "not_found"
    }
  }

  private def checkStoryStatus(storyId: Int): String = {
    try {
      val result = postgresUtil.fetchData(s"SELECT story_status FROM ${config.storiesMetaTable} WHERE id = $storyId")
      result.headOption match {
        case Some(map: Map[_, _]) => map.getOrElse("story_status", "not_found").toString
        case _ => "not_found"
      }
    } catch {
      case e: Exception =>
        println(s"[StoryID: $storyId] Error checking story status: ${e.getMessage}")
        "not_found"
    }
  }

  /**
   * Update feed_status with verification
   */
  private def updateFeedStatusWithVerification(storyId: Int, status: String, errorMessage: Option[String]): Boolean = {
    try {
      val updateQuery = errorMessage match {
        case Some(_) =>
          s"""UPDATE ${config.storiesMetaTable}
             |SET feed_status = ?, feed_error_message = ?, updated_at = NOW()
             |WHERE id = ?""".stripMargin
        case None =>
          s"""UPDATE ${config.storiesMetaTable}
             |SET feed_status = ?, updated_at = NOW()
             |WHERE id = ?""".stripMargin
      }

      val params = errorMessage match {
        case Some(msg) => Seq(status, msg, storyId)
        case None => Seq(status, storyId)
      }

      postgresUtil.executePreparedUpdate(updateQuery, params, config.storiesMetaTable, storyId.toString)

      val verifyStatus = checkFeedStatus(storyId)
      val success = verifyStatus == status

      if (success) {
        println(s"[StoryID: $storyId] Feed status successfully updated to: $status")
      } else {
        println(s"[StoryID: $storyId] Feed status update verification failed. Expected: $status, Got: $verifyStatus")
      }

      success
    } catch {
      case e: Exception =>
        println(s"[StoryID: $storyId] Failed to update feed status: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Update story_status with verification
   */
  private def updateStoryStatusWithVerification(storyId: Int, status: String, errorMessage: Option[String]): Boolean = {
    try {
      val updateQuery = errorMessage match {
        case Some(_) =>
          s"""UPDATE ${config.storiesMetaTable}
             |SET story_status = ?, story_error_message = ?, updated_at = NOW()
             |WHERE id = ?""".stripMargin
        case None =>
          s"""UPDATE ${config.storiesMetaTable}
             |SET story_status = ?, updated_at = NOW()
             |WHERE id = ?""".stripMargin
      }

      val params = errorMessage match {
        case Some(msg) => Seq(status, msg, storyId)
        case None => Seq(status, storyId)
      }

      postgresUtil.executePreparedUpdate(updateQuery, params, config.storiesMetaTable, storyId.toString)

      val verifyStatus = checkStoryStatus(storyId)
      val success = verifyStatus == status

      if (success) {
        println(s"[StoryID: $storyId] Story status successfully updated to: $status")
      } else {
        println(s"[StoryID: $storyId] Story status update verification failed. Expected: $status, Got: $verifyStatus")
      }

      success
    } catch {
      case e: Exception =>
        println(s"[StoryID: $storyId] Failed to update story status: ${e.getMessage}")
        e.printStackTrace()
        false
    }
  }

  /**
   * Extract image paths by removing base URL from pipe-separated image links
   *
   * @param imageLinks Pipe-separated full image/pdf URLs
   * @return Pipe-separated image paths without base URL, or empty string if invalid
   */
  private def extractPaths(imageLinks: String, baseUrl: String, logPrefix: String): String = {
    if (imageLinks == null || imageLinks.trim.isEmpty) {
      return ""
    }
    try {
      val imagePaths = imageLinks
        .split('|').map(_.trim).filter(_.nonEmpty)
        .map { url =>
          if (url.startsWith(baseUrl)) {
            url.replace(baseUrl, "")
          } else {
            url
          }
        }
        .filter(_.nonEmpty)

      val result = imagePaths.mkString(" | ")

      println(s"$logPrefix Extracted ${imagePaths.length} image/pdf paths")
      result

    } catch {
      case e: Exception =>
        println(s"$logPrefix Error extracting image paths: ${e.getMessage}")
        imageLinks
    }
  }

  override def close(): Unit = {
    super.close()
    if (piiAnalyzer != null) piiAnalyzer.close()
    if (storyAnalyzer != null) storyAnalyzer.close()
    //    if (cloudUploader != null) cloudUploader.close()

  }
}