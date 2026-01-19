package org.shikshalokam.job.mitra.stream.processor.utils

import com.google.gson.{Gson, JsonParser}
import org.shikshalokam.job.mitra.stream.processor.task.MitraStreamConfig
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Case class for Story Rating Result
 */
case class StoryRatingResult(document_language: String,
                             impact_and_outcome_score: Double,
                             impact_justification: String,
                             issue_and_challenge_score: Double,
                             issue_justification: String,
                             action_steps_score: Double,
                             action_justification: String,
                             composite_score: Double,
                             tier: String,
                             overall_summary: String)

/**
 * Story Analyzer - Analyzes and ranks stories using LLM based on text fields
 * (challenges, action steps, impact, and content)
 */
class StoryAnalyzer(config: MitraStreamConfig) {

  private val gson = new Gson()
  private val bedrockClient: BedrockRuntimeClient = createBedrockClient()
  private val pdfExtractor: PDFExtractor = new PDFExtractor()

  /**
   * Analyze story using FULL PDF content for accurate ranking
   * Falls back to CSV fields if PDF extraction fails
   *
   * @param pdfLink       URL to story PDF document
   * @param challenges    Story challenges/issues
   * @param actionSteps   Actions taken to address the challenges
   * @param impact        Impact and outcomes achieved
   * @param contextPrompt Story ranking prompt from database
   * @return Either error or story rating result
   */
  def analyzeStory(pdfLink: String, challenges: String, actionSteps: String, impact: String, contextPrompt: String, logPrefix: String): Either[ErrorResponse, StoryRatingResult] = {

    if (contextPrompt == null || contextPrompt.trim.isEmpty) {
      return Left(ErrorResponse(s"$logPrefix [LLM] Story analysis prompt not found in database."))
    }

    // 1. Try to extract from PDF first
    val (contentToAnalyze, detectedLanguage) = if (pdfLink != null && pdfLink.trim.nonEmpty) {
      pdfExtractor.extractTextFromPdf(pdfLink, logPrefix) match {
        case Right((pdfText, language)) =>
          println(s"$logPrefix [LLM] Using PDF content for analysis (${pdfText.length} chars, language: $language)")
          val truncatedText = pdfExtractor.truncateIfNeeded(pdfText, maxChars = 40000, logPrefix)
          (truncatedText, language)

        case Left(error) =>
          println(s"$logPrefix [LLM] PDF extraction failed: $error. Falling back to CSV fields.")
          // Fallback to CSV fields
          val fallbackText = buildTextFromFields(challenges, actionSteps, impact)
          (fallbackText, "Unknown")
      }
    } else {
      println(s"$logPrefix [LLM] No PDF link provided. Using CSV fields.")
      val fallbackText = buildTextFromFields(challenges, actionSteps, impact)
      (fallbackText, "Unknown")
    }

    // Check if we have any content
    if (contentToAnalyze.trim.isEmpty) {
      return Left(ErrorResponse(s"$logPrefix [LLM] No content available for analysis (PDF and CSV fields both empty)"))
    }

    // 2. Build prompt with extracted content
    val fullPrompt = buildPromptWithContent(contentToAnalyze, contextPrompt)
    println(s"$logPrefix [LLM] Built prompt. Length: ${fullPrompt.length} chars, Detected language: $detectedLanguage")

    // 3. Call Claude
    config.modelProvider match {
      case "claude" | "bedrock" => invokeClaudeModel(fullPrompt, logPrefix)
      case "gemini" => Left(ErrorResponse("Gemini model not implemented for story analysis"))
      case "chatgpt" => Left(ErrorResponse("ChatGPT model not implemented for story analysis"))
      case _ => Left(ErrorResponse(s"$logPrefix [LLM] Unknown model provider: ${config.modelProvider}"))
    }
  }

  /**
   * Create AWS Bedrock client
   */
  private def createBedrockClient(): BedrockRuntimeClient = {
    val credentials = AwsBasicCredentials.create(config.awsAccessKey, config.awsSecretKey)
    BedrockRuntimeClient.builder()
      .region(Region.of(config.awsRegion))
      .credentialsProvider(StaticCredentialsProvider.create(credentials))
      .build()
  }

  private def buildStoryAnalysisToolDefinition(): java.util.Map[String, Object] = {
    Map(
      "name" -> "record_story_analysis",
      "description" -> "Record the structured analysis results for the story",
      "input_schema" -> Map(
        "type" -> "object",
        "properties" -> Map(
          "document_language" -> Map(
            "type" -> "string",
            "description" -> "Primary language of the document (e.g., English, Hindi, Kannada)"
          ).asJava,
          "impact_and_outcome_score" -> Map(
            "type" -> "number",
            "description" -> "Score between 0.0 and 1.0 for impact and outcomes"
          ).asJava,
          "impact_justification" -> Map(
            "type" -> "string",
            "description" -> "Detailed justification for impact score"
          ).asJava,
          "issue_and_challenge_score" -> Map(
            "type" -> "number",
            "description" -> "Score between 0.0 and 1.0 for issue clarity"
          ).asJava,
          "issue_justification" -> Map(
            "type" -> "string",
            "description" -> "Detailed justification for issue score"
          ).asJava,
          "action_steps_score" -> Map(
            "type" -> "number",
            "description" -> "Score between 0.0 and 1.0 for action steps"
          ).asJava,
          "action_justification" -> Map(
            "type" -> "string",
            "description" -> "Detailed justification for action steps score"
          ).asJava,
          "composite_score" -> Map(
            "type" -> "number",
            "description" -> "Weighted average composite score"
          ).asJava,
          "tier" -> Map(
            "type" -> "string",
            "enum" -> java.util.Arrays.asList("Excellent", "Good", "Developing", "Needs Improvement"),
            "description" -> "Overall tier classification"
          ).asJava,
          "overall_summary" -> Map(
            "type" -> "string",
            "description" -> "Brief 2-3 sentence summary"
          ).asJava
        ).asJava,
        "required" -> java.util.Arrays.asList(
          "document_language", "impact_and_outcome_score", "impact_justification",
          "issue_and_challenge_score", "issue_justification", "action_steps_score",
          "action_justification", "composite_score", "tier", "overall_summary"
        )
      ).asJava
    ).asJava
  }

  /**
   * Build request body for Claude API
   */
  private def buildRequestBodyWithText(prompt: String): String = {
    val content = java.util.Arrays.asList(
      Map("type" -> "text", "text" -> prompt).asJava
    )

    val message = Map("role" -> "user", "content" -> content).asJava

    val requestBody = Map(
      "anthropic_version" -> config.bedrockModelVesrion,
      "max_tokens" -> config.bedrockMaxTokens.asInstanceOf[Object],
      "temperature" -> config.bedrockModelTemperature.asInstanceOf[Object],
      "messages" -> java.util.Arrays.asList(message),
      "tools" -> java.util.Arrays.asList(buildStoryAnalysisToolDefinition()),
      "tool_choice" -> Map("type" -> "tool", "name" -> "record_story_analysis").asJava
    ).asJava

    gson.toJson(requestBody)
  }

  /**
   * Invoke Claude model for story analysis
   */
  private def invokeClaudeModel(prompt: String, logPrefix: String): Either[ErrorResponse, StoryRatingResult] = {
    Try {
      val requestBodyJson = buildRequestBodyWithText(prompt)
      val request = InvokeModelRequest.builder()
        .modelId(config.bedrockModelId)
        .body(SdkBytes.fromUtf8String(requestBodyJson))
        .build()

      println(s"$logPrefix [LLM] Invoking Claude model for story analysis...")

      val response = bedrockClient.invokeModel(request)
      val responseBody = response.body().asUtf8String()

      println(s"$logPrefix [LLM] Response received. Length: ${responseBody.length} characters")

      parseResponse(responseBody, logPrefix)
    } match {
      case Success(result) => result
      case Failure(e) => Left(ErrorResponse(s"$logPrefix [LLM] Claude API call failed: ${e.getMessage}"))
    }
  }

  /**
   * Parse Claude API response for story analysis
   */
  private def parseResponse(responseBody: String, logPrefix: String): Either[ErrorResponse, StoryRatingResult] = {
    try {
      val jsonResponse = JsonParser.parseString(responseBody).getAsJsonObject

      if (!jsonResponse.has("content") || jsonResponse.get("content").getAsJsonArray.isEmpty) {
        return Left(ErrorResponse(s"$logPrefix [LLM] No content received from Claude API"))
      }

      // Look for tool_use in content array
      val contentArray = jsonResponse.get("content").getAsJsonArray
      var toolUseBlock: com.google.gson.JsonObject = null

      for (i <- 0 until contentArray.size()) {
        val block = contentArray.get(i).getAsJsonObject
        if (block.has("type") && block.get("type").getAsString == "tool_use") {
          toolUseBlock = block
        }
      }

      if (toolUseBlock == null) {
        return Left(ErrorResponse(s"$logPrefix [LLM] No tool use found in response"))
      }

      // Get the input (this is our structured data!)
      val toolInput = toolUseBlock.get("input").getAsJsonObject

      // Parse directly - no duplicate keys possible!
      val result = StoryRatingResult(
        document_language = capitalizeLanguage(toolInput.get("document_language").getAsString),
        impact_and_outcome_score = toolInput.get("impact_and_outcome_score").getAsDouble,
        impact_justification = toolInput.get("impact_justification").getAsString,
        issue_and_challenge_score = toolInput.get("issue_and_challenge_score").getAsDouble,
        issue_justification = toolInput.get("issue_justification").getAsString,
        action_steps_score = toolInput.get("action_steps_score").getAsDouble,
        action_justification = toolInput.get("action_justification").getAsString,
        composite_score = toolInput.get("composite_score").getAsDouble,
        tier = toolInput.get("tier").getAsString,
        overall_summary = toolInput.get("overall_summary").getAsString
      )

      // Validate scores
      if (!isValidScore(result.impact_and_outcome_score) ||
        !isValidScore(result.issue_and_challenge_score) ||
        !isValidScore(result.action_steps_score) ||
        !isValidScore(result.composite_score)) {
        return Left(ErrorResponse(s"$logPrefix [LLM] Scores must be between 0.0 and 1.0"))
      }

      println(s"$logPrefix [LLM] Story analysis parsed successfully (via tool). Tier: ${result.tier}, Composite: ${result.composite_score}")

      Right(result)
    } catch {
      case e: Exception =>
        println(s"$logPrefix [LLM] Failed to parse response: ${e.getMessage}")
        e.printStackTrace()
        Left(ErrorResponse(s"$logPrefix [LLM] Failed to parse story analysis response: ${e.getMessage}"))
    }
  }

  /**
   * Build text from CSV fields as fallback (when PDF fails)
   */
  private def buildTextFromFields(challenges: String, actionSteps: String, impact: String): String = {
    val parts = scala.collection.mutable.ArrayBuffer[String]()

    if (challenges != null && challenges.trim.nonEmpty) {
      parts += s"Challenges and Issues:\n${challenges.trim}\n"
    }

    if (actionSteps != null && actionSteps.trim.nonEmpty) {
      parts += s"Action Steps Taken:\n${actionSteps.trim}\n"
    }

    if (impact != null && impact.trim.nonEmpty) {
      parts += s"Impact and Outcomes:\n${impact.trim}\n"
    }

    parts.mkString("\n")
  }

  /**
   * Build prompt with full content (replaces the old buildPrompt)
   */
  private def buildPromptWithContent(content: String, contextPrompt: String): String = {
    contextPrompt.replace("{story_content}", content)
  }

  /**
   * Validate score is between 0.0 and 1.0
   */
  private def isValidScore(score: Double): Boolean = {
    score >= 0.0 && score <= 1.0
  }

  /**
   * Capitalize language name properly (e.g., "english" -> "English")
   */
  private def capitalizeLanguage(lang: String): String = {
    if (lang == null || lang.isEmpty) "Unknown"
    else lang.substring(0, 1).toUpperCase + lang.substring(1).toLowerCase
  }

  def close(): Unit = {
    if (bedrockClient != null) {
      bedrockClient.close()
    }
  }
}