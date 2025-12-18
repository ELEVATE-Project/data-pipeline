package org.shikshalokam.job.mitra.stream.processor.utils

import com.google.gson.{Gson, JsonParser}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest
import org.shikshalokam.job.mitra.stream.processor.task.MitraStreamConfig
import scala.collection.JavaConverters._
import scala.util.{Try, Failure, Success}

case class ClassifiedItem(challenge: String, theme_id: Int, theme_name: String, pii_flag: Boolean, justification: String, confidence_score: Double, multi_theme_mapped: Boolean)
case class ClassificationResponse(source: String, classified_data: List[ClassifiedItem])
case class ErrorResponse(error: String)

class ThematicAnalyzer(config: MitraStreamConfig) {

  private val gson = new Gson()
  private val bedrockClient: BedrockRuntimeClient = createBedrockClient()

  def analyzeThematicChallenge(challengeList: List[String], contextPrompt: String): Either[ErrorResponse, ClassificationResponse] = {
    if (challengeList.isEmpty) {
      return Left(ErrorResponse("No valid challenge statements provided."))
    }

    val fullPrompt = buildPrompt(challengeList, contextPrompt)

    config.modelProvider match {
      case "claude" | "bedrock" => invokeClaudeModel(fullPrompt, challengeList.size)
      case "gemini" => Left(ErrorResponse("Gemini model not implemented"))
      case "chatgpt" => Left(ErrorResponse("ChatGPT model not implemented"))
      case _ => Left(ErrorResponse(s"Unknown model provider: ${config.modelProvider}"))
    }
  }

  private def createBedrockClient(): BedrockRuntimeClient = {
    val credentials = AwsBasicCredentials.create(config.awsAccessKey, config.awsSecretKey)
    BedrockRuntimeClient.builder()
      .region(Region.of(config.awsRegion))
      .credentialsProvider(StaticCredentialsProvider.create(credentials))
      .build()
  }

  private def buildPrompt(challenges: List[String], contextPrompt: String): String = {
    val challengesFormatted = challenges.map(c => s"- $c").mkString("\n")
    s"$contextPrompt\n$challengesFormatted"
  }

  private def buildRequestBody(prompt: String): String = {
    val message = Map("role" -> "user", "content" -> prompt).asJava
    val requestBody = Map(
      "anthropic_version" -> config.bedrockModelVesrion,
      "max_tokens" -> config.bedrockMaxTokens.asInstanceOf[Object],
      "temperature" -> config.bedrockModelTemperature.asInstanceOf[Object],
      "messages" -> java.util.Arrays.asList(message)
    ).asJava

    gson.toJson(requestBody)
  }

  private def invokeClaudeModel(prompt: String, expectedCount: Int): Either[ErrorResponse, ClassificationResponse] = {
    Try {
      val requestBodyJson = buildRequestBody(prompt)
      val request = InvokeModelRequest.builder()
        .modelId(config.bedrockModelId)
        .body(SdkBytes.fromUtf8String(requestBodyJson))
        .build()

      val response = bedrockClient.invokeModel(request)
      val responseBody = response.body().asUtf8String()

      parseResponse(responseBody, expectedCount)
    } match {
      case Success(result) => result
      case Failure(e) => Left(ErrorResponse(s"Claude API call failed: ${e.getMessage}"))
    }
  }

  private def parseResponse(responseBody: String, expectedCount: Int): Either[ErrorResponse, ClassificationResponse] = {
    val jsonResponse = JsonParser.parseString(responseBody).getAsJsonObject

    if (!jsonResponse.has("content") || jsonResponse.get("content").getAsJsonArray.isEmpty) {
      return Left(ErrorResponse("No content received from Claude API"))
    }

    val textContent = jsonResponse.get("content")
      .getAsJsonArray.get(0)
      .getAsJsonObject.get("text")
      .getAsString

    val cleanedJson = extractJson(textContent)
    val parsedResponse = gson.fromJson(cleanedJson, classOf[java.util.Map[String, Object]])

    if (!parsedResponse.containsKey("classified_data")) {
      return Left(ErrorResponse("Invalid response structure: missing 'classified_data' field"))
    }

    val classifiedData = parsedResponse.get("classified_data")
      .asInstanceOf[java.util.List[java.util.Map[String, Object]]]
      .asScala
      .map(parseClassifiedItem)
      .toList

    Right(ClassificationResponse(config.bedrockModelId, classifiedData))
  }

  private def parseClassifiedItem(item: java.util.Map[String, Object]): ClassifiedItem = {
    ClassifiedItem(
      challenge = Option(item.get("challenge")).map(_.toString).getOrElse(""),
      theme_id = Option(item.get("theme_id")).map(_.toString.toDouble.toInt).getOrElse(0),
      theme_name = Option(item.get("theme_name")).map(_.toString).getOrElse("Unknown"),
      pii_flag = Option(item.get("pii_flag")).exists(_.toString.toBoolean),
      justification = Option(item.get("justification")).map(_.toString).getOrElse(""),
      confidence_score = Option(item.get("confidence_score")).map(_.toString.toDouble).getOrElse(0.0),
      multi_theme_mapped = Option(item.get("multi_theme_mapped")).exists(_.toString.toBoolean))
  }

  private def extractJson(text: String): String = {
    Try(JsonParser.parseString(text)).map(_ => text).getOrElse {
      val start = text.indexOf('{')
      val end = text.lastIndexOf('}') + 1
      if (start != -1 && end > start) text.substring(start, end)
      else throw new IllegalArgumentException("Could not extract JSON from response")
    }
  }

  def close(): Unit = bedrockClient.close()
}