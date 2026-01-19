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

case class PIIAnalysisResult(pii_flag: Boolean, justification: String, confidence_score: Double)

class PIIAnalyzer(config: MitraStreamConfig) {

  private val gson = new Gson()
  private val bedrockClient: BedrockRuntimeClient = createBedrockClient()

  def analyzePII(text: String, contextPrompt: String): Either[ErrorResponse, PIIAnalysisResult] = {
    if (text == null || text.trim.isEmpty) {
      return Left(ErrorResponse("No text provided for PII analysis."))
    }

    if (contextPrompt == null || contextPrompt.trim.isEmpty) {
      return Left(ErrorResponse("PII analysis prompt not found in database."))
    }

    val fullPrompt = buildPrompt(text, contextPrompt)

    config.modelProvider match {
      case "claude" | "bedrock" => invokeClaudeModel(fullPrompt)
      case "gemini" => Left(ErrorResponse("Gemini model not implemented for PII analysis"))
      case "chatgpt" => Left(ErrorResponse("ChatGPT model not implemented for PII analysis"))
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

  private def buildPrompt(text: String, contextPrompt: String): String = contextPrompt.replace({
    "{text}"
  }, text)

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

  private def invokeClaudeModel(prompt: String): Either[ErrorResponse, PIIAnalysisResult] = {
    Try {
      val requestBodyJson = buildRequestBody(prompt)
      val request = InvokeModelRequest.builder()
        .modelId(config.bedrockModelId)
        .body(SdkBytes.fromUtf8String(requestBodyJson))
        .build()

      val response = bedrockClient.invokeModel(request)
      val responseBody = response.body().asUtf8String()

      parseResponse(responseBody)
    } match {
      case Success(result) => result
      case Failure(e) => Left(ErrorResponse(s"Claude API call failed: ${e.getMessage}"))
    }
  }

  private def parseResponse(responseBody: String): Either[ErrorResponse, PIIAnalysisResult] = {
    try {
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

      if (!parsedResponse.containsKey("pii_flag") || !parsedResponse.containsKey("justification") || !parsedResponse.containsKey("confidence_score")) {
        return Left(ErrorResponse("Invalid response structure: missing required fields"))
      }

      val piiFlag = parsedResponse.get("pii_flag") match {
        case b: java.lang.Boolean => b.booleanValue()
        case _ => throw new IllegalArgumentException("pii_flag must be Boolean")
      }

      val justification = parsedResponse.get("justification") match {
        case s: String => s
        case _ => throw new IllegalArgumentException("justification must be String")
      }

      val confidenceScore = parsedResponse.get("confidence_score") match {
        case n: java.lang.Number => n.doubleValue()
        case _ => throw new IllegalArgumentException("confidence_score must be numeric")
      }

      Right(PIIAnalysisResult(piiFlag, justification, confidenceScore))
    } catch {
      case e: Exception =>
        Left(ErrorResponse(s"Failed to parse PII analysis response: ${e.getMessage}"))
    }
  }

  private def extractJson(text: String): String = {
    Try(JsonParser.parseString(text)).map(_ => text).getOrElse {
      val start = text.indexOf('{')
      val end = text.lastIndexOf('}') + 1
      if (start != -1 && end > start) text.substring(start, end)
      else throw new IllegalArgumentException("Could not extract JSON from response")
    }
  }

  def close(): Unit = {
    if (bedrockClient != null) {
      bedrockClient.close()
    }
  }
}