package org.shikshalokam.job.user.mapping.stream.processor.util

import org.slf4j.LoggerFactory
import ujson.{Js, Obj}
import requests._

import scala.util.{Failure, Success, Try}

/**
 * HTTP client for making PATCH requests to User Service API
 * 
 * Endpoint: PATCH http://localhost:7001/user/v1/user/update
 * Header: X-auth-token: {token}
 * Content-Type: application/json
 */
object UserApiClient {
  
  private val logger = LoggerFactory.getLogger(UserApiClient.getClass)
  
  // User Service Configuration
  private val BASE_URL = "http://localhost:7001"
  private val AUTH_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7ImlkIjozMDg3LCJuYW1lIjoiRmFyYWJpIEFobWVkdWxsYWgiLCJzZXNzaW9uX2lkIjoyMzAyMywib3JnYW5pemF0aW9uX2lkcyI6WyI2NyJdLCJvcmdhbml6YXRpb25fY29kZXMiOlsiYnJhY19nYmwiXSwidGVuYW50X2NvZGUiOiJicmFjIiwib3JnYW5pemF0aW9ucyI6W3siaWQiOjY3LCJuYW1lIjoiQlJBQyBHQkwgb3JnIiwiY29kZSI6ImJyYWNfZ2JsIiwiZGVzY3JpcHRpb24iOiJCUkFDIEdCTCBvcmciLCJzdGF0dXMiOiJBQ1RJVkUiLCJyZWxhdGVkX29yZ3MiOm51bGwsInRlbmFudF9jb2RlIjoiYnJhYyIsIm1ldGEiOm51bGwsImNyZWF0ZWRfYnkiOm51bGwsInVwZGF0ZWRfYnkiOjEsInJvbGVzIjpbeyJpZCI6MjEzLCJ0aXRsZSI6InNlc3Npb25fbWFuYWdlciIsImxhYmVsIjoiTGlua2FnZSBDaGFtcGlvbiIsInVzZXJfdHlwZSI6MCwic3RhdHVzIjoiQUNUSVZFIiwib3JnYW5pemF0aW9uX2lkIjo2NywidmlzaWJpbGl0eSI6IlBVQkxJQyIsInRlbmFudF9jb2RlIjoiYnJhYyIsInRyYW5zbGF0aW9ucyI6bnVsbH1dfV19LCJpYXQiOjE3NjkwNjA4NTMsImV4cCI6MTc2OTE0NzI1M30.CPTelpSKG7wHA7WccMMwVI7rk0QajCt_Baf1rh0vCcw"
  
  println(s"[UserApiClient] Initialized with BASE_URL: $BASE_URL")
  println(s"[UserApiClient] AUTH_TOKEN configured: ${if (AUTH_TOKEN.nonEmpty) "***" else "NOT SET"}")
  
  /**
   * Patch user profile data for a student
   * 
   * @param studentId The student ID to update
   * @param profileData JSON object containing profile data (e.g., {"profile": {"phone": "...", "gender": "..."}})
   * @return Success(true) if update successful, Failure(exception) otherwise
   */
  def patchProfile(studentId: String, profileData: Js.Obj): Try[Boolean] = {
    if (studentId == null || studentId.trim.isEmpty) {
      val error = new IllegalArgumentException("studentId cannot be null or empty")
      logger.error("[UserApiClient] studentId is null or empty", error)
      println(s"[UserApiClient] ERROR: studentId is null or empty")
      return Failure(error)
    }
    
    try {
      val url = s"$BASE_URL/user/v1/user/update"
      
      // Merge studentId with profileData into a single payload
      // The API expects the payload directly, so we'll include studentId and merge profile fields
      val payloadObj = Obj()
      
      // Add studentId to payload to identify which user to update
      payloadObj.value("id") = studentId
      
      // Merge profile fields from profileData into payload
      val profileObj = profileData.value.get("profile")
      if (profileObj.isDefined) {
        // Merge profile fields directly into payload (flatten the structure)
        profileObj.get.asInstanceOf[Obj].value.foreach { case (key, value) =>
          payloadObj.value(key) = value
        }
      } else {
        // If no profile object, use the entire profileData
        profileData.value.foreach { case (key, value) =>
          payloadObj.value(key) = value
        }
      }
      
      val jsonPayload = payloadObj.render()
      
      println(s"[UserApiClient] PATCH Request to: $url")
      println(s"[UserApiClient] Request payload: $jsonPayload")
      
      val headers = Map(
        "X-auth-token" -> AUTH_TOKEN,
        "Content-Type" -> "application/json"
      )
      
      println(s"[UserApiClient] Request headers: X-auth-token=***, Content-Type=application/json")
      
      val response = requests.patch(
        url,
        data = jsonPayload,
        headers = headers
      )
      
      println(s"[UserApiClient] Response status code: ${response.statusCode}")
      println(s"[UserApiClient] Response body: ${response.text}")
      
      if (response.statusCode >= 200 && response.statusCode < 300) {
        println(s"[UserApiClient] SUCCESS: Profile updated for studentId=$studentId")
        logger.info(s"[UserApiClient] Successfully updated profile for studentId=$studentId")
        Success(true)
      } else {
        val error = new Exception(s"User Service API returned status ${response.statusCode}: ${response.text}")
        logger.error(s"[UserApiClient] API error for studentId=$studentId: ${error.getMessage}", error)
        println(s"[UserApiClient] ERROR: API returned status ${response.statusCode}: ${response.text}")
        Failure(error)
      }
      
    } catch {
      case e: Exception =>
        logger.error(s"[UserApiClient] Exception while patching profile for studentId=$studentId: ${e.getMessage}", e)
        println(s"[UserApiClient] EXCEPTION: ${e.getMessage}")
        e.printStackTrace()
        Failure(e)
    }
  }
  
  /**
   * Test connection to user service (for debugging)
   */
  def testConnection(): Try[Boolean] = {
    try {
      val url = s"$BASE_URL/api/users/health"
      println(s"[UserApiClient] Testing connection to: $url")
      
      val response = requests.get(url)
      println(s"[UserApiClient] Health check response: ${response.statusCode}")
      Success(response.statusCode == 200)
    } catch {
      case e: Exception =>
        println(s"[UserApiClient] Connection test failed: ${e.getMessage}")
        Failure(e)
    }
  }
}
