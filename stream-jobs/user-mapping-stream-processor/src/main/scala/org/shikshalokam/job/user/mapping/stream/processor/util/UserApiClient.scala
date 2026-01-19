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
  private val AUTH_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7ImlkIjozMDg4LCJuYW1lIjoiQ2Fyb2wgTWlyYW5kYSIsInNlc3Npb25faWQiOjIzMDE4LCJvcmdhbml6YXRpb25faWRzIjpbIjY3Il0sIm9yZ2FuaXphdGlvbl9jb2RlcyI6WyJicmFjX2dibCJdLCJ0ZW5hbnRfY29kZSI6ImJyYWMiLCJvcmdhbml6YXRpb25zIjpbeyJpZCI6NjcsIm5hbWUiOiJCUkFDIEdCTCBvcmciLCJjb2RlIjoiYnJhY19nYmwiLCJkZXNjcmlwdGlvbiI6IkJSQUMgR0JMIG9yZyIsInN0YXR1cyI6IkFDVElWRSIsInJlbGF0ZWRfb3JncyI6bnVsbCwidGVuYW50X2NvZGUiOiJicmFjIiwibWV0YSI6bnVsbCwiY3JlYXRlZF9ieSI6bnVsbCwidXBkYXRlZF9ieSI6MSwicm9sZXMiOlt7ImlkIjoyMTMsInRpdGxlIjoic2Vzc2lvbl9tYW5hZ2VyIiwibGFiZWwiOiJMaW5rYWdlIENoYW1waW9uIiwidXNlcl90eXBlIjowLCJzdGF0dXMiOiJBQ1RJVkUiLCJvcmdhbml6YXRpb25faWQiOjY3LCJ2aXNpYmlsaXR5IjoiUFVCTElDIiwidGVuYW50X2NvZGUiOiJicmFjIiwidHJhbnNsYXRpb25zIjpudWxsfV19XX0sImlhdCI6MTc2ODM4Mzc1MCwiZXhwIjoxNzY4NDcwMTUwfQ.T8Ycr0X3bbVOCi63p8CdHlt9hAwClrKS9euGwV6ht78"
  
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
