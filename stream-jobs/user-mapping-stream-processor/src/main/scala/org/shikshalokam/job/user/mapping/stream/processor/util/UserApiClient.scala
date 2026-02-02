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
  private val BASE_URL = "http://172.132.44.221:7001"
  private val AUTH_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7ImlkIjozMDg3LCJuYW1lIjoiZmFyYWJpIFVwZGF0ZWQgdHdvIiwic2Vzc2lvbl9pZCI6MjMwMjgsIm9yZ2FuaXphdGlvbl9pZHMiOlsiNjciXSwib3JnYW5pemF0aW9uX2NvZGVzIjpbImJyYWNfZ2JsIl0sInRlbmFudF9jb2RlIjoiYnJhYyIsIm9yZ2FuaXphdGlvbnMiOlt7ImlkIjo2NywibmFtZSI6IkJSQUMgR0JMIG9yZyIsImNvZGUiOiJicmFjX2dibCIsImRlc2NyaXB0aW9uIjoiQlJBQyBHQkwgb3JnIiwic3RhdHVzIjoiQUNUSVZFIiwicmVsYXRlZF9vcmdzIjpudWxsLCJ0ZW5hbnRfY29kZSI6ImJyYWMiLCJtZXRhIjpudWxsLCJjcmVhdGVkX2J5IjpudWxsLCJ1cGRhdGVkX2J5IjoxLCJyb2xlcyI6W3siaWQiOjIxMywidGl0bGUiOiJzZXNzaW9uX21hbmFnZXIiLCJsYWJlbCI6IkxpbmthZ2UgQ2hhbXBpb24iLCJ1c2VyX3R5cGUiOjAsInN0YXR1cyI6IkFDVElWRSIsIm9yZ2FuaXphdGlvbl9pZCI6NjcsInZpc2liaWxpdHkiOiJQVUJMSUMiLCJ0ZW5hbnRfY29kZSI6ImJyYWMiLCJ0cmFuc2xhdGlvbnMiOm51bGx9XX1dfSwiaWF0IjoxNzY5NzUxNjcxLCJleHAiOjE3Njk4MzgwNzF9.wJpLStvXoU81vGQtIijs867BC7QJWO2F1w5F8W1X8-8"
  
  
  /**
   * Patch user profile data for a user
   * 
   * @param id The user ID to update
   * @param profileData JSON object containing profile data (e.g., {"profile": {"phone": "...", "gender": "..."}})
   * @return Success(true) if update successful, Failure(exception) otherwise
   */
  def patchProfile(id: String, profileData: Js.Obj): Try[Boolean] = {
    if (id == null || id.trim.isEmpty) {
      val error = new IllegalArgumentException("id cannot be null or empty")
      logger.error("[UserApiClient] id is null or empty", error)
      return Failure(error)
    }
    
    try {
      val url = s"$BASE_URL/user/v1/user/update"
      
      // Merge id with profileData into a single payload
      // The API expects the payload directly, so we'll include id and merge profile fields
      val payloadObj = Obj()
      
      // Add id to payload to identify which user to update
      payloadObj.value("id") = id
      
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
      
      // println(s"[UserApiClient] PATCH Request to: $url")
      // println(s"[UserApiClient] Request payload: $jsonPayload")
      
      val headers = Map(
        "X-auth-token" -> AUTH_TOKEN,
        "Content-Type" -> "application/json"
      )
      
      // println(s"[UserApiClient] Request headers: X-auth-token=***, Content-Type=application/json")
      
      // Use check = false to prevent RequestFailedException from being thrown for non-2xx status codes
      // This allows us to handle error responses gracefully
      val response = requests.patch(
        url,
        data = jsonPayload,
        headers = headers,
        check = false
      )
      
      // println(s"[UserApiClient] Response status code: ${response.statusCode}")
      // println(s"[UserApiClient] Response body: ${response.text}")
      
      if (response.statusCode >= 200 && response.statusCode < 300) {
        logger.info(s"[UserApiClient] Successfully updated profile for id=$id")
        Success(true)
      } else {
        val error = new Exception(s"User Service API returned status ${response.statusCode}: ${response.text}")
        logger.error(s"[UserApiClient] API error for id=$id: ${error.getMessage}", error)
        Failure(error)
      }
      
    } catch {
      case e: Exception =>
        logger.error(s"[UserApiClient] Exception while patching profile for id=$id: ${e.getMessage}", e)
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
