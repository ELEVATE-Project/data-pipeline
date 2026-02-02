package org.shikshalokam.job.user.mapping.stream.processor.util

import org.slf4j.LoggerFactory
import ujson.{Js, Obj}
import requests._

import scala.util.{Failure, Success, Try}

/**
 * HTTP client for making PATCH requests to User Service API
 * 
 * Endpoint: PATCH {baseUrl}/user/v1/user/update
 * Header: X-auth-token: {token}
 * Content-Type: application/json
 */
object UserApiClient {
  
  private val logger = LoggerFactory.getLogger(UserApiClient.getClass)
  
  /**
   * Patch user profile data for a user
   * 
   * @param id The user ID to update
   * @param profileData JSON object containing profile data (e.g., {"profile": {"phone": "...", "gender": "..."}})
   * @param baseUrl The base URL for the User Service API
   * @param authToken The authentication token for the User Service API
   * @return Success(true) if update successful, Failure(exception) otherwise
   */
  def patchProfile(id: String, profileData: Js.Obj, baseUrl: String, authToken: String): Try[Boolean] = {
    if (id == null || id.trim.isEmpty) {
      val error = new IllegalArgumentException("id cannot be null or empty")
      logger.error("[UserApiClient] id is null or empty", error)
      return Failure(error)
    }
    
    try {
      val url = s"$baseUrl/user/v1/user/update"
      
      println(s"[UserApiClient] Initialized with BASE_URL: $baseUrl")
      println(s"[UserApiClient] AUTH_TOKEN configured: ${if (authToken.nonEmpty) "***" else "NOT SET"}")
      
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
        "X-auth-token" -> authToken,
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
   * 
   * @param baseUrl The base URL for the User Service API
   */
  def testConnection(baseUrl: String): Try[Boolean] = {
    try {
      val url = s"$baseUrl/api/users/health"
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
