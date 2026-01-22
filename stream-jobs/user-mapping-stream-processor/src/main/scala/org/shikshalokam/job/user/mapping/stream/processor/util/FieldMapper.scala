package org.shikshalokam.job.user.mapping.stream.processor.util

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import ujson.{Js, Obj}

import java.util
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * FieldMapper utility to transform observation data fields to user profile fields
 * based on mappings defined in field-mappings.conf
 * 
 * Example mapping:
 * name -> profile.name
 * about -> profile.about
 */
object FieldMapper {
  
  private val logger = LoggerFactory.getLogger(FieldMapper.getClass)
  
  // Load field mappings from config file
  private val mappings: Map[String, String] = loadMappings()
  
  println(s"[FieldMapper] Loaded ${mappings.size} field mappings")
  mappings.foreach { case (source, target) =>
    println(s"[FieldMapper] Mapping: $source -> $target")
  }
  
  /**
   * Load field mappings from field-mappings.conf
   * @return Map of source field -> target field path
   */
  private def loadMappings(): Map[String, String] = {
    try {
      // Use parseResources to load only our config file, avoiding conflicts with other configs
      val config = ConfigFactory.parseResources("field-mappings.conf")
      if (config.isEmpty) {
        logger.warn("[FieldMapper] field-mappings.conf is empty or not found")
        println("[FieldMapper] WARNING: field-mappings.conf is empty or not found")
        return Map.empty[String, String]
      }
      
      val configMap = config.entrySet().asScala.map { entry =>
        val rawKey = entry.getKey
        val key = rawKey.replaceAll("\"", "")
        val value = config.getString(rawKey)
        (key, value)
      }.toMap
      
      println(s"[FieldMapper] Successfully loaded ${configMap.size} mappings from field-mappings.conf")
      configMap
    } catch {
      case e: Exception =>
        logger.error(s"[FieldMapper] Failed to load field-mappings.conf: ${e.getMessage}", e)
        println(s"[FieldMapper] ERROR: Failed to load field-mappings.conf: ${e.getMessage}")
        e.printStackTrace()
        Map.empty[String, String]
    }
  }
  
  /**
   * Check if a value is null or empty
   * @param value The value to check
   * @return true if value is null, empty string, or empty collection
   */
  private def isValueEmpty(value: Any): Boolean = {
    if (value == null) {
      return true
    }
    
    value match {
      case s: String => s.trim.isEmpty
      case coll: java.util.Collection[_] => coll.isEmpty
      case arr: Array[_] => arr.isEmpty
      case map: java.util.Map[_, _] => map.isEmpty
      case _ => false // Other types (numbers, booleans) are not considered empty
    }
  }
  
  /**
   * Transform observation data to user profile patch format
   * 
   * Input observationData example:
   * {
   *   "name": "Carol Miranda Updated Two",
   *   "about": "admin Update",
   *   "dob": "22-12-1990"
   * }
   * 
   * Output profile format:
   * {
   *   "profile": {
   *     "name": "Carol Miranda Updated One",
   *     "about": "admin Update",
   *     "dob": "22-12-1990"
   *   }
   * }
   * 
   * @param observationData Map containing observation field values
   * @return JsObject representing the profile patch structure
   */
  def transform(observationData: util.Map[String, Any]): Js.Obj = {
    try {
      println(s"[FieldMapper] Starting transformation of observation data")
      println(s"[FieldMapper] Input observationData keys: ${observationData.keySet().asScala.mkString(", ")}")
      
      val profileObj = Obj()
      
      // Iterate through each mapping
      mappings.foreach { case (sourceField, targetPath) =>
        try {
          // Get value from observation data
          val value = observationData.get(sourceField)
          
          // Check if value is null or empty, and skip if so
          if (isValueEmpty(value)) {
            println(s"[FieldMapper] Source field '$sourceField' is null or empty, skipping")
          } else {
            // Parse target path (e.g., "profile.phone" -> ["profile", "phone"])
            val pathParts = targetPath.split("\\.")
            
            if (pathParts.length >= 2) {
              val rootKey = pathParts(0) // e.g., "profile"
              val fieldKey = pathParts(1) // e.g., "phone"
              
              // We expect all mappings to be under "profile", so rootKey should be "profile"
              if (rootKey == "profile") {
                // Set the field value directly in profile object
                value match {
                  case s: String => profileObj.value(fieldKey) = s
                  case n: Number => 
                    // Convert Java Number to ujson numeric value
                    profileObj.value(fieldKey) = n.doubleValue()
                  case i: Int => profileObj.value(fieldKey) = i
                  case l: Long => profileObj.value(fieldKey) = l
                  case d: Double => profileObj.value(fieldKey) = d
                  case f: Float => profileObj.value(fieldKey) = f
                  case b: Boolean => profileObj.value(fieldKey) = b
                  case _ => profileObj.value(fieldKey) = value.toString
                }
                
                println(s"[FieldMapper] Mapped $sourceField -> $targetPath = $value")
              } else {
                println(s"[FieldMapper] WARNING: Root key '$rootKey' is not 'profile', skipping field $sourceField")
              }
            } else {
              println(s"[FieldMapper] WARNING: Invalid target path format: $targetPath (expected format: 'profile.field')")
            }
          }
        } catch {
          case e: Exception =>
            logger.error(s"[FieldMapper] Error mapping field $sourceField: ${e.getMessage}", e)
            println(s"[FieldMapper] ERROR mapping field $sourceField: ${e.getMessage}")
        }
      }
      
      // Return the result with "profile" as root
      val result = Obj("profile" -> profileObj)
      println(s"[FieldMapper] Transformation complete. Result: ${result.render()}")
      result
      
    } catch {
      case e: Exception =>
        logger.error(s"[FieldMapper] Error during transformation: ${e.getMessage}", e)
        println(s"[FieldMapper] ERROR during transformation: ${e.getMessage}")
        e.printStackTrace()
        Obj("profile" -> Obj()) // Return empty profile on error
    }
  }
  
  /**
   * Get the mappings for debugging
   */
  def getMappings: Map[String, String] = mappings
}
