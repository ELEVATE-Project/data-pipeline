package org.shikshalokam.job.user.mapping.stream.processor.util

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import ujson.{Arr, Js, Null, Obj, read, write}

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
   * Convert a Java Map to ujson.Obj recursively
   */
  private def javaMapToUjsonObj(map: java.util.Map[_, _]): ujson.Obj = {
    val obj = Obj()
    map.asScala.foreach { case (k, v) =>
      val key = k.toString
      obj.value(key) = anyToUjsonValue(v)
    }
    obj
  }
  
  /**
   * Convert a Java Collection to ujson.Arr recursively
   */
  private def javaCollectionToUjsonArr(coll: java.util.Collection[_]): ujson.Arr = {
    val arr = ujson.Arr()
    coll.asScala.foreach { item =>
      arr.value += anyToUjsonValue(item)
    }
    arr
  }
  
  /**
   * Convert any value to ujson.Value recursively
   */
  private def anyToUjsonValue(value: Any): ujson.Value = {
    value match {
      case null => ujson.Null
      case s: String => s
      case n: Number => n.doubleValue()
      case i: Int => i
      case l: Long => l
      case d: Double => d
      case f: Float => f
      case b: Boolean => b
      case map: java.util.Map[_, _] => javaMapToUjsonObj(map)
      case coll: java.util.Collection[_] => javaCollectionToUjsonArr(coll)
      case arr: Array[_] => 
        val ujsonArr = ujson.Arr()
        arr.foreach { item => ujsonArr.value += anyToUjsonValue(item) }
        ujsonArr
      case _ => value.toString
    }
  }
  
  /**
   * Check if a value is null or empty
   * Used to determine if a field should be skipped during mapping
   * 
   * Rule: Update a field only if it has a valid value
   * If a field is null, empty string, or missing → do not update it
   * 
   * @param value The value to check
   * @return true if value is null, empty string, or empty collection (should be skipped)
   */
  def isValueEmpty(value: Any): Boolean = {
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
   * Transform observation data (userProfile) to user profile patch format
   * 
   * Only maps the following allowed fields from userProfile:
   * - name
   * - username
   * - dob
   * - phoneCode (from phone_code)
   * - about
   * - preferredLanguage (from preferred_language)
   * - tenantCode (from tenant_code)
   * - meta
   * 
   * Important Rule: Update a field only if it has a valid value
   * If a field is null, empty string, or missing → do not update it
   * Existing user data should not be overwritten with empty values
   * 
   * Input observationData (userProfile) example:
   * {
   *   "name": "Carol Miranda Updated Two",
   *   "about": "admin Update",
   *   "dob": "22-12-1990",
   *   "phone_code": "+91",
   *   "preferred_language": "en",
   *   "tenant_code": "qa",
   *   "meta": {"key": "value"}
   * }
   * 
   * Output profile format:
   * {
   *   "profile": {
   *     "name": "Carol Miranda Updated Two",
   *     "about": "admin Update",
   *     "dob": "22-12-1990",
   *     "phoneCode": "+91",
   *     "preferredLanguage": "en",
   *     "tenantCode": "qa",
   *     "meta": {"key": "value"}
   *   }
   * }
   * 
   * @param observationData Map containing userProfile field values from observation event
   * @return JsObject representing the profile patch structure with only non-empty fields
   */
  def transform(observationData: util.Map[String, Any]): Js.Obj = {
    try {
      println(s"[FieldMapper] Starting transformation of userProfile data")
      println(s"[FieldMapper] Input userProfile keys: ${observationData.keySet().asScala.mkString(", ")}")
      
      val profileObj = Obj()
      var mappedFieldsCount = 0
      
      // Iterate through each configured mapping (only allowed fields are in the config)
      mappings.foreach { case (sourceField, targetPath) =>
        try {
          // Get value from observation data (userProfile)
          val value = observationData.get(sourceField)
          
          // Check if value is null or empty, and skip if so
          // This ensures we only update fields with valid values
          if (isValueEmpty(value)) {
            println(s"[FieldMapper] Source field '$sourceField' is null, empty, or missing - skipping (will not overwrite existing user data)")
          } else {
            // Parse target path (e.g., "profile.phoneCode" -> ["profile", "phoneCode"])
            val pathParts = targetPath.split("\\.")
            
            if (pathParts.length >= 2) {
              val rootKey = pathParts(0) // e.g., "profile"
              val fieldKey = pathParts(1) // e.g., "phoneCode"
              
              // We expect all mappings to be under "profile", so rootKey should be "profile"
              if (rootKey == "profile") {
                // Set the field value directly in profile object
                // Only non-empty values reach this point
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
                  case map: java.util.Map[_, _] => 
                    // Handle nested objects like meta - convert directly to ujson.Obj
                    try {
                      profileObj.value(fieldKey) = javaMapToUjsonObj(map)
                    } catch {
                      case e: Exception =>
                        logger.warn(s"[FieldMapper] Could not convert map to JSON for field $fieldKey, using string representation", e)
                        profileObj.value(fieldKey) = map.toString
                    }
                  case coll: java.util.Collection[_] =>
                    // Handle collections - convert directly to ujson.Arr
                    try {
                      profileObj.value(fieldKey) = javaCollectionToUjsonArr(coll)
                    } catch {
                      case e: Exception =>
                        logger.warn(s"[FieldMapper] Could not convert collection to JSON for field $fieldKey, using string representation", e)
                        profileObj.value(fieldKey) = coll.toString
                    }
                  case _ => profileObj.value(fieldKey) = value.toString
                }
                
                mappedFieldsCount += 1
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
      println(s"[FieldMapper] Transformation complete. Mapped $mappedFieldsCount non-empty fields. Result: ${result.render()}")
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
