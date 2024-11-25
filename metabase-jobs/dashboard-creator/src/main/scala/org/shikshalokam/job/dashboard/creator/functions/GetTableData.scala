package org.shikshalokam.job.dashboard.creator.functions

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.shikshalokam.job.util.MetabaseUtil
import scala.collection.immutable._


object GetTableData {
  implicit val formats: DefaultFormats.type = DefaultFormats

  // Function to extract table names and IDs
  def extractTables(metadata: String): List[Map[String, Any]] = {
    // Parse JSON and extract the "tables" section
    (for {
      JObject(table) <- parse(metadata) \ "tables"
      JField("name", JString(name)) <- table
      JField("id", JInt(id)) <- table
    } yield Map("name" -> name, "id" -> id)).toList
  }

  // Function to extract table names and field IDs from nested JSON
  def extractTablesAndFields(metadata: String): List[Map[String, Any]] = {
    // Parse JSON and extract the "tables" section and their "fields"
    (for {
      JObject(table) <- parse(metadata) \ "tables"
      JField("name", JString(tableName)) <- table
      JArray(fields) = table.find(_._1 == "fields").map(_._2).getOrElse(JArray(Nil))
      JObject(field) <- fields
      JField("name", JString(fieldName)) <- field
      JField("id", JInt(fieldId)) <- field
    } yield Map("table" -> tableName, "field_name" -> fieldName, "field_id" -> fieldId)).toList
  }

  def getMetadataJson(metabaseUtil: MetabaseUtil = null,databaseId: Int): String = {
    val metadata = metabaseUtil.getDatabaseMetadata(databaseId) // Directly get the String
    val tables = extractTables(metadata)
    val tablesAndFields = extractTablesAndFields(metadata)

    val result = JObject(
      "tables" -> JArray(tables.map { table =>
        JObject(
          "name" -> JString(table("name").toString),
          "id" -> JInt(table("id") match {
            case b: BigInt if b.isValidInt => b.intValue
            case i: Int => i
            case other => throw new IllegalArgumentException(s"Unexpected type for table id: ${other.getClass}")
          })
        )
      }),
      "fields" -> JArray(tablesAndFields.map { field =>
        JObject(
          "table" -> JString(field("table").toString),
          "field_name" -> JString(field("field_name").toString),
          "field_id" -> JInt(field("field_id") match {
            case b: BigInt if b.isValidInt => b.intValue
            case i: Int => i
            case other => throw new IllegalArgumentException(s"Unexpected type for field_id: ${other.getClass}")
          })
        )
      })
    )

    // Convert to JSON string with pretty formatting
    Serialization.writePretty(result)
  }

  def getFieldId(metabaseUtil: MetabaseUtil = null, databaseId: Int, tableName: String, fieldName: String): Int = {
    val metadata = getMetadataJson(metabaseUtil, databaseId)
    val parsedJson = parse(metadata)

    // Extract the field ID based on table and field name
    (for {
      JObject(field) <- parsedJson \ "fields"
      JField("table", JString(t)) <- field if t == tableName
      JField("field_name", JString(f)) <- field if f == fieldName
      JField("field_id", JInt(fieldId)) <- field
    } yield fieldId.toInt).head
  }

}

