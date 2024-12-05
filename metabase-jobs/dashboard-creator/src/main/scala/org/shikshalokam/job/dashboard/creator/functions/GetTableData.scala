package org.shikshalokam.job.dashboard.creator.functions

import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import scala.collection.JavaConverters._

object GetTableData {
  def getTableMetadataId(databaseId: Int, metabaseUtil: MetabaseUtil, tableName: String, columnName: String, postgresUtil: PostgresUtil, metaTableQuery: String): Int = {
    val metadataJson = mapper.readTree(metabaseUtil.getDatabaseMetadata(databaseId))
    metadataJson.path("tables").elements().asScala
      .find(_.path("name").asText() == s"$tableName")
      .flatMap(table => table.path("fields").elements().asScala
        .find(_.path("name").asText() == s"$columnName"))
      .map(field => {
        val fieldId = field.path("id").asInt()
        println(s"Field ID for $columnName: $fieldId")
        fieldId
      }).getOrElse {
        val errorMessage = s"$columnName field not found"
        val updateTableQuery = metaTableQuery.replace("'errorMessage'", s"'${errorMessage.replace("'", "''")}'")
        postgresUtil.insertData(updateTableQuery)
        throw new Exception(s"$columnName field not found")
      }
  }
}

