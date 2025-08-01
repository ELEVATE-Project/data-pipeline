package org.shikshalokam.job.user.dashboard.creator.functions

import org.shikshalokam.job.util.JSONUtil.mapper
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import scala.collection.JavaConverters._
import scala.concurrent.blocking

object GetTableData {
  def getTableMetadataId(databaseId: Int, metabaseUtil: MetabaseUtil, tableName: String, columnName: String, postgresUtil: PostgresUtil, metaTableQuery: String): Int = {
    var attempts = 0
    val maxRetries = 3
    val retryDelayMillis = 10000 // 10 seconds

    while (attempts < maxRetries) {
      try {
        val metadataJson = mapper.readTree(metabaseUtil.getDatabaseMetadata(databaseId))
        return metadataJson.path("tables").elements().asScala
          .find(_.path("name").asText() == s"$tableName")
          .flatMap(table => table.path("fields").elements().asScala
            .find(_.path("name").asText() == s"$columnName"))
          .map(field => {
            val fieldId = field.path("id").asInt()
            println(s"Field ID for $columnName: $fieldId")
            fieldId
          }).getOrElse {
            throw new Exception(s"$columnName field not found")
          }
      } catch {
        case e: Exception =>
          attempts += 1
          if (attempts >= maxRetries) {
            val errorMessage = s"$columnName field not found after $maxRetries attempts"
            val updateTableQuery = metaTableQuery.replace("'errorMessage'", s"'${errorMessage.replace("'", "''")}'")
            postgresUtil.insertData(updateTableQuery)
            throw new Exception(errorMessage, e)
          } else {
            println(s"Retrying... Attempt $attempts of $maxRetries. Error: ${e.getMessage}")
            blocking(Thread.sleep(retryDelayMillis))
          }
      }
    }
    throw new Exception(s"Unexpected error while fetching $columnName field")
  }
}

