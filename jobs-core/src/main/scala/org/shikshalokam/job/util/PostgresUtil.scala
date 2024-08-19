package org.shikshalokam.job.util

import java.sql.{Connection, DriverManager, SQLException}

class PostgresUtil(dbUrl: String, dbUser: String, dbPassword: String) {

  private val url: String = dbUrl
  private val user: String = dbUser
  private val password: String = dbPassword

  /**
   * Load PostgreSQL JDBC Driver and Establish the connection
   */
  def getConnection: Connection = {
    try {
      Class.forName("org.postgresql.Driver")
      DriverManager.getConnection(url, user, password)
    } catch {
      case e: SQLException =>
        println("Error connecting to the database: " + e.getMessage)
        throw e
      case e: ClassNotFoundException =>
        println("PostgreSQL JDBC driver not found: " + e.getMessage)
        throw e
    }
  }

  def createTable(createTableQuery: String, tableName: String): Unit = {
    val connection = getConnection
    try {
      connection.createStatement().executeUpdate(createTableQuery)
      println(s"${tableName} table created successfully.")
    } catch {
      case e: SQLException =>
        println("Error creating table: " + e.getMessage)
        throw e
    } finally {
      connection.close()
    }
  }

  def executeUpdate(query: String, table: String, id: String): Unit = {
    val connection = getConnection
    try {
      connection.createStatement().executeUpdate(query)
      println(s"Data inserted into ${table} table successfully with id $id.")
    } catch {
      case e: SQLException =>
        println("Error inserting data: " + e.getMessage)
        throw e
    } finally {
      connection.close()
    }
  }

}
