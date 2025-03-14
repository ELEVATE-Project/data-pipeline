package org.shikshalokam.job.util

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

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
      //println(s"${tableName} table created successfully.")
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

  def executePreparedUpdate(query: String, params: Seq[Any], table: String, id: String): Unit = {
    val connection = getConnection
    var preparedStatement: PreparedStatement = null
    try {
      preparedStatement = connection.prepareStatement(query)
      // Loop through params and set them to the PreparedStatement
      for ((param, index) <- params.zipWithIndex) {
        param match {
          case v: String => preparedStatement.setString(index + 1, v)
          case v: Int => preparedStatement.setInt(index + 1, v)
          case v: Boolean => preparedStatement.setBoolean(index + 1, v)
          case null => preparedStatement.setNull(index + 1, java.sql.Types.NULL)
          case _ => throw new IllegalArgumentException(s"Unsupported parameter type at index ${index + 1}")
        }
      }
      preparedStatement.executeUpdate()
      println(s"Data inserted into ${table} table successfully with id $id.")
    } catch {
      case e: SQLException =>
        println("Error inserting data: " + e.getMessage)
        throw e
    } finally {
      if (preparedStatement != null) preparedStatement.close()
      connection.close()
    }
  }

  def fetchData(query: String): List[Map[String, Any]] = {
    val connection = getConnection
    var result = List[Map[String, Any]]()
    try {
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(query)
      val metaData = resultSet.getMetaData
      val columnCount = metaData.getColumnCount

      while (resultSet.next()) {
        val row = (1 to columnCount).map { i =>
          metaData.getColumnName(i) -> resultSet.getObject(i)
        }.toMap
        result = result :+ row
      }
       //println("Fetch data query executed successfully.")
      result

    } catch {
      case e: SQLException =>
        println("Error fetching data: " + e.getMessage)
        throw e
    } finally {
      connection.close()
    }
  }

  def insertData(query: String): Int = {
    val connection = getConnection
    try {
      val statement = connection.createStatement()
      val affectedRows = statement.executeUpdate(query)
      //println("Insert query executed successfully.")
      affectedRows

    } catch {
      case e: SQLException =>
        println("Error inserting data: " + e.getMessage)
        throw e
    } finally {
      connection.close()
    }
  }

}
