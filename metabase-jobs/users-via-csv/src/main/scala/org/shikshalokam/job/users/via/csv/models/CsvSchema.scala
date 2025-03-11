package org.shikshalokam.job.users.via.csv.models

case class FileNames(files: List[String])

object CsvSchema {
  val headers: List[String] = List("firstName", "lastName", "email", "password", "roles", "stateId", "districtId", "programName")
}
