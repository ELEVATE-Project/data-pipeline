package org.shikshalokam.job.users.via.csv.services

import akka.actor.{Actor, Props}
import org.shikshalokam.job.users.via.csv.functions.MetabaseUserManagementFunction

// Actor to process CSV in the background
class CsvProcessingActor extends Actor {
  override def receive: Receive = {
    case filename: String =>
      println(s"------------ Start Processing CSV file $filename ------------")
      MetabaseUserManagementFunction.processCsvFile(filename)
      println(s"CSV processing for file $filename completed.\n\n")
  }
}

object CsvProcessingActor {
  def props: Props = Props[CsvProcessingActor]
}
