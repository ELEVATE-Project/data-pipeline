package org.shikshalokam.job.akkaservice.services

import akka.actor.{Actor, Props}
import org.shikshalokam.job.akkaservice.functions.Functions

// Actor to process CSV in the background
class CsvProcessingActor extends Actor {
  override def receive: Receive = {
    case filename: String =>
      println(s"------------ Start Processing CSV file $filename ------------")
      Functions.processCsvFile(filename)
      println(s"CSV processing for file $filename completed.\n\n")
  }
}

object CsvProcessingActor {
  def props: Props = Props[CsvProcessingActor]
}
