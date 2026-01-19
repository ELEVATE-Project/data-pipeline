package org.shikshalokam.job.mitra.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest

class StoryEvent(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def id: Int = readOrDefault[String]("id", null).replace(",", "").toInt

  def title: String = readOrDefault[String]("Title", null)

  def content: String = readOrDefault[String]("content", null)

  def pdfLink: String = readOrDefault[String]("Pdf", null)

  def imageLinks: String = readOrDefault[String]("Images", null)

  def role: String = readOrDefault[String]("designation", null)

  def district: String = readOrDefault[String]("District", null)

  def state: String = readOrDefault[String]("state", null)

  def actionSteps: String = readOrDefault[String]("Action Steps", null)

  def impact: String = readOrDefault[String]("impact", null)

  def challenges: String = readOrDefault[String]("Challenges", null)

  def maskedContent: String = readOrDefault[String]("masked_content", null)

}

