package org.shikshalokam.job.domain.reader

trait ParentType {
  def readChild[T]: Option[T]

  def addChild(value: Any): Unit
}
