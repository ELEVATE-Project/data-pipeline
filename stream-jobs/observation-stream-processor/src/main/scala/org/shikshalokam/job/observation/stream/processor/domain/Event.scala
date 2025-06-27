package org.shikshalokam.job.observation.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def _id: String = readOrDefault[String]("_id", "")

  def solutionId: String = readOrDefault[String]("solutionId", "")

  def solutionName: String = readOrDefault[String]("solutionInfo.name", "")

  def solutionExternalId: String = readOrDefault[String]("solutionExternalId", "")

  def solutionDescription: String = readOrDefault[String]("solutionInfo.description", "")

  def createdBy: String = readOrDefault[String]("createdBy", "")

  def status: String = readOrDefault[String]("status", "")

  def submissionNumber: Integer = readOrDefault[Integer]("submissionNumber", 0)

  def programName: String = readOrDefault[String]("programInfo.name", "")

  def programId: String = readOrDefault[String]("programInfo._id", "")

  def programExternalId: String = readOrDefault[String]("programExternalId", "")

  def programDescription: String = readOrDefault[String]("programInfo.description", "")

  def observationName: String = readOrDefault[String]("observationInformation.name", "")

  def observationId: String = readOrDefault[String]("observationId", "")

  def userOneProfileName: String = readOrDefault[String]("userProfile.state.label", "")

  def userOneProfileId: String = readOrDefault[String]("userProfile.state.value", "")

  def userTwoProfileName: String = readOrDefault[String]("userProfile.district.label", "")

  def userTwoProfileId: String = readOrDefault[String]("userProfile.district.value", "")

  def userThreeProfileName: String = readOrDefault[String]("userProfile.block.label", "")

  def userThreeProfileId: String = readOrDefault[String]("userProfile.block.value", "")

  def userFourProfileName: String = readOrDefault[String]("userProfile.cluster.label", "")

  def userFourProfileId: String = readOrDefault[String]("userProfile.cluster.value", "")

  def userFiveProfileName: String = readOrDefault[String]("userProfile.school.label", "")

  def userFiveProfileId: String = readOrDefault[String]("userProfile.school.externalId", "")

  def orgName: String = readOrDefault[String]("userProfile.organization.name", "")

  def organisationId: String = readOrDefault[Int]("orgId", 0).toString

  def tenantId: String = readOrDefault[String]("tenantId", "")

  def organisation: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("userProfile.organizations", List.empty)

  def themes: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("themes", null)

  def criteria: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("criteria", null)

  def answers: Map[String, Any] = readOrDefault[Map[String, Any]]("answers", null)

  def userRoles: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("userProfile.user_roles", null)

  def isRubric: Boolean = readOrDefault[Boolean]("isRubricDriven", false)

  def completedDate: String = readOrDefault[String]("completedDate", "")

  def entityType: String = readOrDefault[String]("entityType", null)

  def parentOneName: String = readOrDefault[Seq[Map[String, Any]]]("entityInformation.parentInformation.state", Seq.empty).headOption.flatMap(_.get("name")).map(_.toString).getOrElse(null)

  def parentOneId: String = readOrDefault[Seq[Map[String, Any]]]("entityInformation.parentInformation.state", Seq.empty).headOption.flatMap(_.get("_id")).map(_.toString).getOrElse(null)

  def parentTwoName: String = readOrDefault[Seq[Map[String, Any]]]("entityInformation.parentInformation.district", Seq.empty).headOption.flatMap(_.get("name")).map(_.toString).getOrElse(null)

  def parentTwoId: String = readOrDefault[Seq[Map[String, Any]]]("entityInformation.parentInformation.district", Seq.empty).headOption.flatMap(_.get("_id")).map(_.toString).getOrElse(null)

  def parentThreeName: String = readOrDefault[Seq[Map[String, Any]]]("entityInformation.parentInformation.block", Seq.empty).headOption.flatMap(_.get("name")).map(_.toString).getOrElse(null)

  def parentThreeId: String = readOrDefault[Seq[Map[String, Any]]]("entityInformation.parentInformation.block", Seq.empty).headOption.flatMap(_.get("_id")).map(_.toString).getOrElse(null)

  def parentFourName: String = readOrDefault[Seq[Map[String, Any]]]("entityInformation.parentInformation.cluster", Seq.empty).headOption.flatMap(_.get("name")).map(_.toString).getOrElse(null)

  def parentFourId: String = readOrDefault[Seq[Map[String, Any]]]("entityInformation.parentInformation.cluster", Seq.empty).headOption.flatMap(_.get("_id")).map(_.toString).getOrElse(null)

  def parentFiveName: String = readOrDefault[Seq[Map[String, Any]]]("entityInformation.parentInformation.school", Seq.empty).headOption.flatMap(_.get("name")).map(_.toString).getOrElse(null)

  def parentFiveId: String = readOrDefault[Seq[Map[String, Any]]]("entityInformation.parentInformation.school", Seq.empty).headOption.flatMap(_.get("externalId")).map(_.toString).getOrElse(null)

}

