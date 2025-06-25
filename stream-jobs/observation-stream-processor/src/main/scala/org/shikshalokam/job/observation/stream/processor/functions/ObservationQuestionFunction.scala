package org.shikshalokam.job.observation.stream.processor.functions

import com.twitter.util.Config.intoOption
import org.shikshalokam.job.observation.stream.processor.task.ObservationStreamConfig
import org.shikshalokam.job.util.PostgresUtil

import scala.collection.immutable._

case class QuestionParams(
                           payload: Option[Map[String, Any]], questionId: String, solutionId: String, solutionName: String, submissionId: String, submissionNumber: Integer, userId: String, userRoleIds: String,
                           userRoles: String, programName: String, programId: String, observationName: String, observationId: String, value: String,
                           userStateName: String, userStateId: String, userDistrictName: String, userDistrictId: String, userBlockName: String, userBlockId: String, userClusterName: String, userClusterId: String, userSchoolName: String, userSchoolId: String,
                           tenantId: String, orgId: String, orgCode: String, orgName: String, statusOfSubmission: String, submittedAt: String, entityType: String,
                           parentOneName: String, parentOneId: String, parentTwoName: String, parentTwoId: String, parentThreeName: String, parentThreeId: String, parentFourName: String, parentFourId: String, parentFiveName: String, parentFiveId: String,
                           domainName: String, criteriaName: String, score: Integer, hasParentQuestion: Boolean, parentQuestionText: String, evidences: String, remarks: String
                         )

class ObservationQuestionFunction(postgresUtil: PostgresUtil, config: ObservationStreamConfig, questionTable: String) {

  def processQuestionType(params: QuestionParams, questionType: String): Unit = {
    val question = extractField(params.payload, "question")
    val labels = extractField(params.payload, "labels")

    val questionParam = Seq(
      params.solutionId, params.solutionName, params.submissionId, params.submissionNumber, params.userId, params.userRoleIds,
      params.userRoles, params.programName, params.programId, params.observationName, params.observationId,
      params.userStateName, params.userStateId, params.userDistrictName, params.userDistrictId,
      params.userBlockName, params.userBlockId, params.userClusterName, params.userClusterId,
      params.userSchoolName, params.userSchoolId, params.tenantId, params.orgId, params.orgCode,
      params.orgName, params.statusOfSubmission, params.submittedAt, params.entityType,
      params.parentOneName, params.parentOneId, params.parentTwoName, params.parentTwoId,
      params.parentThreeName, params.parentThreeId, params.parentFourName, params.parentFourId,
      params.parentFiveName, params.parentFiveId, params.domainName, params.criteriaName,
      params.questionId, question, params.value, params.score.orNull, params.hasParentQuestion,
      params.parentQuestionText, params.evidences, params.remarks, questionType, labels
    )

    postgresUtil.executePreparedUpdate(insertQuestionQuery, questionParam, questionTable, params.solutionId)
  }

  private val insertQuestionQuery =
    s"""INSERT INTO $questionTable (
       |    solution_id, solution_name, submission_id, submission_number, user_id, user_role_ids, user_roles, program_name, program_id,
       |    observation_name, observation_id, user_state_name, user_state_id, user_district_name, user_district_id,
       |    user_block_name, user_block_id, user_cluster_name, user_cluster_id, user_school_name, user_school_id,
       |    tenant_id, org_id, org_code, org_name, status_of_submission, submitted_at, entityType, parent_one_name,
       |    parent_one_id, parent_two_name, parent_two_id, parent_three_name, parent_three_id, parent_four_name,
       |    parent_four_id, parent_five_name, parent_five_id, domain_name, criteria_name, question_id, question_text,
       |    value, score, has_parent_question, parent_question_text, evidence, remarks, question_type, labels
       |) VALUES (
       |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
       |)""".stripMargin

  private def extractField(payload: Option[Map[String, Any]], key: String): String = {
    payload.flatMap(_.get(key)) match {
      case Some(qList: List[_]) =>
        qList.collect { case q if q != null && q.toString.nonEmpty => q.toString }.mkString(" | ")
      case _ => ""
    }
  }
}
