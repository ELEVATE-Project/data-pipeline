package org.shikshalokam.job.observation.stream.processor.functions

import org.shikshalokam.job.observation.stream.processor.task.ObservationStreamConfig
import org.shikshalokam.job.util.PostgresUtil

import scala.collection.immutable._


class ObservationQuestionFunction(postgresUtil: PostgresUtil, config: ObservationStreamConfig, questionTable: String) {

  private def extractField(payload: Option[Map[String, Any]], key: String): String = {
    payload.flatMap(_.get(key)) match {
      case Some(qList: List[_]) =>
        qList.collect { case q if q != null => q.toString }.headOption.getOrElse("")
      case _ => ""
    }
  }

  private def insertQuestion(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                             user_id: String, user_roles: String, program_name: String, program_id: String, observation_name: String, observation_id: String,
                             value: String, state_name: String, district_name: String, block_name: String, cluster_name: String,
                             school_name: String, school_id: String, org_name: String, domain_name: String, criteria_name: String, has_parent_question: Boolean,
                             parent_question_text: String, question_type: String, evidences: String, remarks: String, score: Option[Integer] = None, completed_date: String): Unit = {

    val question = extractField(payload, "question")
    val labels = extractField(payload, "labels")

    val insertQuestionQuery =
      s"""INSERT INTO $questionTable (
         |    solution_id, solution_name ,submission_id, user_id, user_roles, program_name, program_id, observation_name, observation_id,
         |    state_name, district_name, block_name, cluster_name, school_name, school_id, org_name, question_id, question_text, value, score, domain_name,
         |    criteria_name, has_parent_question, parent_question_text, evidence, submitted_at, remarks, question_type, labels
         |) VALUES (
         |   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         |);
         |""".stripMargin

    val questionParam = Seq(
      solution_id, solution_name, submission_id, user_id, user_roles, program_name, program_id, observation_name, observation_id, state_name,
      district_name, block_name, cluster_name, school_name, school_id, org_name, question_id, question, value, score.orNull, domain_name, criteria_name,
      has_parent_question, parent_question_text, evidences, completed_date, remarks, question_type, labels
    )

    // Check for null values in parameters
    questionParam.zipWithIndex.foreach { case (param, index) =>
      if (param == null) println(s"Debug: Null value found at index $index in questionParam")
    }

    postgresUtil.executePreparedUpdate(insertQuestionQuery, questionParam, questionTable, solution_id)
  }

  def textQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                       user_id: String, user_roles: String, program_name: String, program_id: String, observation_name: String, observation_id: String, value: String, state_name: String, district_name: String,
                       block_name: String, cluster_name: String, school_name: String, school_id: String, org_name: String, domain_name: String, criteria_name: String, score: Integer, has_parent_question: Boolean,
                       parent_question_text: String, evidence: String, remarks: String, completed_date: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, user_roles, program_name, program_id, observation_name, observation_id, value, state_name, district_name, block_name, cluster_name,
      school_name, school_id, org_name, domain_name, criteria_name, has_parent_question, parent_question_text, "text", evidence, remarks, Some(score), completed_date)
  }

  def radioQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                        user_id: String, user_roles: String, program_name: String, program_id: String, observation_name: String, observation_id: String, value: String, state_name: String, district_name: String,
                        block_name: String, cluster_name: String, school_name: String, school_id: String, org_name: String, domain_name: String, criteria_name: String, score: Integer, has_parent_question: Boolean,
                        parent_question_text: String, evidence: String, remarks: String, completed_date: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, user_roles, program_name, program_id,
      observation_name, observation_id, value, state_name, district_name, block_name, cluster_name, school_name, school_id, org_name, domain_name, criteria_name, has_parent_question, parent_question_text, "radio", evidence, remarks, Some(score), completed_date)
  }

  def dateQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                       user_id: String, user_roles: String, program_name: String, program_id: String, observation_name: String, observation_id: String, value: String, state_name: String, district_name: String,
                       block_name: String, cluster_name: String, school_name: String, school_id: String, org_name: String, domain_name: String, criteria_name: String, score: Integer, has_parent_question: Boolean,
                       parent_question_text: String, evidence: String, remarks: String, completed_date: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, user_roles, program_name, program_id,
      observation_name, observation_id, value, state_name, district_name, block_name, cluster_name, school_name, school_id, org_name, domain_name, criteria_name, has_parent_question, parent_question_text, "date", evidence, remarks, Some(score), completed_date)
  }

  def multiselectQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                              user_id: String, user_roles: String, program_name: String, program_id: String, observation_name: String, observation_id: String, value: String, state_name: String, district_name: String,
                              block_name: String, cluster_name: String, school_name: String, school_id: String, org_name: String, domain_name: String, criteria_name: String, score: Integer, has_parent_question: Boolean,
                              parent_question_text: String, evidence: String, remarks: String, completed_date: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, user_roles, program_name, program_id,
      observation_name, observation_id, value, state_name, district_name, block_name, cluster_name, school_name, school_id, org_name, domain_name, criteria_name, has_parent_question, parent_question_text, "multiselect", evidence, remarks, Some(score), completed_date)
  }

  def numberQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                         user_id: String, user_roles: String, program_name: String, program_id: String, observation_name: String, observation_id: String, value: String, state_name: String, district_name: String,
                         block_name: String, cluster_name: String, school_name: String, school_id: String, org_name: String, domain_name: String, criteria_name: String, score: Integer, has_parent_question: Boolean,
                         parent_question_text: String, evidence: String, remarks: String, completed_date: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, user_roles, program_name, program_id,
      observation_name, observation_id, value, state_name, district_name, block_name, cluster_name, school_name, school_id, org_name, domain_name, criteria_name, has_parent_question, parent_question_text, "number", evidence, remarks, Some(score), completed_date)
  }

  def sliderQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                         user_id: String, user_roles: String, program_name: String, program_id: String, observation_name: String, observation_id: String, value: String, state_name: String, district_name: String,
                         block_name: String, cluster_name: String, school_name: String, school_id: String, org_name: String, domain_name: String, criteria_name: String, score: Integer, has_parent_question: Boolean,
                         parent_question_text: String, evidence: String, remarks: String, completed_date: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, user_roles, program_name, program_id,
      observation_name, observation_id, value, state_name, district_name, block_name, cluster_name, school_name, school_id, org_name, domain_name, criteria_name, has_parent_question, parent_question_text, "slider", evidence, remarks, Some(score), completed_date)
  }
}
