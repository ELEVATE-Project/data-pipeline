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
                             user_id: String, submission_number: Integer, value: String, state_name: String, district_name: String,
                             block_name: String, cluster_name: String, school_name: String, domain_name: String, criteria_name: String, has_parent_question: Boolean,
                             parent_question_text: String, question_type: String, score: Option[Integer] = None): Unit = {
    val question = extractField(payload, "question")
    val labels = extractField(payload, "labels")

    val insertQuestionQuery =
      s"""INSERT INTO $questionTable (
         |    solution_id, solution_name ,submission_id, user_id, state_name, district_name, block_name, cluster_name, school_name, org_name,
         |    question_id, question_text, value, score, domain_name, criteria_name, has_parent_question, parent_question_text, evidence,
         |    submitted_at, remarks, question_type, labels
         |) VALUES (
         |   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         |);
         |""".stripMargin

    val questionParam = Seq(
      solution_id, solution_name, submission_id, user_id, state_name, district_name, block_name, cluster_name, school_name, null,
      question_id, question, value, score.orNull, domain_name, criteria_name, has_parent_question, parent_question_text, null,
      null, null, question_type, labels
    )

    postgresUtil.executePreparedUpdate(insertQuestionQuery, questionParam, questionTable, solution_id)
  }

  def textQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                       user_id: String, submission_number: Integer, value: String, state_name: String, district_name: String,
                       block_name: String, cluster_name: String, school_name: String, domain_name: String, criteria_name: String, has_parent_question: Boolean,
                       parent_question_text: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, submission_number, value, state_name, district_name, block_name, cluster_name, school_name, domain_name, criteria_name, has_parent_question, parent_question_text, "text")
  }

  def radioQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                        user_id: String, submission_number: Integer, value: String, state_name: String, district_name: String,
                        block_name: String, cluster_name: String, school_name: String, domain_name: String, criteria_name: String, score: Integer, has_parent_question: Boolean,
                        parent_question_text: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, submission_number, value, state_name, district_name, block_name, cluster_name, school_name, domain_name, criteria_name, has_parent_question, parent_question_text, "radio", Some(score))
  }

  def dateQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                       user_id: String, submission_number: Integer, value: String, state_name: String, district_name: String,
                       block_name: String, cluster_name: String, school_name: String, domain_name: String, criteria_name: String, score: Integer, has_parent_question: Boolean,
                       parent_question_text: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, submission_number, value, state_name, district_name, block_name, cluster_name, school_name, domain_name, criteria_name, has_parent_question, parent_question_text, "date", Some(score))
  }

  def multiselectQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                              user_id: String, submission_number: Integer, value: String, state_name: String, district_name: String,
                              block_name: String, cluster_name: String, school_name: String, domain_name: String, criteria_name: String, has_parent_question: Boolean,
                              parent_question_text: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, submission_number, value, state_name, district_name, block_name, cluster_name, school_name, domain_name, criteria_name, has_parent_question, parent_question_text, "multiselect")
  }

  def numberQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                         user_id: String, submission_number: Integer, value: String, state_name: String, district_name: String,
                         block_name: String, cluster_name: String, school_name: String, domain_name: String, criteria_name: String, has_parent_question: Boolean,
                         parent_question_text: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, submission_number, value, state_name, district_name, block_name, cluster_name, school_name, domain_name, criteria_name, has_parent_question, parent_question_text, "number")
  }

  def sliderQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String, submission_id: String,
                         user_id: String, submission_number: Integer, value: String, state_name: String, district_name: String,
                         block_name: String, cluster_name: String, school_name: String, domain_name: String, criteria_name: String, has_parent_question: Boolean,
                         parent_question_text: String): Unit = {
    insertQuestion(payload, question_id, solution_id, solution_name, submission_id, user_id, submission_number, value, state_name, district_name, block_name, cluster_name, school_name, domain_name, criteria_name, has_parent_question, parent_question_text, "slider")
  }
}
