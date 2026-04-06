package org.shikshalokam.job.survey.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.survey.stream.processor.domain.Event
import org.shikshalokam.job.survey.stream.processor.task.SurveyStreamConfig
import org.shikshalokam.job.util.{PostgresUtil, ScalaJsonUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.util
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.immutable._

class SurveyStreamFunction(config: SurveyStreamConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[SurveyStreamFunction])

  override def metricsList(): List[String] = {
    List(config.surveysCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val pgHost: String = config.pgHost
    val pgPort: String = config.pgPort
    val pgUsername: String = config.pgUsername
    val pgPassword: String = config.pgPassword
    val pgDataBase: String = config.pgDataBase
    val connectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$pgDataBase"
    postgresUtil = new PostgresUtil(connectionUrl, pgUsername, pgPassword)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    if (event.status.toLowerCase() == "started" || event.status.toLowerCase() == "inprogress" || event.status.toLowerCase() == "submitted" || event.status.toLowerCase() == "completed") {
      println(s"***************** Start of Processing the Survey Event with Id = ${event._id} *****************")
      val surveyQuestionTable = event.solutionId
      val surveyStatusTable = s""""${event.solutionId}_survey_status""""
      val surveyId = event._id
      val userId = event.createdBy
      val stateId = event.stateId
      val stateName = event.stateName
      val districtId = event.districtId
      val districtName = event.districtName
      val blockId = event.blockId
      val blockName = event.blockName
      val clusterId = event.clusterId
      val clusterName = event.clusterName
      val schoolId = event.schoolId
      val schoolName = event.schoolName
      val programName = event.programName
      val programId = event.programId
      val solutionName = event.solutionName
      val solutionId = event.solutionId
      val status = event.status
      val submissionDate = event.completedDate
      val tenantId = event.tenantId
      val solutionExternalId = event.solutionExternalId
      val solutionDescription = event.solutionDescription
      val programExternalId = event.programExternalId
      val programDescription = event.programDescription
      val privateProgram: Null = null
      val projectCategories: Null = null
      val projectDuration: Null = null

      var userRoleIds: String = ""
      var userRoles: String = ""
      var organisationId: String = ""
      var organisationName: String = ""
      var organisationCode: String = ""
      val parentOrgId: String = event.parentOrgId

      event.organisation.foreach { org =>
        if (org.get("code").contains(event.organisationId)) {
          organisationName = org.get("name").map(_.toString).getOrElse("")
          organisationId = org.get("id").map(_.toString).getOrElse("")
          organisationCode = org.get("code").map(_.toString).getOrElse("")
          val roles = org.get("roles").map(_.asInstanceOf[List[Map[String, Any]]]).getOrElse(List.empty)
          val (userRoleIdsExtracted, userRolesExtracted) = extractUserRolesData(roles)
          userRoleIds = userRoleIdsExtracted
          userRoles = userRolesExtracted
        } else {
          println(s"Organisation with ID ${event.organisationId} not found in the event data.")
        }
      }

      val statusDashboardFilters: List[Map[String, String]] = List(
        Map(
          "state_name" -> stateName,
          "district_name" -> districtName,
          "block_name" -> blockName,
          "cluster_name" -> clusterName,
          "school_name" -> schoolName,
          "organisation_name" -> organisationName,
          "table_name" -> surveyStatusTable
        )
      )

      val questionDashboardFilters: List[Map[String, String]] = List(
        Map(
          "state_name" -> stateName,
          "district_name" -> districtName,
          "block_name" -> blockName,
          "cluster_name" -> clusterName,
          "school_name" -> schoolName,
          "organisation_name" -> organisationName,
          "table_name" -> s""""$surveyQuestionTable""""
        )
      )

      val AlterSurveyQuestionsTableQuery =
        s"""ALTER TABLE IF EXISTS "$surveyQuestionTable"
           |ADD COLUMN IF NOT EXISTS tenant_id TEXT;
           |""".stripMargin

      val AlterSurveyStatusTableQuery =
        s"""ALTER TABLE IF EXISTS $surveyStatusTable
           |ADD COLUMN IF NOT EXISTS tenant_id TEXT;
           |""".stripMargin

      postgresUtil.createTable(config.createSolutionsTable, config.solutions)
      postgresUtil.createTable(config.createDashboardMetadataTable, config.dashboard_metadata)

      /**
       * Extracting Solution Data
       *
       */
      println("\n==> SURVEY - Solution Data ")
      println("solutionId = " + event.solutionId)
      println("solutionExternalId = " + event.solutionExternalId)
      println("solutionName = " + event.solutionName)
      println("solutionDescription = " + event.solutionDescription)
      println("duration = " + projectDuration)
      println("categories = " + projectCategories)
      println("programId = " + event.programId)
      println("programName = " + event.programName)
      println("programExternalId = " + event.programExternalId)
      println("programDescription = " + event.programDescription)
      println("privateProgram = " + privateProgram)

      val upsertSolutionQuery =
        s"""INSERT INTO ${config.solutions} (solution_id, external_id, name, description, duration, categories, program_id, program_name, program_external_id, program_description, private_program, org_id)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |ON CONFLICT (solution_id) DO UPDATE SET
           |    external_id = ?,
           |    name = ?,
           |    description = ?,
           |    duration = ?,
           |    categories = ?,
           |    program_id = ?,
           |    program_name = ?,
           |    program_external_id = ?,
           |    program_description = ?,
           |    private_program = ?,
           |    org_id = ?;
           |""".stripMargin

      val solutionParams = Seq(
        // Insert parameters
        solutionId, solutionExternalId, solutionName, solutionDescription, projectDuration, projectCategories, programId, programName, programExternalId, programDescription, privateProgram, parentOrgId,

        // Update parameters (matching columns in the ON CONFLICT clause)
        solutionExternalId, solutionName, solutionDescription, projectDuration, projectCategories, programId, programName, programExternalId, programDescription, privateProgram, parentOrgId
      )

      postgresUtil.executePreparedUpdate(upsertSolutionQuery, solutionParams, config.solutions, solutionId)


      /**
       * Extracting Survey Data per user submission
       *
       */
      println("\n==> Survey Data per user submission ")
      println("surveyId = " + event._id)
      println("userId = " + event.createdBy)
      println("userRoleIds = " + userRoleIds)
      println("userRoles = " + userRoles)
      println("stateId = " + event.stateId)
      println("stateName = " + event.stateName)
      println("districtId = " + event.districtId)
      println("districtName = " + event.districtName)
      println("blockId = " + event.blockId)
      println("blockName = " + event.blockName)
      println("clusterId = " + event.clusterId)
      println("clusterName = " + event.clusterName)
      println("schoolId = " + event.schoolId)
      println("schoolName = " + event.schoolName)
      println("tenantId = " + event.tenantId)
      println("organisationId = " + organisationId)
      println("organisationName = " + organisationName)
      println("organisationCode = " + organisationCode)
      println("programName = " + event.programName)
      println("programId = " + event.programId)
      println("solutionName = " + event.solutionName)
      println("solutionId = " + event.solutionId)
      println("status = " + event.status)
      println("submissionDate = " + event.completedDate)

      //CREATE TABLE IF NOT EXISTS
      postgresUtil.checkAndCreateTable(surveyStatusTable, config.createSurveyStatusTableQuery.replace("@surveyStatusTable", surveyStatusTable))
      postgresUtil.executeUpdate(AlterSurveyStatusTableQuery, surveyStatusTable, solutionId)

      val upsertSurveyDataQuery =
        s"""INSERT INTO $surveyStatusTable (
           |    survey_id, user_id, user_role_ids, user_roles, state_id, state_name, district_id, district_name,
           |    block_id, block_name, cluster_id, cluster_name, school_id, school_name, tenant_id, organisation_id,
           |    organisation_name, organisation_code, program_name, program_id, solution_name, solution_id,
           |    status, submission_date
           |) VALUES (
           |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
           |) ON CONFLICT (survey_id) DO UPDATE SET
           |    user_id = ?, user_role_ids = ?, user_roles = ?, state_id = ?, state_name = ?, district_id = ?, district_name = ?,
           |    block_id = ?, block_name = ?, cluster_id = ?, cluster_name = ?, school_id = ?, school_name = ?, tenant_id = ?, organisation_id = ?,
           |    organisation_name = ?, organisation_code = ?, program_name = ?, program_id = ?, solution_name = ?, solution_id = ?,
           |    status = ?, submission_date = ?;
           |""".stripMargin


      val surveyParams = Seq(
        // Insert parameters
        surveyId, userId, userRoleIds, userRoles, stateId, stateName, districtId, districtName,
        blockId, blockName, clusterId, clusterName, schoolId, schoolName, tenantId, organisationId,
        organisationName, organisationCode, programName, programId, solutionName, solutionId,
        status, submissionDate,

        // Update parameters
        userId, userRoleIds, userRoles, stateId, stateName, districtId, districtName,
        blockId, blockName, clusterId, clusterName, schoolId, schoolName, tenantId, organisationId,
        organisationName, organisationCode, programName, programId, solutionName, solutionId,
        status, submissionDate
      )
      checkExistenceOfFilterData(statusDashboardFilters, context, solutionId)
      postgresUtil.executePreparedUpdate(upsertSurveyDataQuery, surveyParams, surveyStatusTable, surveyId)

      if (event.status == "completed") {
        /**
         * Extracting Survey Questions Data
         */
        postgresUtil.checkAndCreateTable(surveyQuestionTable, config.createSurveyQuestionsTableQuery.replace("@surveyQuestionTable", surveyQuestionTable))
        postgresUtil.executeUpdate(AlterSurveyQuestionsTableQuery, surveyQuestionTable, solutionId)

        val solution_id = event.solutionId
        val solution_name = event.solutionName
        val user_id = event.createdBy
        val state_name = event.stateName
        val district_name = event.districtName
        val block_name = event.blockName
        val cluster_name = event.clusterName
        val school_name = event.schoolName
        val answersKey = event.answers

        val deleteQuestionQuery =
          s"""DELETE FROM "$surveyQuestionTable"
             |WHERE user_id = ?
             |  AND state_name = ?
             |  AND district_name = ?
             |  AND block_name = ?
             |  AND cluster_name = ?
             |  AND school_name = ?;
             |""".stripMargin
        val deleteQuestionParams = Seq(user_id, state_name, district_name, block_name, cluster_name, school_name)
        postgresUtil.executePreparedUpdate(deleteQuestionQuery, deleteQuestionParams, surveyQuestionTable, user_id)

        answersKey match {
          case answersMap: Map[_, _] =>
            answersMap.foreach { case (_, value) =>
              val questionsMap = value.asInstanceOf[Map[String, Any]]
              val question_id: String = questionsMap.get("qid").collect { case v: String => v }.getOrElse("")
              val payloadOpt: Option[Map[String, Any]] = questionsMap.get("payload").collect { case m: Map[String @unchecked, Any @unchecked] => m }
              val report_type: String = questionsMap.get("reportType").map(_.toString).getOrElse("Default")
              val responseType = questionsMap.get("responseType").map(_.toString).getOrElse("")
              val remarks: String = questionsMap.get("remarks").map(_.toString).getOrElse("")
              val attachments: List[Map[String, Any]] = questionsMap.get("fileName").collect { case list: List[Map[String, Any]] => list }.getOrElse(Nil)
              val evidence: String = extractEvidenceData(attachments)

              if (payloadOpt.isDefined) {
                if (responseType == "matrix") {
                  val parent_question_text: String = questionsMap.get("payload") match {
                    case Some(payloadMap: Map[_, _]) =>
                      payloadMap.asInstanceOf[Map[String, Any]].get("question") match {
                        case Some(qList: List[_]) =>
                          qList.collect { case q: String if q.nonEmpty => q }.headOption.getOrElse("")
                        case Some(q: String) => q
                        case _ => ""
                      }
                    case _ => ""
                  }
                  val has_parent_question: Boolean = parent_question_text.nonEmpty

                  processQuestion(responseType, questionsMap, payloadOpt, question_id, has_parent_question, parent_question_text, evidence, remarks, report_type)
                } else {
                  processQuestion(responseType, questionsMap, payloadOpt, question_id, has_parent_question = false, parent_question_text = null, evidence, remarks, report_type)
                }
              } else {
                println(s"Skipping question_id=$question_id as payload is missing.")
              }
            }

          case _ =>
            logger.error("Unexpected structure for answers field")
        }


        def processQuestion(responseType: String, questionsMap: Map[String, Any], payload: Option[Map[String, Any]], question_id: String, has_parent_question: Boolean, parent_question_text: String, evidence: String, remarks: String, report_type: String): Unit = {
          val value: String = questionsMap.get("value") match {
            case Some(v: String) => v
            case Some(v: Int) => v.toString
            case _ => ""
          }
          val score: Integer = questionsMap.get("scoreAchieved") match {
            case Some(v: Integer) => v
            case _ => 0
          }

          responseType match {
            case "text" =>
              textQuestionType(payload, question_id, solution_id, solution_name, user_id, value, state_name,
                district_name, block_name, cluster_name, school_name, has_parent_question, parent_question_text, evidence, remarks, report_type)
            case "radio" =>
              radioQuestionType(payload, question_id, solution_id, solution_name, user_id, value, state_name,
                district_name, block_name, cluster_name, school_name, score, has_parent_question, parent_question_text, evidence, remarks, report_type)
            case "date" =>
              dateQuestionType(payload, question_id, solution_id, solution_name, user_id, value, state_name,
                district_name, block_name, cluster_name, school_name, score, has_parent_question, parent_question_text, evidence, remarks, report_type)
            case "multiselect" =>
              multiselectQuestionType(payload, question_id, solution_id, solution_name, user_id, value, state_name,
                district_name, block_name, cluster_name, school_name, has_parent_question, parent_question_text, evidence, remarks, report_type)
            case "number" =>
              numberQuestionType(payload, question_id, solution_id, solution_name, user_id, value, state_name,
                district_name, block_name, cluster_name, school_name, has_parent_question, parent_question_text, evidence, remarks, report_type)
            case "slider" =>
              sliderQuestionType(payload, question_id, solution_id, solution_name, user_id, value, state_name,
                district_name, block_name, cluster_name, school_name, has_parent_question, parent_question_text, evidence, remarks, report_type)
            case "matrix" =>
              questionsMap.get("value") match {
                case Some(valueList: List[Map[String, Any]]) =>
                  valueList.foreach { instance =>
                    instance.foreach { case (_, questionData) =>
                      val matrixQuestionMap = questionData.asInstanceOf[Map[String, Any]]
                      val matrixQuestionId: String = matrixQuestionMap.get("qid") match {
                        case Some(v: String) => v
                        case _ => ""
                      }
                      val matrixPayload = matrixQuestionMap.get("payload").map(_.asInstanceOf[Map[String, Any]])
                      val matrixResponseType = matrixQuestionMap.get("responseType").map(_.toString).getOrElse("")
                      val matrixRemarks: String = questionsMap.get("remarks").map(_.toString).getOrElse("")
                      val matrixAttachments: List[Map[String, Any]] = questionsMap.get("fileName").collect { case list: List[Map[String, Any]] => list }.getOrElse(Nil)
                      val matrixEvidence: String = extractEvidenceData(matrixAttachments)
                      processQuestion(matrixResponseType, matrixQuestionMap, matrixPayload, matrixQuestionId, has_parent_question, parent_question_text, matrixEvidence, matrixRemarks, report_type)
                    }
                  }
                case _ => println("No matrix data found.")
              }
            case _ => println(s"Unsupported responseType: $responseType")
          }
        }

      }

      def extractField(payload: Option[Map[String, Any]], key: String): String = {
        payload.flatMap(_.get(key)) match {
          case Some(qList: List[_]) =>
            qList.collect { case q if q != null && q.toString.nonEmpty => q.toString }.mkString(" | ")
          case _ => ""
        }
      }

      def insertQuestion(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String,
                         user_id: String, value: String, state_name: String, district_name: String,
                         block_name: String, cluster_name: String, school_name: String, has_parent_question: Boolean,
                         parent_question_text: String, question_type: String, evidence: String, remarks: String, report_type: String): Unit = {

        if (!payload.exists(_.contains("labels"))) {
          println(s"Skipping question $question_id as 'labels' key is missing.")
          return
        }

        val question = extractField(payload, "question")
        val labels = extractField(payload, "labels")

        val insertQuestionQuery =
          s"""INSERT INTO "$surveyQuestionTable" (
             |    survey_id, user_id, user_role_ids, user_roles, state_id, state_name, district_id, district_name,
             |    block_id, block_name, cluster_id, cluster_name, school_id, school_name, tenant_id, organisation_id,
             |    organisation_name, organisation_code, program_name, program_id, solution_name, solution_id,
             |    question_id, question_text, labels, value, has_parent_question, parent_question_text, evidence, question_type, remarks, report_type
             |) VALUES (
             |   ?, ?, ?, ?, ?, ?, ?, ?,
             |   ?, ?, ?, ?, ?, ?, ?,
             |   ?, ?, ?, ?, ?, ?,
             |   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             |);
             |""".stripMargin

        val questionParam = Seq(
          surveyId, userId, userRoleIds, userRoles, stateId, stateName, districtId, districtName,
          blockId, blockName, clusterId, clusterName, schoolId, schoolName, tenantId, organisationId,
          organisationName, organisationCode, programName, programId, solutionName, solutionId,
          question_id, question, labels, value, has_parent_question, parent_question_text, evidence, question_type, remarks, report_type
        )
        checkExistenceOfFilterData(questionDashboardFilters, context, solutionId)
        postgresUtil.executePreparedUpdate(insertQuestionQuery, questionParam, surveyQuestionTable, solution_id)
      }

      def textQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String,
                           user_id: String, value: String, state_name: String, district_name: String,
                           block_name: String, cluster_name: String, school_name: String, has_parent_question: Boolean,
                           parent_question_text: String, evidence: String, remarks: String, report_type: String): Unit = {
        insertQuestion(payload, question_id, solution_id, solution_name, user_id, value, state_name, district_name, block_name, cluster_name, school_name, has_parent_question, parent_question_text, "text", evidence, remarks, report_type)
      }

      def radioQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String,
                            user_id: String, value: String, state_name: String, district_name: String,
                            block_name: String, cluster_name: String, school_name: String, score: Integer, has_parent_question: Boolean,
                            parent_question_text: String, evidence: String, remarks: String, report_type: String): Unit = {
        insertQuestion(payload, question_id, solution_id, solution_name, user_id, value, state_name, district_name, block_name, cluster_name, school_name, has_parent_question, parent_question_text, "radio", evidence, remarks, report_type)
      }

      def dateQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String,
                           user_id: String, value: String, state_name: String, district_name: String,
                           block_name: String, cluster_name: String, school_name: String, score: Integer, has_parent_question: Boolean,
                           parent_question_text: String, evidence: String, remarks: String, report_type: String): Unit = {
        insertQuestion(payload, question_id, solution_id, solution_name, user_id, value, state_name, district_name, block_name, cluster_name, school_name, has_parent_question, parent_question_text, "date", evidence, remarks, report_type)
      }

      def multiselectQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String,
                                  user_id: String, value: String, state_name: String, district_name: String,
                                  block_name: String, cluster_name: String, school_name: String, has_parent_question: Boolean,
                                  parent_question_text: String, evidence: String, remarks: String, report_type: String): Unit = {
        insertQuestion(payload, question_id, solution_id, solution_name, user_id, value, state_name, district_name, block_name, cluster_name, school_name, has_parent_question, parent_question_text, "multiselect", evidence, remarks, report_type)
      }

      def numberQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String,
                             user_id: String, value: String, state_name: String, district_name: String,
                             block_name: String, cluster_name: String, school_name: String, has_parent_question: Boolean,
                             parent_question_text: String, evidence: String, remarks: String, report_type: String): Unit = {
        insertQuestion(payload, question_id, solution_id, solution_name, user_id, value, state_name, district_name, block_name, cluster_name, school_name, has_parent_question, parent_question_text, "number", evidence, remarks, report_type)
      }

      def sliderQuestionType(payload: Option[Map[String, Any]], question_id: String, solution_id: String, solution_name: String,
                             user_id: String, value: String, state_name: String, district_name: String,
                             block_name: String, cluster_name: String, school_name: String, has_parent_question: Boolean,
                             parent_question_text: String, evidence: String, remarks: String, report_type: String): Unit = {
        insertQuestion(payload, question_id, solution_id, solution_name, user_id, value, state_name, district_name, block_name, cluster_name, school_name, has_parent_question, parent_question_text, "slider", evidence, remarks, report_type)
      }


      /**
       * Logic to populate kafka messages for creating metabase dashboard
       */
      if (event.status == "completed") {
        val dashboardData = new java.util.HashMap[String, String]()
        val dashboardConfig = Seq(
          ("admin", "1", "admin"),
          ("program", event.programId, "targetedProgram"),
          ("solution", event.solutionId, "targetedSolution")
        )

        dashboardConfig
          .filter { case (key, _, _) => config.reportsEnabled.contains(key) }
          .foreach { case (key, value, target) =>
            checkAndInsert(key, value, dashboardData, target)
          }

        if (!dashboardData.isEmpty) {
          pushSurveyDashboardEvents(dashboardData, context)
        }
      }
    } else {
      println(s"Skipping the survey event with Id = ${event._id} and status = ${event.status} as it is not in a valid status.")
    }
  }

  private def extractUserRolesData(roles: List[Map[String, Any]]): (String, String) = {
    if (roles == null || roles.isEmpty) {
      ("", "")
    } else {
      val roleId = roles.map { role => role.get("id").map(_.toString).getOrElse("") }
      val roleName = roles.map { role => role.get("title").map(_.toString).getOrElse("") }
      (roleId.mkString(", "), roleName.mkString(", "))
    }
  }

  private def checkAndInsert(entityType: String, targetedId: String, dashboardData: java.util.HashMap[String, String], dashboardKey: String): Unit = {
    val query = s"SELECT EXISTS (SELECT 1 FROM ${config.dashboard_metadata} WHERE entity_id = '$targetedId') AS is_${entityType}_present"
    val result = postgresUtil.fetchData(query)

    result.foreach { row =>
      row.get(s"is_${entityType}_present") match {
        case Some(isPresent: Boolean) if isPresent =>
          println(s"$entityType details already exist.")
        case _ =>
          if (entityType == "admin") {
            val insertQuery = s"INSERT INTO ${config.dashboard_metadata} (entity_type, entity_name, entity_id) VALUES ('$entityType', 'Admin', '$targetedId')"
            val affectedRows = postgresUtil.insertData(insertQuery)
            println(s"Inserted Admin details. Affected rows: $affectedRows")
            dashboardData.put(dashboardKey, "1")
          } else {
            val getEntityNameQuery =
              s"""
                 |SELECT DISTINCT ${
                if (entityType == "solution") "name"
                else s"${entityType}_name"
              } AS ${entityType}_name
                 |FROM ${
                entityType match {
                  case "program" => config.solutions
                  case "solution" => config.solutions
                }
              }
                 |WHERE ${entityType}_id = '$targetedId'
               """.stripMargin.replaceAll("\n", " ")
            val result = postgresUtil.fetchData(getEntityNameQuery)
            result.foreach { id =>
              val entityName = id.get(s"${entityType}_name").map(_.toString).getOrElse("")
              val upsertMetaDataQuery =
                s"""INSERT INTO ${config.dashboard_metadata} (
                   |    entity_type, entity_name, entity_id
                   |) VALUES (
                   |    ?, ?, ?
                   |) ON CONFLICT (entity_id) DO UPDATE SET
                   |    entity_type = ?, entity_name = ?;
                   |""".stripMargin

              val dashboardParams = Seq(
                entityType, entityName, targetedId, // Insert parameters
                entityType, entityName // Update parameters (matching columns in the ON CONFLICT clause)
              )
              postgresUtil.executePreparedUpdate(upsertMetaDataQuery, dashboardParams, config.dashboard_metadata, targetedId)
              println(s"Inserted [$entityName : $targetedId] details.")
              dashboardData.put(dashboardKey, targetedId)
            }
          }
      }
    }
  }

  private def checkExistenceOfFilterData(filterList: List[Map[String, String]], context: ProcessFunction[Event, Event]#Context, solutionId: String): Unit = {
    println(">>>>>>>>>>>>>> Checking existence of filter data...")
    filterList.foreach { filter =>
      val tableName = filter.getOrElse("table_name", "")
      val columnValuePairs = filter.filterKeys(_ != "table_name").filter { case (_, v) => v != null && v.nonEmpty }
      if (tableName.nonEmpty && columnValuePairs.nonEmpty) {
        val whereClause = columnValuePairs.map { case (col, _) => s"""$col = ?""" }.mkString(" AND ")
        val params = columnValuePairs.values.toSeq
        val queryWithParams = params.foldLeft(s"SELECT EXISTS (SELECT 1 FROM $tableName WHERE $whereClause) AS data_exists") {
          case (q, v) => q.replaceFirst("\\?", s"'${v.replace("'", "''")}'")
        }
        val dataExists = postgresUtil.executeQuery(queryWithParams) { rs =>
          if (rs.next()) rs.getBoolean("data_exists") else false
        }
        val rowCountQuery = s"SELECT COUNT(*) AS row_count FROM $tableName"
        val rowCount = postgresUtil.executeQuery(rowCountQuery) { rs =>
          if (rs.next()) rs.getLong("row_count") else 0
        }
        if (!dataExists && rowCount > 0) {
          val eventData = new java.util.HashMap[String, String]()
          eventData.put("targetedSolution", solutionId)
          eventData.put("filterTable", tableName.stripPrefix("\"").stripSuffix("\""))
          eventData.put("filterSync", "Yes")
          println(s"As dataExits = $dataExists, rowCount = $rowCount hence pushing the message to kafka.")
          pushSurveyDashboardEvents(eventData, context)
        } else {
          println(s"As dataExits = $dataExists, rowCount = $rowCount hence stopping function.")
          return
        }
      } else {
        println(s"table doesn't exits or filter json is empty, hence stopping function.  ")
      }
    }
    println(s">>>>>>>>>>>>>> Completed checking existence of filter data...")
  }

  private def pushSurveyDashboardEvents(dashboardData: util.HashMap[String, String], context: ProcessFunction[Event, Event]#Context): util.HashMap[String, AnyRef] = {
    val objects = new util.HashMap[String, AnyRef]() {
      put("_id", java.util.UUID.randomUUID().toString)
      put("reportType", "Survey")
      put("publishedAt", DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(ZoneId.systemDefault())
        .format(Instant.ofEpochMilli(System.currentTimeMillis())).asInstanceOf[AnyRef])
      put("dashboardData", dashboardData)
    }
    val event = ScalaJsonUtil.serialize(objects)
    context.output(config.eventOutputTag, event)
    println(s"----> Pushed new Kafka message to ${config.outputTopic} topic")
    println(objects)
    objects
  }

  private def extractEvidenceData(attachments: List[Map[String, Any]]): String = {
    val evidenceList = attachments.map { attachment =>
      if (attachment.get("type").contains("link")) {
        attachment.get("name").map(_.toString).getOrElse("")
      } else {
        attachment.get("sourcePath").map(_.toString).getOrElse("")
      }
    }
    evidenceList.mkString(", ")
  }

}