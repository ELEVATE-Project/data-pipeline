package org.shikshalokam.job.observation.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.observation.stream.processor.domain.Event
import org.shikshalokam.job.observation.stream.processor.task.ObservationStreamConfig
import org.shikshalokam.job.util.{PostgresUtil, ScalaJsonUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.util
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.immutable._

class ObservationStreamFunction(config: ObservationStreamConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObservationStreamFunction])

  override def metricsList(): List[String] = {
    List(config.observationCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
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
      println(s"***************** Start of Processing the Observation Event with Id = ${event._id} *****************")
      var userRoleIds: String = ""
      var userRoles: String = ""
      var orgId: String = ""
      var orgName: String = ""
      var orgCode: String = ""

      event.organisation.foreach { org =>
        if (org.get("code").contains(event.organisationId)) {
          orgName = org.get("name").map(_.toString).getOrElse("")
          orgId = org.get("id").map(_.toString).getOrElse("")
          orgCode = org.get("code").map(_.toString).getOrElse("")
          val roles = org.get("roles").map(_.asInstanceOf[List[Map[String, Any]]]).getOrElse(List.empty)
          val (userRoleIdsExtracted, userRolesExtracted) = extractUserRolesData(roles)
          userRoleIds = userRoleIdsExtracted
          userRoles = userRolesExtracted
        } else {
          println(s"Organisation with ID ${event.organisationId} not found in the event data.")
        }
      }

      def extractUserRolesData(roles: List[Map[String, Any]]): (String, String) = {
        if (roles == null || roles.isEmpty) {
          ("", "")
        } else {
          val roleId = roles.map { role => role.get("id").map(_.toString).getOrElse("") }
          val roleName = roles.map { role => role.get("title").map(_.toString).getOrElse("") }
          (roleId.mkString(", "), roleName.mkString(", "))
        }
      }

      def checkAndCreateTable(tableName: String, createTableQuery: String): Unit = {
        val checkTableExistsQuery =
          s"""SELECT EXISTS (
             |  SELECT FROM information_schema.tables
             |  WHERE table_name = '$tableName'
             |);
             |""".stripMargin

        val tableExists = postgresUtil.executeQuery(checkTableExistsQuery) { resultSet =>
          if (resultSet.next()) resultSet.getBoolean(1) else false
        }

        if (!tableExists) {
          postgresUtil.createTable(createTableQuery, tableName)
        }
      }

      val statusOfSubmission = event.status
      val submissionId = event._id
      val userId = event.createdBy
      val submissionNumber = event.submissionNumber
      val userOneProfileName = event.userOneProfileName
      val userOneProfileId = event.userOneProfileId
      val userTwoProfileName = event.userTwoProfileName
      val userTwoProfileId = event.userTwoProfileId
      val userThreeProfileName = event.userThreeProfileName
      val userThreeProfileId = event.userThreeProfileId
      val userFourProfileName = event.userFourProfileName
      val userFourProfileId = event.userFourProfileId
      val userFiveProfileName = event.userFiveProfileName
      val userFiveProfileId = event.userFiveProfileId
      val themes = event.themes
      val criteriaEvent = event.criteria
      val solutionId = event.solutionId
      val solutionName = event.solutionName
      val programName = event.programName
      val programId = event.programId
      val observationName = event.observationName
      val observationId = event.observationId
      val parentOrgId = event.parentOrgId
      val tenantId = event.tenantId
      val solutionExternalId = event.solutionExternalId
      val solutionDescription = event.solutionDescription
      val programExternalId = event.programExternalId
      val programDescription = event.programDescription
      val privateProgram: Null = null
      val projectCategories: Null = null
      val projectDuration: Null = null
      val completedDate = event.completedDate
      val domainTable = s""""${solutionId}_domain""""
      val questionTable = s""""${solutionId}_questions""""
      val statusTable = s""""${solutionId}_status""""
      val isRubric = event.isRubric
      val entityType = event.entityType
      val entityId = event.entityId
      val entityName = event.entityName
      val entityExternalId = event.entityExternalId
      val parentOneObservedName = event.parentOneName
      val parentOneObservedId = event.parentOneId
      val parentTwoObservedName = event.parentTwoName
      val parentTwoObservedId = event.parentTwoId
      val parentThreeObservedName = event.parentThreeName
      val parentThreeObservedId = event.parentThreeId
      val parentFourObservedName = event.parentFourName
      val parentFourObservedId = event.parentFourId
      val parentFiveObservedName = event.parentFiveName
      val parentFiveObservedId = event.parentFiveId

      println(s"statusOfSubmission = $statusOfSubmission")
      println(s"submissionId = $submissionId")
      println(s"userId = $userId")
      println(s"submissionNumber = $submissionNumber")
      println(s"userOneProfileName = $userOneProfileName")
      println(s"userOneProfileId = $userOneProfileId")
      println(s"userTwoProfileName = $userTwoProfileName")
      println(s"userTwoProfileId = $userTwoProfileId")
      println(s"userThreeProfileName = $userThreeProfileName")
      println(s"userThreeProfileId = $userThreeProfileId")
      println(s"userFourProfileName = $userFourProfileName")
      println(s"userFourProfileId = $userFourProfileId")
      println(s"userFiveProfileName = $userFiveProfileName")
      println(s"userFiveProfileId = $userFiveProfileId")
      println(s"themes = $themes")
      println(s"criteriaEvent = $criteriaEvent")
      println(s"solutionId = $solutionId")
      println(s"solutionName = $solutionName")
      println(s"programName = $programName")
      println(s"programId = $programId")
      println(s"observationName = $observationName")
      println(s"observationId = $observationId")
      println(s"tenantId = $tenantId")
      println(s"solutionExternalId = $solutionExternalId")
      println(s"solutionDescription = $solutionDescription")
      println(s"programExternalId = $programExternalId")
      println(s"programDescription = $programDescription")
      println(s"privateProgram = $privateProgram")
      println(s"projectCategories = $projectCategories")
      println(s"projectDuration = $projectDuration")
      println(s"completedDate = $completedDate")
      println(s"domainTable = $domainTable")
      println(s"questionTable = $questionTable")
      println(s"statusTable = $statusTable")
      println(s"isRubric = $isRubric")
      println(s"entityType = $entityType")
      println(s"entityId = $entityId")
      println(s"entityName = $entityName")
      println(s"entityExternalId = $entityExternalId")
      println(s"parentOneObservedName = $parentOneObservedName")
      println(s"parentOneObservedId = $parentOneObservedId")
      println(s"parentTwoObservedName = $parentTwoObservedName")
      println(s"parentTwoObservedId = $parentTwoObservedId")
      println(s"parentThreeObservedName = $parentThreeObservedName")
      println(s"parentThreeObservedId = $parentThreeObservedId")
      println(s"parentFourObservedName = $parentFourObservedName")
      println(s"parentFourObservedId = $parentFourObservedId")
      println(s"parentFiveObservedName = $parentFiveObservedName")
      println(s"parentFiveObservedId = $parentFiveObservedId")
      println("\n\n")

      val statusDashboardFilters: List[Map[String, String]] = List(
        Map(
          "user_one_profile_name" -> userOneProfileName,
          "user_two_profile_name" -> userTwoProfileName,
          "user_three_profile_name" -> userThreeProfileName,
          "user_four_profile_name" -> userFourProfileName,
          "user_five_profile_name" -> userFiveProfileName,
          "parent_one_name" -> parentOneObservedName,
          "parent_two_name" -> parentTwoObservedName,
          "parent_three_name" -> parentThreeObservedName,
          "parent_four_name" -> parentFourObservedName,
          "parent_five_name" -> parentFiveObservedName,
          "entity_name" -> entityName,
          "org_name" -> orgName,
          "table_name" -> statusTable
        )
      )

      val questionDashboardFilters: List[Map[String, String]] = List(
        Map(
          "user_one_profile_name" -> userOneProfileName,
          "user_two_profile_name" -> userTwoProfileName,
          "user_three_profile_name" -> userThreeProfileName,
          "user_four_profile_name" -> userFourProfileName,
          "user_five_profile_name" -> userFiveProfileName,
          "parent_one_name" -> parentOneObservedName,
          "parent_two_name" -> parentTwoObservedName,
          "parent_three_name" -> parentThreeObservedName,
          "parent_four_name" -> parentFourObservedName,
          "parent_five_name" -> parentFiveObservedName,
          "entity_name" -> entityName,
          "org_name" -> orgName,
          "table_name" -> questionTable
        )
      )

      val domainDashboardFilters: List[Map[String, String]] = List(
        Map(
          "user_one_profile_name" -> userOneProfileName,
          "user_two_profile_name" -> userTwoProfileName,
          "user_three_profile_name" -> userThreeProfileName,
          "user_four_profile_name" -> userFourProfileName,
          "user_five_profile_name" -> userFiveProfileName,
          "parent_one_name" -> parentOneObservedName,
          "parent_two_name" -> parentTwoObservedName,
          "parent_three_name" -> parentThreeObservedName,
          "parent_four_name" -> parentFourObservedName,
          "parent_five_name" -> parentFiveObservedName,
          "entity_name" -> entityName,
          "org_name" -> orgName,
          "table_name" -> domainTable
        )
      )

      postgresUtil.createTable(config.createDashboardMetadataTable, config.dashboard_metadata)

      /**
       * Performs an upsert operation on the solution table irrespective of the submission status.
       * This ensures the solution information is always updated/inserted in the database.
       */
      if (solutionId != null) {
        println("===> Processing observation solution data")
        postgresUtil.createTable(config.createSolutionsTable, config.solutions)
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
        println("\n\n")
      }

      /**
       * Performs an delete and insert operation on the observation status table irrespective of the submission status.
       * This ensures the observation status table will always holds latest data in the database.
       */
      if (statusOfSubmission != null) {
        println("===> Processing observation status data")
        checkAndCreateTable(statusTable, config.createStatusTable.replace("@statusTable", statusTable))
        val deleteQuery = s"""DELETE FROM $statusTable WHERE user_id = ? AND submission_id = ? AND submission_number = ?; """
        val deleteParams = Seq(userId, submissionId, submissionNumber)
        postgresUtil.executePreparedDelete(deleteQuery, deleteParams, statusTable, submissionId)

        val insertStatusQuery =
          s"""INSERT INTO $statusTable (
             |    user_id, user_role_ids, user_roles, solution_id, solution_name, submission_id, submission_number,
             |    program_name, program_id, observation_name, observation_id, user_one_profile_name, user_one_profile_id,
             |    user_two_profile_name, user_two_profile_id, user_three_profile_name, user_three_profile_id, user_four_profile_name,
             |    user_four_profile_id, user_five_profile_name, user_five_profile_id, tenant_id, org_id, org_code, org_name,
             |    status_of_submission, submitted_at, entity_type, entity_id, entity_name, entity_external_id, parent_one_name, parent_one_id, parent_two_name,
             |    parent_two_id, parent_three_name, parent_three_id, parent_four_name, parent_four_id,
             |    parent_five_name, parent_five_id
             |) VALUES (
             |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             |);""".stripMargin

        val statusParams = Seq(
          userId, userRoleIds, userRoles, solutionId, solutionName, submissionId, submissionNumber,
          programName, programId, observationName, observationId, userOneProfileName, userOneProfileId,
          userTwoProfileName, userTwoProfileId, userThreeProfileName, userThreeProfileId, userFourProfileName,
          userFourProfileId, userFiveProfileName, userFiveProfileId, tenantId, orgId, orgCode, orgName,
          statusOfSubmission, completedDate, entityType, entityId, entityName, entityExternalId, parentOneObservedName, parentOneObservedId, parentTwoObservedName,
          parentTwoObservedId, parentThreeObservedName, parentThreeObservedId, parentFourObservedName, parentFourObservedId,
          parentFiveObservedName, parentFiveObservedId
        )
        checkExistenceOfFilterData(statusDashboardFilters, context, solutionId)
        postgresUtil.executePreparedUpdate(insertStatusQuery, statusParams, statusTable, submissionId)
        println("\n\n")
      }

      /**
       * Performs an delete and insert operation on the observation domain and question tables only is submission status is completed.
       * This ensures the observation domain and question tables will always holds latest data in the database.
       */
      if (statusOfSubmission == "completed") {
        println("===> Processing observation domain data")
        if (isRubric) {
          checkAndCreateTable(domainTable, config.createDomainsTable.replace("@domainTable", domainTable))
          val deleteQuery = s"""DELETE FROM $domainTable WHERE user_id = ? AND submission_id = ? AND submission_number = ?; """
          val deleteParams = Seq(userId, submissionId, submissionNumber)
          postgresUtil.executePreparedDelete(deleteQuery, deleteParams, domainTable, submissionId)
          themes.foreach { domain =>
            val domainName = domain("name")
            val domainLevel = domain.getOrElse("pointsBasedLevel", null)
            val maybeChildren = domain.get("children").map(_.asInstanceOf[List[Map[String, Any]]])
            val maybeCriteria = domain.get("criteria").map(_.asInstanceOf[List[Map[String, Any]]])

            maybeChildren match {
              case Some(subdomains) =>
                subdomains.foreach { subdomain =>
                  val maybeSubCriteria = subdomain.get("criteria").map(_.asInstanceOf[List[Map[String, Any]]])
                  maybeSubCriteria.foreach { criteriaList =>
                    criteriaList.foreach { criteria =>
                      val criteriaId = criteria("criteriaId")

                      criteriaEvent.find(_("_id") == criteriaId).foreach { crit =>
                      }
                    }
                  }
                }

              case None =>
                maybeCriteria.foreach { criteriaList =>
                  criteriaList.foreach { criteria =>
                    val criteriaId = criteria("criteriaId")
                    println(s"criteria_id: $criteriaId")

                    criteriaEvent.find(c => c("_id") == criteriaId).foreach { crit =>
                      val criteriaName = crit("name")
                      val criteriaLevel = crit("score")
                      val insertCriteriaQuery =
                        s"""INSERT INTO $domainTable (
                           |    user_id, user_role_ids, user_roles, solution_id, solution_name, submission_id, submission_number,
                           |    program_name, program_id, observation_name, observation_id, tenant_id, org_name, org_id, org_code,
                           |    user_one_profile_name, user_one_profile_id, user_two_profile_name, user_two_profile_id, user_three_profile_name, user_three_profile_id,
                           |    user_four_profile_name, user_four_profile_id, user_five_profile_name, user_five_profile_id, domain, domain_level,
                           |    criteria, criteria_level, completed_date, entity_type, entity_id, entity_name, entity_external_id, parent_one_name, parent_one_id,
                           |    parent_two_name, parent_two_id, parent_three_name, parent_three_id, parent_four_name,
                           |    parent_four_id, parent_five_name, parent_five_id
                           |) VALUES (
                           |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                           |);
                           |""".stripMargin

                      val criteriaParams = Seq(
                        userId, userRoleIds, userRoles, solutionId, solutionName, submissionId, submissionNumber, programName, programId, observationName,
                        observationId, tenantId, orgName, orgId, orgCode, userOneProfileName, userOneProfileId, userTwoProfileName, userTwoProfileId, userThreeProfileName,
                        userThreeProfileId, userFourProfileName, userFourProfileId, userFiveProfileName, userFiveProfileId, domainName, domainLevel, criteriaName, criteriaLevel,
                        completedDate, entityType, entityId, entityName, entityExternalId, parentOneObservedName, parentOneObservedId, parentTwoObservedName, parentTwoObservedId, parentThreeObservedName, parentThreeObservedId,
                        parentFourObservedName, parentFourObservedId, parentFiveObservedName, parentFiveObservedId)

                      checkExistenceOfFilterData(domainDashboardFilters, context, solutionId)
                      postgresUtil.executePreparedUpdate(insertCriteriaQuery, criteriaParams, domainTable, solutionId)
                    }
                  }
                }
                println("\n\n")
            }
          }
        } else {
          println("** This observation not attached with any rubrics so skipping processing the domain data \n\n")
        }

        println("===> Processing observation question data")
        checkAndCreateTable(questionTable, config.createQuestionsTable.replace("@questionTable", questionTable))
        val deleteQuery = s"""DELETE FROM $questionTable WHERE user_id = ? AND submission_id = ? AND submission_number = ?; """
        val deleteParams = Seq(userId, submissionId, submissionNumber)
        postgresUtil.executePreparedDelete(deleteQuery, deleteParams, questionTable, submissionId)
        checkExistenceOfFilterData(questionDashboardFilters, context, solutionId)
        val questionsFunction = new ObservationQuestionFunction(postgresUtil, config, questionTable)
        val answersKey = event.answers

        def processQuestion(responseType: String, questionsMap: Map[String, Any], payload: Option[Map[String, Any]], questionId: String, domainName: String,
                            criteriaName: String, hasParentQuestion: Boolean, parentQuestionText: String, evidences: String, remarks: String, reportType: String): Unit = {
          val value: String = questionsMap.get("value") match {
            case Some(v: String) => v
            case Some(v: Int) => v.toString
            case _ => ""
          }
          val score: Integer = questionsMap.get("scoreAchieved") match {
            case Some(v: Integer) => v
            case _ => 0
          }

          val commonParams = QuestionParams(payload = payload, questionId = questionId, solutionId = solutionId, solutionName = solutionName, submissionId = submissionId, submissionNumber = submissionNumber, userId = userId, userRoleIds = userRoleIds,
            userRoles = userRoles, programName = programName, programId = programId, observationName = observationName, observationId = observationId, value = value,
            userOneProfileName = userOneProfileName, userOneProfileId = userOneProfileId, userTwoProfileName = userTwoProfileName, userTwoProfileId = userTwoProfileId, userThreeProfileName = userThreeProfileName, userThreeProfileId = userThreeProfileId,
            userFourProfileName = userFourProfileName, userFourProfileId = userFourProfileId, userFiveProfileName = userFiveProfileName, userFiveProfileId = userFiveProfileId,
            tenantId = tenantId, orgId = orgId, orgCode = orgCode, orgName = orgName, statusOfSubmission = statusOfSubmission, submittedAt = completedDate, entityType = entityType, entityId = entityId, entityName = entityName, entityExternalId = entityExternalId,
            parentOneName = parentOneObservedName, parentOneId = parentOneObservedId, parentTwoName = parentTwoObservedName, parentTwoId = parentTwoObservedId, parentThreeName = parentThreeObservedName, parentThreeId = parentThreeObservedId,
            parentFourName = parentFourObservedName, parentFourId = parentFourObservedId, parentFiveName = parentFiveObservedName, parentFiveId = parentFiveObservedId,
            domainName = domainName, criteriaName = criteriaName, score = score, hasParentQuestion = hasParentQuestion, parentQuestionText = parentQuestionText, evidences = evidences, remarks = remarks, reportType = reportType
          )
          responseType match {
            case "text" => questionsFunction.processQuestionType(commonParams, "text")
            case "radio" => questionsFunction.processQuestionType(commonParams, "radio")
            case "date" => questionsFunction.processQuestionType(commonParams, "date")
            case "multiselect" => questionsFunction.processQuestionType(commonParams, "multiselect")
            case "number" => questionsFunction.processQuestionType(commonParams, "number")
            case "slider" => questionsFunction.processQuestionType(commonParams, "slider")

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
                      processQuestion(matrixResponseType, matrixQuestionMap, matrixPayload, matrixQuestionId, domainName, criteriaName, hasParentQuestion, parentQuestionText, evidences, remarks, reportType)
                    }
                  }
                case _ => println("No matrix data found.")
              }
            case _ => println(s"Unsupported responseType: $responseType")
          }
        }

        answersKey match {
          case answersMap: Map[_, _] =>
            answersMap.foreach { case (_, value) =>
              val questionsMap = value.asInstanceOf[Map[String, Any]]
              val payloadOpt: Option[Map[String, Any]] = questionsMap.get("payload").collect { case m: Map[String@unchecked, Any@unchecked] => m }
              println(s"qid: ${questionsMap.get("qid")}")
              val question_id: String = questionsMap.get("qid") match {
                case Some(v: String) => v
                case _ => ""
              }
              val report_type: String = questionsMap.get("reportType") match {
                case Some(v: String) => v
                case _ => "Default"
              }
              val question_criteria_id: String = questionsMap.get("criteriaId") match {
                case Some(v: String) => v
                case _ => ""
              }
              var domain_name: String = ""
              var criteria_name: String = ""
              var remarks: String = questionsMap.get("remarks") match {
                case Some(v: String) => v
                case _ => ""
              }
              val evidences = questionsMap match {
                case map: Map[String, Any] if map.nonEmpty =>
                  val extractedEvidences = map.get("fileName") match {
                    case Some(fileList: List[Map[String, Any]]) =>
                      fileList.collect {
                        case file if file.contains("sourcePath") => file("sourcePath").toString
                      }
                    case _ => Seq.empty
                  }
                  if (extractedEvidences.isEmpty) null else extractedEvidences.mkString(",")
                case _ => null
              }

              val result = for {
                theme <- themes
                criteriaList = theme("criteria").asInstanceOf[List[Map[String, Any]]]
                matchingCriteria <- criteriaList.find(_("criteriaId") == question_criteria_id)
                themeName = theme("name").toString
                criteriaName <- criteriaEvent.find(_("_id") == question_criteria_id).map(_("name").toString)
              } yield (themeName, criteriaName)

              result.foreach { case (themeName, criteriaName) =>
                domain_name = themeName
                criteria_name = criteriaName
                println(s"Domain Name: $themeName, Criteria Name: $criteriaName")
              }

              val payload = questionsMap.get("payload") match {
                case Some(value: Map[String, Any]) => Some(value)
                case _ => None
              }
              val responseType = questionsMap.get("responseType").map(_.toString).getOrElse("")

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

                  processQuestion(responseType, questionsMap, payload, question_id, domain_name, criteria_name, has_parent_question, parent_question_text, evidences, remarks, report_type)
                } else {
                  processQuestion(responseType, questionsMap, payload, question_id, domain_name, criteria_name, false, null, evidences, remarks, report_type)
                }
              } else {
                println(s"Skipping question_id=$question_id as payload is missing.")
              }
            }
          case _ =>
            logger.error("Unexpected structure for answers field")
        }

      }

      /**
       * Logic to populate kafka messages for creating metabase dashboard
       */
      if (statusOfSubmission == "completed") {
        val dashboardData = new java.util.HashMap[String, String]()
        val dashboardConfig = Seq(
          ("admin", "1", "admin"),
          ("program", programId, "targetedProgram"),
          ("solution", solutionId, "targetedSolution")
        )

        dashboardConfig
          .filter { case (key, _, _) => config.reportsEnabled.contains(key) }
          .foreach { case (key, value, target) =>
            checkAndInsert(key, value, dashboardData, target)
          }

        if (!dashboardData.isEmpty) {
          dashboardData.put("isRubric", isRubric.toString)
          dashboardData.put("entityType", entityType)
          pushObservationDashboardEvents(dashboardData, context)
        }
      }

      def checkAndInsert(entityType: String, targetedId: String, dashboardData: java.util.HashMap[String, String], dashboardKey: String): Unit = {
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
                val (entityColumn, sourceTable) = entityType match {
                  case "program"  => (s"${entityType}_name", config.solutions)
                  case "solution" => ("name", config.solutions)
                }

                val getEntityNameQuery =
                  s"""
                     |SELECT DISTINCT $entityColumn AS ${entityType}_name
                     |FROM $sourceTable
                     |WHERE ${entityType}_id = '$targetedId'
                     |""".stripMargin.replaceAll("\n", " ")

                val result = postgresUtil.fetchData(getEntityNameQuery)
                result.foreach { id =>
                  val entityName = id.get(s"${entityType}_name").map(_.toString).getOrElse("")

                  if (entityType == "solution") {
                    // Special insert/upsert logic for solution only
                    val upsertQuery =
                      s"""INSERT INTO ${config.dashboard_metadata} (
                         |    entity_type, entity_name, entity_id,
                         |    report_type, is_rubrics, parent_name, linked_to
                         |) VALUES (?, ?, ?, ?, ?, ?, ?)
                         |ON CONFLICT (entity_id) DO UPDATE SET
                         |    entity_type = EXCLUDED.entity_type,
                         |    entity_name = EXCLUDED.entity_name,
                         |    report_type = EXCLUDED.report_type,
                         |    is_rubrics = EXCLUDED.is_rubrics,
                         |    parent_name = EXCLUDED.parent_name,
                         |    linked_to = EXCLUDED.linked_to
                         |""".stripMargin

                    val params = Seq(
                      entityType, entityName, targetedId,
                      "observation", isRubric.asInstanceOf[AnyRef], event.entityType, programId
                    )
                    postgresUtil.executePreparedUpdate(upsertQuery, params, config.dashboard_metadata, targetedId)
                    println(s"Inserted [$entityName : $targetedId] with reportType=observation, isRubric=$isRubric, parent_name=${event.entityType}, linked_to=$programId.")
                  } else {
                    // Default logic for program or others
                    val insertQuery =
                      s"""INSERT INTO ${config.dashboard_metadata} (
                         |    entity_type, entity_name, entity_id
                         |) VALUES ('$entityType', '$entityName', '$targetedId')
                         |ON CONFLICT (entity_id) DO NOTHING
                         |""".stripMargin.replaceAll("\n", " ")

                    val affectedRows = postgresUtil.insertData(insertQuery)
                    println(s"Inserted [$entityName : $targetedId] with default metadata. Affected rows: $affectedRows")
                  }

                  dashboardData.put(dashboardKey, targetedId)
                }
              }
          }
        }
      }

      def checkExistenceOfFilterData(filterList: List[Map[String, String]], context: ProcessFunction[Event, Event]#Context, solutionId: String): Unit = {
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
              pushObservationDashboardEvents(eventData, context)
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

      def pushObservationDashboardEvents(dashboardData: util.HashMap[String, String], context: ProcessFunction[Event, Event]#Context): util.HashMap[String, AnyRef] = {
        val objects = new util.HashMap[String, AnyRef]() {
          put("_id", java.util.UUID.randomUUID().toString)
          put("reportType", "Observation")
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
    } else {
      println(s"Skipping the observation event with Id = ${event._id} and status = ${event.status} as it is not in a valid status.")
    }
  }
}