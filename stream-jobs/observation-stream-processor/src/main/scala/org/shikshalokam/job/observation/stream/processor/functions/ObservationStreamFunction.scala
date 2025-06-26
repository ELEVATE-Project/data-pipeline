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
      val userStateName = event.stateName
      val userStateId = event.stateId
      val userDistrictName = event.districtName
      val userDistrictId = event.districtId
      val userBlockName = event.blockName
      val userBlockId = event.blockId
      val userClusterName = event.clusterName
      val userClusterId = event.clusterId
      val userSchoolName = event.schoolName
      val userSchoolId = event.schoolId
      val themes = event.themes
      val criteriaEvent = event.criteria
      val solutionId = event.solutionId
      val solutionName = event.solutionName
      val programName = event.programName
      val programId = event.programId
      val observationName = event.observationName
      val observationId = event.observationId
      val tenantId = event.tenantId
      val solutionExternalId = event.solutionExternalId
      val solutionDescription = event.solutionDescription
      val programExternalId = event.programExternalId
      val programDescription = event.programDescription
      val privateProgram = null
      val projectCategories = null
      val projectDuration = null
      val completedDate = event.completedDate
      val domainTable = s""""${solutionId}_domain""""
      val questionTable = s""""${solutionId}_questions""""
      val statusTable = s""""${solutionId}_status""""
      val isRubric = event.isRubric
      val entityType = event.entityType
      val targetedStateName = event.targetedStateName
      val targetedStateId = event.targetedStateId
      val targetedDistrictName = event.targetedDistrictName
      val targetedDistrictId = event.targetedDistrictId
      val targetedBlockName = event.targetedBlockName
      val targetedBlockId = event.targetedBlockId
      val targetedClusterName = event.targetedClusterName
      val targetedClusterId = event.targetedClusterId
      val targetedSchoolName = event.targetedSchoolName
      val targetedSchoolId = event.targetedSchoolId

      println(s"statusOfSubmission = $statusOfSubmission")
      println(s"submissionId = $submissionId")
      println(s"userId = $userId")
      println(s"submissionNumber = $submissionNumber")
      println(s"userStateName = $userStateName")
      println(s"userStateId = $userStateId")
      println(s"userDistrictName = $userDistrictName")
      println(s"userDistrictId = $userDistrictId")
      println(s"userBlockName = $userBlockName")
      println(s"userBlockId = $userBlockId")
      println(s"userClusterName = $userClusterName")
      println(s"userClusterId = $userClusterId")
      println(s"userSchoolName = $userSchoolName")
      println(s"userSchoolId = $userSchoolId")
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
      println(s"targetedStateName = $targetedStateName")
      println(s"targetedStateId = $targetedStateId")
      println(s"targetedDistrictName = $targetedDistrictName")
      println(s"targetedDistrictId = $targetedDistrictId")
      println(s"targetedBlockName = $targetedBlockName")
      println(s"targetedBlockId = $targetedBlockId")
      println(s"targetedClusterName = $targetedClusterName")
      println(s"targetedClusterId = $targetedClusterId")
      println(s"targetedSchoolName = $targetedSchoolName")
      println(s"targetedSchoolId = $targetedSchoolId")
      println("\n\n")

      val createDomainsTable =
        s"""CREATE TABLE IF NOT EXISTS $domainTable (
           |    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
           |    user_id TEXT ,
           |    user_role_ids TEXT,
           |    user_roles TEXT,
           |    solution_id TEXT ,
           |    solution_name TEXT,
           |    submission_id TEXT,
           |    submission_number INTEGER,
           |    program_name TEXT,
           |    program_id TEXT,
           |    observation_name TEXT,
           |    observation_id TEXT,
           |    tenant_id TEXT,
           |    org_name TEXT,
           |    org_id TEXT,
           |    org_code TEXT,
           |    user_state_name TEXT,
           |    user_state_id TEXT,
           |    user_district_name TEXT,
           |    user_district_id TEXT,
           |    user_block_name TEXT,
           |    user_block_id TEXT,
           |    user_cluster_name TEXT,
           |    user_cluster_id TEXT,
           |    user_school_name TEXT,
           |    user_school_id TEXT,
           |    domain TEXT,
           |    domain_level TEXT,
           |    criteria TEXT,
           |    criteria_level TEXT,
           |    completed_date TEXT,
           |    entityType TEXT,
           |    parent_one_name TEXT,
           |    parent_one_id TEXT,
           |    parent_two_name TEXT,
           |    parent_two_id TEXT,
           |    parent_three_name TEXT,
           |    parent_three_id TEXT,
           |    parent_four_name TEXT,
           |    parent_four_id TEXT,
           |    parent_five_name TEXT,
           |    parent_five_id TEXT
           |);""".stripMargin

      val createQuestionsTable =
        s"""CREATE TABLE IF NOT EXISTS $questionTable (
           |    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
           |    user_id TEXT ,
           |    user_role_ids TEXT,
           |    user_roles TEXT,
           |    solution_id TEXT ,
           |    solution_name TEXT,
           |    submission_id TEXT,
           |    submission_number INTEGER,
           |    status_of_submission TEXT,
           |    program_name TEXT,
           |    program_id TEXT,
           |    observation_name TEXT,
           |    observation_id TEXT,
           |    tenant_id TEXT,
           |    org_name TEXT,
           |    org_id TEXT,
           |    org_code TEXT,
           |    user_state_name TEXT,
           |    user_state_id TEXT,
           |    user_district_name TEXT,
           |    user_district_id TEXT,
           |    user_block_name TEXT,
           |    user_block_id TEXT,
           |    user_cluster_name TEXT,
           |    user_cluster_id TEXT,
           |    user_school_name TEXT,
           |    user_school_id TEXT,
           |    question_id TEXT,
           |    question_text TEXT,
           |    labels TEXT,
           |    value TEXT,
           |    score INTEGER,
           |    domain_name TEXT,
           |    criteria_name TEXT,
           |    has_parent_question BOOLEAN,
           |    parent_question_text TEXT,
           |    evidence TEXT,
           |    submitted_at TEXT,
           |    remarks TEXT,
           |    question_type TEXT,
           |    entityType TEXT,
           |    parent_one_name TEXT,
           |    parent_one_id TEXT,
           |    parent_two_name TEXT,
           |    parent_two_id TEXT,
           |    parent_three_name TEXT,
           |    parent_three_id TEXT,
           |    parent_four_name TEXT,
           |    parent_four_id TEXT,
           |    parent_five_name TEXT,
           |    parent_five_id TEXT
           |);""".stripMargin

      val createStatusTable =
        s"""CREATE TABLE IF NOT EXISTS $statusTable (
           |    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
           |    user_id TEXT ,
           |    user_role_ids TEXT,
           |    user_roles TEXT,
           |    solution_id TEXT ,
           |    solution_name TEXT,
           |    submission_id TEXT,
           |    submission_number INTEGER,
           |    program_name TEXT,
           |    program_id TEXT,
           |    observation_name TEXT,
           |    observation_id TEXT,
           |    user_state_name TEXT,
           |    user_state_id TEXT,
           |    user_district_name TEXT,
           |    user_district_id TEXT,
           |    user_block_name TEXT,
           |    user_block_id TEXT,
           |    user_cluster_name TEXT,
           |    user_cluster_id TEXT,
           |    user_school_name TEXT,
           |    user_school_id TEXT,
           |    tenant_id TEXT,
           |    org_id TEXT,
           |    org_code TEXT,
           |    org_name TEXT,
           |    status_of_submission TEXT,
           |    submitted_at TEXT,
           |    entityType TEXT,
           |    parent_one_name TEXT,
           |    parent_one_id TEXT,
           |    parent_two_name TEXT,
           |    parent_two_id TEXT,
           |    parent_three_name TEXT,
           |    parent_three_id TEXT,
           |    parent_four_name TEXT,
           |    parent_four_id TEXT,
           |    parent_five_name TEXT,
           |    parent_five_id TEXT
           |);""".stripMargin

      postgresUtil.createTable(config.createDashboardMetadataTable, config.dashboard_metadata)

      /**
       * Creating required tables
       */
      checkAndCreateTable(statusTable, createStatusTable)
      checkAndCreateTable(questionTable, createQuestionsTable)
      if (isRubric != false) checkAndCreateTable(domainTable, createDomainsTable)

      /**
       * Performs an upsert operation on the solution table irrespective of the submission status.
       * This ensures the solution information is always updated/inserted in the database.
       */
      if (solutionId != null) {
        println("===> Processing observation solution data")
        postgresUtil.createTable(config.createSolutionsTable, config.solutions)
        val upsertSolutionQuery =
          s"""INSERT INTO ${config.solutions} (solution_id, external_id, name, description, duration, categories, program_id, program_name, program_external_id, program_description, private_program)
             |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
             |    private_program = ?;
             |""".stripMargin

        val solutionParams = Seq(
          // Insert parameters
          solutionId, solutionExternalId, solutionName, solutionDescription, projectDuration, projectCategories, programId, programName, programExternalId, programDescription, privateProgram,

          // Update parameters (matching columns in the ON CONFLICT clause)
          solutionExternalId, solutionName, solutionDescription, projectDuration, projectCategories, programId, programName, programExternalId, programDescription, privateProgram
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
        val deleteQuery = s"""DELETE FROM $statusTable WHERE user_id = ? AND submission_id = ? AND submission_number = ?; """
        val deleteParams = Seq(userId, submissionId, submissionNumber)
        postgresUtil.executePreparedDelete(deleteQuery, deleteParams, statusTable, submissionId)

        val insertStatusQuery =
          s"""INSERT INTO $statusTable (
             |    user_id, user_role_ids, user_roles, solution_id, solution_name, submission_id, submission_number,
             |    program_name, program_id, observation_name, observation_id, user_state_name, user_state_id,
             |    user_district_name, user_district_id, user_block_name, user_block_id, user_cluster_name,
             |    user_cluster_id, user_school_name, user_school_id, tenant_id, org_id, org_code, org_name,
             |    status_of_submission, submitted_at, entityType, parent_one_name, parent_one_id, parent_two_name,
             |    parent_two_id, parent_three_name, parent_three_id, parent_four_name, parent_four_id,
             |    parent_five_name, parent_five_id
             |) VALUES (
             |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             |);""".stripMargin

        val statusParams = Seq(
          userId, userRoleIds, userRoles, solutionId, solutionName, submissionId, submissionNumber,
          programName, programId, observationName, observationId, userStateName, userStateId,
          userDistrictName, userDistrictId, userBlockName, userBlockId, userClusterName,
          userClusterId, userSchoolName, userSchoolId, tenantId, orgId, orgCode, orgName,
          statusOfSubmission, completedDate, entityType, targetedStateName, targetedStateId, targetedDistrictName,
          targetedDistrictId, targetedBlockName, targetedBlockId, targetedClusterName, targetedClusterId,
          targetedSchoolName, targetedSchoolId
        )
        postgresUtil.executePreparedUpdate(insertStatusQuery, statusParams, statusTable, submissionId)
        println("\n\n")
      }

      /**
       * Performs an delete and insert operation on the observation domain and question tables only is submission status is completed.
       * This ensures the observation domain and question tables will always holds latest data in the database.
       */
      if (statusOfSubmission == "completed") {
        println("===> Processing observation domain data")
        if (isRubric != false) {
          checkAndCreateTable(domainTable, createDomainsTable)
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
                           |    user_state_name, user_state_id, user_district_name, user_district_id, user_block_name, user_block_id,
                           |    user_cluster_name, user_cluster_id, user_school_name, user_school_id, domain, domain_level,
                           |    criteria, criteria_level, completed_date, entityType, parent_one_name, parent_one_id,
                           |    parent_two_name, parent_two_id, parent_three_name, parent_three_id, parent_four_name,
                           |    parent_four_id, parent_five_name, parent_five_id
                           |) VALUES (
                           |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                           |);
                           |""".stripMargin

                      val criteriaParams = Seq(
                        userId, userRoleIds, userRoles, solutionId, solutionName, submissionId, submissionNumber, programName, programId, observationName,
                        observationId, tenantId, orgName, orgId, orgCode, userStateName, userStateId, userDistrictName, userDistrictId, userBlockName,
                        userBlockId, userClusterName, userClusterId, userSchoolName, userSchoolId, domainName, domainLevel, criteriaName, criteriaLevel,
                        completedDate, entityType, targetedStateName, targetedStateId, targetedDistrictName, targetedDistrictId, targetedBlockName, targetedBlockId,
                        targetedClusterName, targetedClusterId, targetedSchoolName, targetedSchoolId)

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
        val deleteQuery = s"""DELETE FROM $questionTable WHERE user_id = ? AND submission_id = ? AND submission_number = ?; """
        val deleteParams = Seq(userId, submissionId, submissionNumber)
        postgresUtil.executePreparedDelete(deleteQuery, deleteParams, questionTable, submissionId)
        val questionsFunction = new ObservationQuestionFunction(postgresUtil, config, questionTable)
        val answersKey = event.answers

        def processQuestion(responseType: String, questionsMap: Map[String, Any], payload: Option[Map[String, Any]], questionId: String, domainName: String,
                            criteriaName: String, hasParentQuestion: Boolean, parentQuestionText: String, evidences: String, remarks: String): Unit = {
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
            userStateName = userStateName, userStateId = userStateId, userDistrictName = userDistrictName, userDistrictId = userDistrictId, userBlockName = userBlockName, userBlockId = userBlockId,
            userClusterName = userClusterName, userClusterId = userClusterId, userSchoolName = userSchoolName, userSchoolId = userSchoolId,
            tenantId = tenantId, orgId = orgId, orgCode = orgCode, orgName = orgName, statusOfSubmission = statusOfSubmission, submittedAt = completedDate, entityType = entityType,
            parentOneName = targetedStateName, parentOneId = targetedStateId, parentTwoName = targetedDistrictName, parentTwoId = targetedDistrictId, parentThreeName = targetedBlockName, parentThreeId = targetedBlockId,
            parentFourName = targetedClusterName, parentFourId = targetedClusterId, parentFiveName = targetedSchoolName, parentFiveId = targetedSchoolId,
            domainName = domainName, criteriaName = criteriaName, score = score, hasParentQuestion = hasParentQuestion, parentQuestionText = parentQuestionText, evidences = evidences, remarks = remarks
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
                      processQuestion(matrixResponseType, matrixQuestionMap, matrixPayload, matrixQuestionId, domainName, criteriaName, hasParentQuestion, parentQuestionText, evidences, remarks)
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

                  processQuestion(responseType, questionsMap, payload, question_id, domain_name, criteria_name, has_parent_question, parent_question_text, evidences, remarks)
                } else {
                  processQuestion(responseType, questionsMap, payload, question_id, domain_name, criteria_name, false, null, evidences, remarks)
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