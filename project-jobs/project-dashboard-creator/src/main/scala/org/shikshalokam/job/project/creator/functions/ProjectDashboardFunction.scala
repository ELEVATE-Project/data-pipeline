package org.shikshalokam.job.project.creator.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.BaseProcessFunction
import org.shikshalokam.job.util.PostgresUtil
import org.shikshalokam.job.project.creator.domain.Event
import org.shikshalokam.job.project.creator.task.ProjectDashboardConfig
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.collection.immutable._

class ProjectDashboardFunction(config: ProjectDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProjectDashboardFunction])

  override def metricsList(): List[String] = {
    List(config.projectsCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val pgHost: String = config.pgHost
    val pgPort: String = config.pgPort
    val pgUsername: String = config.pgUsername
    val pgPassword: String = config.pgPassword
    val pgDataBase: String = config.pgDataBase
    val metabaseUrl: String = config.metabaseUrl
    val metabaseUsername: String = config.metabaseUsername
    val metabasePassword: String = config.metabasePassword
    val connectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$pgDataBase"
    postgresUtil = new PostgresUtil(connectionUrl, pgUsername, pgPassword)
    metabaseUtil = new MetabaseUtil(metabaseUrl, metabaseUsername, metabasePassword)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    println(s"***************** Start of Processing the Project Event with Id = ${event._id}*****************")

    //TODO: Remove the below lines and build actual logic
    val listCollections = metabaseUtil.listCollections()
    println("Collections JSON = " + listCollections)

    val listDashboards = metabaseUtil.listDashboards()
    println("Dashboards JSON = " + listDashboards)

    val getDashboardInfo = metabaseUtil.getDashboardDetailsById(257)
    println("Dashboard Info JSON = " + getDashboardInfo)

    val listDatabaseDetails = metabaseUtil.listDatabaseDetails()
    println("Database Details JSON = " + listDatabaseDetails)

    val getDatabaseMetadata = metabaseUtil.getDatabaseMetadata(34)
    println("Database Metadata JSON = " + getDatabaseMetadata)

//    val collectionRequestBody =
//      """{
//        |  "name": "New Collection",
//        |  "description": "Collection for project reports"
//        |}""".stripMargin
//    val createColelction = metabaseUtil.createCollection(collectionRequestBody)
//    println("Create Collection JSON = " + createColelction)
//
//
//    val dashboardRequestBody =
//      """{
//        |  "name": "New Dashboard1",
//        |  "collection_id": "273"
//        |}""".stripMargin
//    val createDashboard = metabaseUtil.createDashboard(dashboardRequestBody)
//    println("Create Dashboard Json = " + createDashboard)
//
//    val questionCardRequestBody =
//      """{
//        |    "name": "No. of improvements in inProgress status currently",
//        |    "collection_id": 273,
//        |    "dataset_query": {
//        |        "database": 34,
//        |        "type": "native",
//        |        "native": {
//        |            "query": "SELECT\n  COUNT(DISTINCT projects.projectid) AS \"no_of_projects_inprogress\"\nFROM\n  projects\nJOIN solutions on projects.solutionid = solutions.solutionid\nWHERE\nprojects.status = 'inProgress'\n[[AND {{state_param}} ]]\n[[AND {{district_param}}]]\n[[AND {{program_param}}]]",
//        |            "template-tags": {
//        |                "state_param": {
//        |                    "type": "dimension",
//        |                    "name": "state_param",
//        |                    "display-name": "State Param",
//        |                    "default": null,
//        |                    "dimension": [
//        |                        "field",
//        |                        189,
//        |                        null
//        |                    ],
//        |                    "widget-type": "string/=",
//        |                    "options": null
//        |                },
//        |                "district_param": {
//        |                    "type": "dimension",
//        |                    "name": "district_param",
//        |                    "display-name": "District Param",
//        |                    "default": null,
//        |                    "dimension": [
//        |                        "field",
//        |                        196,
//        |                        null
//        |                    ],
//        |                    "widget-type": "string/=",
//        |                    "options": null
//        |                },
//        |                "program_param": {
//        |                    "type": "dimension",
//        |                    "name": "program_param",
//        |                    "display-name": "Program Param",
//        |                    "default": null,
//        |                    "dimension": [
//        |                        "field",
//        |                        173,
//        |                        null
//        |                    ],
//        |                    "widget-type": "string/=",
//        |                    "options": null
//        |                }
//        |            }
//        |        }
//        |    },
//        |    "display": "scalar",
//        |    "visualization_settings": {},
//        |    "parameters": [
//        |        {
//        |            "id": "state_param",
//        |            "type": "string/=",
//        |            "target": [
//        |                "dimension",
//        |                [
//        |                    "template-tag",
//        |                    "state_param"
//        |                ]
//        |            ],
//        |            "name": "State Param"
//        |        },
//        |        {
//        |            "id": "district_param",
//        |            "type": "string/=",
//        |            "target": [
//        |                "dimension",
//        |                [
//        |                    "template-tag",
//        |                    "district_param"
//        |                ]
//        |            ],
//        |            "name": "District Param"
//        |        },
//        |        {
//        |            "id": "program_param",
//        |            "type": "string/=",
//        |            "target": [
//        |                "dimension",
//        |                [
//        |                    "template-tag",
//        |                    "program_param"
//        |                ]
//        |            ],
//        |            "name": "Program Param"
//        |        }
//        |    ]
//        |}""".stripMargin
//
//    val createQuestionCard = metabaseUtil.createQuestionCard(questionCardRequestBody)
//    println("Create Question Card Json = " + createQuestionCard)
//
//
//    val addQuestionToDashboardRequestBody =
//      """{
//        |    "card_id": 654,
//        |    "dashboard_tab_id": null,
//        |    "id": 2,
//        |    "col": 6,
//        |    "row": 0,
//        |    "size_x": 6,
//        |    "size_y": 3,
//        |    "visualization_settings": {},
//        |    "parameter_mappings": [
//        |        {
//        |            "parameter_id": "c32c8fc5",
//        |            "card_id": 654,
//        |            "target": [
//        |                "dimension",
//        |                [
//        |                    "template-tag",
//        |                    "state_param"
//        |                ]
//        |            ]
//        |        },
//        |        {
//        |            "parameter_id": "74a10335",
//        |            "card_id": 654,
//        |            "target": [
//        |                "dimension",
//        |                [
//        |                    "template-tag",
//        |                    "district_param"
//        |                ]
//        |            ]
//        |        },
//        |        {
//        |            "parameter_id": "8c7d86ea",
//        |            "card_id": 654,
//        |            "target": [
//        |                "dimension",
//        |                [
//        |                    "template-tag",
//        |                    "program_param"
//        |                ]
//        |            ]
//        |        }
//        |    ]
//        |}
//        |""".stripMargin
//
//    val addQuestionToDashboard = metabaseUtil.addQuestionCardToDashboard(257, addQuestionToDashboardRequestBody)
//    println("Add Question To Dashboard Json = " + addQuestionToDashboard)

    println(s"***************** End of Processing the Project Event *****************\n")
  }
}
