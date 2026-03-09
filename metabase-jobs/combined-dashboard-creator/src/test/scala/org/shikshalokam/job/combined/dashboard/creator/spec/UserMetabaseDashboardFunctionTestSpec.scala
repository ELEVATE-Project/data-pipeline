package org.shikshalokam.job.combined.dashboard.creator.spec

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.shikshalokam.BaseTestSpec
import org.shikshalokam.job.combined.dashboard.creator.domain.{MentoringEvent, ObservationEvent, ProjectEvent, SurveyEvent, UserEvent}
import org.shikshalokam.job.combined.dashboard.creator.task.{CombinedDashboardCreatorConfig, CombinedDashboardCreatorTask}
import org.shikshalokam.job.connector.FlinkKafkaConnector

class UserMetabaseDashboardFunctionTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[UserEvent] = TypeExtractor.getForClass(classOf[UserEvent])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())

  val config: Config = ConfigFactory.load("test.conf")
    .withValue("kafka.input.mentoring.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("kafka.input.observation.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("kafka.input.survey.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("kafka.input.project.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("kafka.input.userservice.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("kafka.input.programservice.enabled", ConfigValueFactory.fromAnyRef(false))
  val jobConfig: CombinedDashboardCreatorConfig = new CombinedDashboardCreatorConfig(config)


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }

  def initialize(): Unit = {
    when(mockKafkaUtil.kafkaJobRequestSource[UserEvent](jobConfig.userInputTopic))
      .thenReturn(new UserMetabaseEventSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.userInputTopic)).thenReturn(new GenerateUserSink)
  }

  "Combined Dashboard Creator Job - User" should "execute successfully" in {
    initialize()
    CombinedDashboardCreatorTask.runJob(jobConfig, mockKafkaUtil)
  }
}
