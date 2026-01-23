package org.shikshalokam.job.combined.dashboard.creator.spec

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.shikshalokam.BaseTestSpec
import org.shikshalokam.job.combined.dashboard.creator.domain.SurveyEvent
import org.shikshalokam.job.combined.dashboard.creator.task.{CombinedDashboardCreatorConfig, CombinedDashboardCreatorTask}
import org.shikshalokam.job.connector.FlinkKafkaConnector

class SurveyMetabaseDashboardFunctionTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[SurveyEvent] = TypeExtractor.getForClass(classOf[SurveyEvent])
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
    .withValue("kafka.input.project.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("kafka.input.user.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("kafka.input.userservice.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("kafka.input.programservice.enabled", ConfigValueFactory.fromAnyRef(false))

  val jobConfig: CombinedDashboardCreatorConfig = new CombinedDashboardCreatorConfig(config)


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    //Embedded Postgres connection
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }

  def initialize(): Unit = {
    when(mockKafkaUtil.kafkaJobRequestSource[SurveyEvent](jobConfig.surveyInputTopic, jobConfig.surveyConsumerGroup))
      .thenReturn(new SurveyMetabaseEventSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.surveyInputTopic)).thenReturn(new GenerateSurveySink)
  }

  "Combined Dashboard Creator Job - Survey" should "execute successfully" in {
    initialize()
    CombinedDashboardCreatorTask.runJob(jobConfig, mockKafkaUtil)
  }
}
