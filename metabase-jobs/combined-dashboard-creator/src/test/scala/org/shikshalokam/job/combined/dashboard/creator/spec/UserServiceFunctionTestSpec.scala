package org.shikshalokam.job.combined.dashboard.creator.spec

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.shikshalokam.BaseTestSpec
import org.shikshalokam.job.connector.FlinkKafkaConnector
import org.shikshalokam.job.combined.dashboard.creator.domain.UserMappingEvent
import org.shikshalokam.job.combined.dashboard.creator.task.{CombinedDashboardCreatorConfig, CombinedDashboardCreatorTask}
import org.shikshalokam.job.util.MetabaseUtil

import java.io.File

class UserServiceFunctionTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[UserMappingEvent] = TypeExtractor.getForClass(classOf[UserMappingEvent])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val mockMetabaseUtil: MetabaseUtil = mock[MetabaseUtil](Mockito.withSettings().serializable())

  val config: Config = ConfigFactory.parseFile(new File("unified-test.conf")).resolve().withFallback(ConfigFactory.systemEnvironment())
    .withValue("combined.mentoring.dashboard.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.observation.dashboard.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.survey.dashboard.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.project.dashboard.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.user.dashboard.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.user.mapping.job.enabled", ConfigValueFactory.fromAnyRef(true))
    .withValue("combined.program.mapping.job.enabled", ConfigValueFactory.fromAnyRef(true))

  val jobConfig: CombinedDashboardCreatorConfig = new CombinedDashboardCreatorConfig(config)


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }

  def initialize() {
    when(mockKafkaUtil.kafkaJobRequestSource[UserMappingEvent](jobConfig.userServiceInputTopic))
      .thenReturn(new UserServiceEventSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.notificationOutputTopic))
      .thenReturn(new GenerateUserServiceSink)
    when(mockKafkaUtil.kafkaJobRequestSource[UserMappingEvent](jobConfig.programServiceInputTopic))
      .thenReturn(new ProgramServiceEventSource)
  }

  "User Service Job " should "execute successfully " in {
    initialize()
    CombinedDashboardCreatorTask.runJob(jobConfig, mockKafkaUtil, null, mockMetabaseUtil)
  }
}
