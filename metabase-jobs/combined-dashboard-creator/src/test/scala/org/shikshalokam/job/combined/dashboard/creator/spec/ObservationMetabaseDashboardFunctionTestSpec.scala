package org.shikshalokam.job.combined.dashboard.creator.spec

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.shikshalokam.BaseTestSpec
import org.shikshalokam.job.combined.dashboard.creator.domain.ObservationEvent
import org.shikshalokam.job.combined.dashboard.creator.task.{CombinedDashboardCreatorConfig, CombinedDashboardCreatorTask}
import org.shikshalokam.job.connector.FlinkKafkaConnector

import java.io.File

class ObservationMetabaseDashboardFunctionTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[ObservationEvent] = TypeExtractor.getForClass(classOf[ObservationEvent])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())

  val config: Config = ConfigFactory.parseFile(new File("unified-test.conf")).resolve().withFallback(ConfigFactory.systemEnvironment())
    .withValue("combined.mentoring.dashboard.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.project.dashboard.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.survey.dashboard.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.user.dashboard.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.user.mapping.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.program.mapping.job.enabled", ConfigValueFactory.fromAnyRef(false))

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
    when(mockKafkaUtil.kafkaJobRequestSource[ObservationEvent](jobConfig.observationInputTopic))
      .thenReturn(new ObservationMetabaseEventSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.observationInputTopic)).thenReturn(new GenerateObservationSink)
  }

  "Combined Dashboard Creator Job - Observation" should "execute successfully" in {
    initialize()
    CombinedDashboardCreatorTask.runJob(jobConfig, mockKafkaUtil)
  }
}
