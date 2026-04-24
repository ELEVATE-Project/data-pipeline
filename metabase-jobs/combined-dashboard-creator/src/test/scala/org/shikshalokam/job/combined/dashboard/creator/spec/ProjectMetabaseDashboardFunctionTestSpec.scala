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
import org.shikshalokam.job.combined.dashboard.creator.domain.ProjectEvent
import org.shikshalokam.job.combined.dashboard.creator.task.{CombinedDashboardCreatorConfig, CombinedDashboardCreatorTask}


import java.io.File

class ProjectMetabaseDashboardFunctionTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[ProjectEvent] = TypeExtractor.getForClass(classOf[ProjectEvent])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())

  val config: Config = ConfigFactory.parseFile(new File("unified-test.conf")).resolve().withFallback(ConfigFactory.systemEnvironment())
    .withValue("combined.mentoring.dashboard.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.observation.dashboard.job.enabled", ConfigValueFactory.fromAnyRef(false))
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

  def initialize() {
    when(mockKafkaUtil.kafkaJobRequestSource[ProjectEvent](jobConfig.projectInputTopic))
      .thenReturn(new ProjectMetabaseEventSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.projectInputTopic)).thenReturn(new GenerateProjectSink)
  }

  "Metabase Dashboard Creator Job " should "execute successfully " in {
    initialize()
    CombinedDashboardCreatorTask.runJob(jobConfig, mockKafkaUtil)
  }
}
