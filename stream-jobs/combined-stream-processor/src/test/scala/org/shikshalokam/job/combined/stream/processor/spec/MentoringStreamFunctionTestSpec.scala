package org.shikshalokam.job.combined.stream.processor.spec

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.shikshalokam.BaseTestSpec
import org.shikshalokam.job.combined.stream.processor.domain.MentoringEvent
import org.shikshalokam.job.combined.stream.processor.task.{UnifiedStreamConfig, UnifiedStreamTask}
import org.shikshalokam.job.connector.FlinkKafkaConnector

import java.io.File


class MentoringStreamFunctionTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[MentoringEvent] = TypeExtractor.getForClass(classOf[MentoringEvent])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())

  val config: Config = ConfigFactory.parseFile(new File("unified-test.conf")).resolve().withFallback(ConfigFactory.systemEnvironment())
    .withValue("combined.project.stream.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.survey.stream.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.observation.stream.job.enabled", ConfigValueFactory.fromAnyRef(false))
    .withValue("combined.user.stream.job.enabled", ConfigValueFactory.fromAnyRef(false))
  val jobConfig: UnifiedStreamConfig = new UnifiedStreamConfig(config)


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }

  def initialize(): Unit = {
    when(mockKafkaUtil.kafkaJobRequestSource[MentoringEvent](jobConfig.mentoringInputTopic))
      .thenReturn(new MentoringEventSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.mentoringOutputTopic))
      .thenReturn(new GenerateMentoringSink)
  }

  "Mentoring Stream Job " should "execute successfully " in {
    initialize()
    UnifiedStreamTask.runJob(jobConfig, mockKafkaUtil)
  }

}