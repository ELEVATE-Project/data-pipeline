package org.shikshalokam.job.combined.stream.processor.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.shikshalokam.BaseTestSpec
import org.shikshalokam.job.combined.stream.processor.domain.ProjectEvent
import org.shikshalokam.job.combined.stream.processor.task.{UnifiedStreamConfig, UnifiedStreamTask}
import org.shikshalokam.job.connector.FlinkKafkaConnector

import java.io.File

class ProjectStreamFunctionTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[ProjectEvent] = TypeExtractor.getForClass(classOf[ProjectEvent])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())

  val config: Config = ConfigFactory.parseFile(new File("unified-pipeline.conf")).resolve().withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: UnifiedStreamConfig = new UnifiedStreamConfig(config)


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
    when(mockKafkaUtil.kafkaJobRequestSource[ProjectEvent](jobConfig.projectInputTopic))
      .thenReturn(new ProjectEventSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.projectOutputTopic))
      .thenReturn(new GenerateProjectSink)
  }

  "Project Stream Job " should "execute successfully " in {
    initialize()
    UnifiedStreamTask.runJob(jobConfig, mockKafkaUtil, "project")
  }

}