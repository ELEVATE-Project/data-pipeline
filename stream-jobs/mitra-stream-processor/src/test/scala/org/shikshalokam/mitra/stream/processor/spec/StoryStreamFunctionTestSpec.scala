package org.shikshalokam.mitra.stream.processor.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.shikshalokam.BaseTestSpec
import org.shikshalokam.job.connector.FlinkKafkaConnector
import org.shikshalokam.job.mitra.stream.processor.domain.StoryEvent
import org.shikshalokam.job.mitra.stream.processor.task.{MitraStreamConfig, MitraStreamTask}

class StoryStreamFunctionTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[StoryEvent] = TypeExtractor.getForClass(classOf[StoryEvent])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())

  val config: Config = ConfigFactory.load("mitra-stream-test.conf")
  val jobConfig: MitraStreamConfig = new MitraStreamConfig(config)


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
    when(mockKafkaUtil.kafkaJobRequestSource[StoryEvent](jobConfig.storyInputTopic))
      .thenReturn(new StoryEventSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.storyOutputTopic))
      .thenReturn(new GenerateStorySink)
  }

  "Story Stream Job " should "execute successfully " in {
    initialize()
    MitraStreamTask.runJob(jobConfig, mockKafkaUtil, Option("story"))
  }

}