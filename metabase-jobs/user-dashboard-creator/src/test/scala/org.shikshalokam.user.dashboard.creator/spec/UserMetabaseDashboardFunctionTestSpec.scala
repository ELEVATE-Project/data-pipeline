package org.shikshalokam.user.dashboard.creator.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.shikshalokam.BaseTestSpec
import org.shikshalokam.job.connector.FlinkKafkaConnector
import org.shikshalokam.job.user.dashboard.creator.domain.Event
import org.shikshalokam.job.user.dashboard.creator.task.{UserMetabaseDashboardConfig, MetabaseDashboardTask}

class UserMetabaseDashboardFunctionTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())

  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: UserMetabaseDashboardConfig = new UserMetabaseDashboardConfig(config)


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    //Embedded Postgres connection
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }

  def initialize():Unit = {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.inputTopic))
      .thenReturn(new UserMetabaseEventSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.inputTopic)).thenReturn(new GenerateMetabaseDashboardSink)
  }

  "Metabase Dashboard Creator Job " should "execute successfully " in {
    initialize()
    new MetabaseDashboardTask(jobConfig, mockKafkaUtil).process()
  }
}
