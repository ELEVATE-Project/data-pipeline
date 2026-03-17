package org.shikshalokam.job.akkaservice.config

import com.typesafe.config.{Config, ConfigFactory}
import java.io.File

object AppConfig {
  private val configPath = "unified-test.conf" //Change this to unified-test.conf for testing

  private val rootConfig = if (new File(configPath).exists()) {
    println(s"[INFO] Loading config from: $configPath")
    ConfigFactory.parseFile(new File(configPath)).resolve()
  } else {
    println(s"[INFO] File not found: $configPath, loading from classpath")
    ConfigFactory.load(configPath)
  }

  val config: Config = rootConfig.withFallback(ConfigFactory.load())
}