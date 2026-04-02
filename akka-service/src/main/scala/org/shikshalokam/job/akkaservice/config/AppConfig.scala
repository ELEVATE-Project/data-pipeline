package org.shikshalokam.job.akkaservice.config

import com.typesafe.config.{Config, ConfigFactory}
import java.io.File

object AppConfig {
  val config: Config = {
    val configuredFile = System.getProperty("config.file", "unified-common.conf")
    val file = new File(configuredFile)
    
    val rootConfig = if (file.exists()) {
      println(s"[INFO] Loading config from resolved path: ${file.getAbsolutePath}")
      ConfigFactory.parseFile(file).resolve()
    } else {
      println(s"[INFO] Config file not found on disk, attempting to load from classpath: $configuredFile")
      ConfigFactory.load(configuredFile)
    }
    
    rootConfig.withFallback(ConfigFactory.load())
  }
}