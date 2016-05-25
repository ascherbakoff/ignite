package ru.sbt.bm.ignite

import java.io.File

import com.typesafe.config.ConfigFactory

/**
  * Client config class for external config definition
  * @param threadNumber number of worker threads to submit to pool
  * @param statusCount summary number of statuses to put
  * @param isAsync enable asynchronous client mode
  */
case class IgniteClientConfig(threadNumber: Int,
                              statusCount: Int,
                              isAsync: Boolean) {

}

object IgniteClientConfig {
  def apply(file: String): IgniteClientConfig = {
    val root = ConfigFactory.parseFile(new File(file)).getConfig("ignite-client")
    IgniteClientConfig(
      root.getInt("threadNumber"),
      root.getInt("statusCount"),
      root.getBoolean("isAsync")
    )
  }
}
