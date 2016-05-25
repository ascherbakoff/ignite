package ru.sbt.bm.ignite

import java.io.File
import java.util.concurrent.Executors
import java.util.{Timer, TimerTask}

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.lang.IgniteUuid
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import ru.sbt.bm.ignite.services.{IgniteAsyncLinker, IgniteSyncLinker}

import scala.io.StdIn
import scala.util.{Failure, Success, Try}

object FastMain extends App with LoggerInitializer with StrictLogging {
  logger.info("Start client")

  val config = Try(IgniteClientConfig("configs/application.conf")) match {
    case Success(configuration) => configuration
    case Failure(exception) =>
      logger.error("Config parse error", exception)
      sys.exit(1)
  }

  val statusCount = config.statusCount
  val threadCount = config.threadNumber
  val isAsync = config.isAsync
  val StatusCountByThread = statusCount / threadCount

  logger.info(s"Running client with following parameters:\n\tStatus count: $statusCount\n\tThread count:$threadCount\n\tisAsynch:$isAsync")
  logger.info(s"Status count per thread is $StatusCountByThread")

  val ignite: Ignite = Ignition.start("configs/test-client-config.xml")
  val scalaStatuses: IgniteCache[String, IgniteUuid] =
    if (isAsync) {
      logger.info("Create async cache")
      ignite.getOrCreateCache("Links").withAsync()
    } else {
      logger.info("Create sync cache")
      ignite.getOrCreateCache("Links")
    }

  val threadPool = Executors.newFixedThreadPool(threadCount)
  val workers =
    for (threadNumber <- 0 until threadCount) yield {
    logger.info(s"Starting thread #$threadNumber")
    val worker = if (scalaStatuses.isAsync) {
      new IgniteAsyncLinker(
        ignite,
        scalaStatuses,
        startIteration = threadNumber * StatusCountByThread,
        iterationCount = StatusCountByThread
      )
    } else {
      new IgniteSyncLinker(
        ignite,
        scalaStatuses,
        startIteration = threadNumber * StatusCountByThread,
        iterationCount = StatusCountByThread
      )
    }
    threadPool.submit(worker)
    worker
  }

  val period = 1000
  val timer = new Timer()
  timer.scheduleAtFixedRate(new TimerTask {
    var counter = 0
    def run(): Unit = {
      val currentCounter = workers.map(_.counter).sum
      val delta = currentCounter - counter
      counter = currentCounter
      logger.info(s"Deltat $delta for period $period ms")
    }
  }, 10, period)

  StdIn.readLine("Press any key for close\n")
  threadPool.shutdown()
  timer.cancel()
  ignite.close()
  logger.info("Finish client")
}

trait LoggerInitializer {
  System.setProperty("logback.configurationFile", new File("configs", "logback.xml").getCanonicalPath)
}
