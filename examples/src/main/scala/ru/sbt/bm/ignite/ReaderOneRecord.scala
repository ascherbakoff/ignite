//package ru.sbt.bm.ignite
//
//import java.util.Calendar
//import java.util.concurrent.Executors
//
//import com.typesafe.scalalogging.StrictLogging
//import org.apache.ignite.{Ignite, IgniteCache, Ignition}
//import ru.sbt.bm.cep.data.Status
//import ru.sbt.bm.ignite.services.{IgniteReaderZeroRecord, IgniteReaderOneRecord}
//
//import scala.io.StdIn
//
//object ReaderOneRecord extends App with StrictLogging {
//  val ignite: Ignite = Ignition.start("test-client-config.xml")
//  val scalaStatuses: IgniteCache[Long, Status] = ignite.getOrCreateCache("Statuses")
//
//  val currentDate = Calendar.getInstance().getTime
//
//  val findEventByLocalIdSql = "SELECT * FROM \"Statuses\".STATUS where localId = ? " +
//    "and applicationCode = ? " +
//    "and businessProcessTemplateMnemonic = ? " +
//    "and operationTemplateMnemonic = ? limit 1"
//
//  val statusCount = 1000L
//  val threadCount = 1
//  val StatusCountByThread = statusCount / threadCount
//
//  logger.debug("Start write items to cache")
//  val now = Calendar.getInstance().getTime
//  for(i <- 0L until statusCount) {
//    scalaStatuses.put(i,
//      Status(
//        id = i,
//        businessProcessId = 0,
//        operationId = 0,
//        businessProcessTemplateMnemonic = "BP1",
//        operationTemplateMnemonic = "OP1",
//        statusTemplateMnemonic = "Status1",
//        applicationCode = "App1",
//        localID = s"localId$i",
//        localID2 = "localID2",
//        statusDate = now,
//        interfaceID = "",
//        interfaceID2 = "",
//        interfaceApplicationCode2 = ""
//      )
//    )
//  }
//  logger.debug("Finish write items to cache")
//
//  StdIn.readLine("Press any key to continue\n")
//
//  val threadPool = Executors.newFixedThreadPool(threadCount)
//  (0 until threadCount).foreach {
//    threadNumber =>
//      threadPool.submit(
//        new IgniteReaderOneRecord(
//          cache = scalaStatuses,
//          startIteration = threadNumber * StatusCountByThread,
//          iterationCount = StatusCountByThread,
//          findEventByLocalIdSql
//        )
//      )
//  }
//
//  StdIn.readLine("Press any key to close\n")
//  scalaStatuses.clear()
//  ignite.close()
//}
