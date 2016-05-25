//package ru.sbt.bm.ignite
//
//import java.util.Calendar
//import java.util.concurrent.Executors
//
//import com.typesafe.scalalogging.StrictLogging
//import org.apache.ignite.{IgniteCache, Ignition, Ignite}
//import ru.sbt.bm.cep.data.Status
//import ru.sbt.bm.ignite.services.IgniteReaderZeroRecord
//
//import scala.io.StdIn
//
//import org.apache.ignite.scalar.scalar._
//
//object ReaderNoRecordsMain extends App with StrictLogging {
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
//  val statusCount = 10000
//  val threadCount = 1
//  val StatusCountByThread = statusCount / threadCount
//
//  val threadPool = Executors.newFixedThreadPool(threadCount)
//  (0 until threadCount).foreach {
//    threadNumber =>
//      threadPool.submit(
//        new IgniteReaderZeroRecord(
//          cache = scalaStatuses,
//          startIteration = threadNumber * StatusCountByThread,
//          iterationCount = StatusCountByThread,
//          findEventByLocalIdSql
//        )
//      )
//  }
//
//  StdIn.readLine("Press any key to close\n")
//  ignite.close()
//}
