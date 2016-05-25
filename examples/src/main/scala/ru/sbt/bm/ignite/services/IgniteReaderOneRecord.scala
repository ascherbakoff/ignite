//package ru.sbt.bm.ignite.services
//
//import java.util.Calendar
//
//import com.typesafe.scalalogging.StrictLogging
//import org.apache.ignite.IgniteCache
//import org.apache.ignite.scalar.scalar._
//import ru.sbt.bm.cep.data.Status
//
//class IgniteReaderOneRecord(
//  val cache:IgniteCache[Long, Status],
//  val startIteration: Long,
//  val iterationCount: Long,
//  val sqlText: String) extends Runnable with StrictLogging {
//
//  def run(): Unit = {
//    logger.debug("Start read one records")
//    for (i <- startIteration until (startIteration + iterationCount)) {
//      val query = cache.sqlFields(sqlText, s"localId$i", "Application1", "BusinessProcess1", "Operation1")
//      if (query.getAll.isEmpty) {
//        logger.error("No events found")
//      }
//    }
//    logger.debug("Finish read one records")
//  }
//}
