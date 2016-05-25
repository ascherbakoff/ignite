//package ru.sbt.bm.ignite.services
//
//import java.util.Calendar
//
//import com.typesafe.scalalogging.StrictLogging
//import org.apache.ignite.IgniteCache
//import org.apache.ignite.scalar.scalar._
//import ru.sbt.bm.cep.data.Status
//
//
//class IgniteReaderZeroRecord(
//  cache:IgniteCache[Long, Status],
//  startIteration: Long,
//  iterationCount: Long,
//  queryText: String) extends Runnable with StrictLogging {
//
//  def run(): Unit = {
//    val currentDate = Calendar.getInstance().getTime
//    logger.debug("Start read no records")
//    for (i <- startIteration until (startIteration + iterationCount)) {
//      val query = cache.sqlFields(queryText, s"NolocalId$i", "Application1", "BusinessProcess1", "Operation1")
//      query.getAll
//    }
//    logger.debug("Finish read no records")
//  }
//}
