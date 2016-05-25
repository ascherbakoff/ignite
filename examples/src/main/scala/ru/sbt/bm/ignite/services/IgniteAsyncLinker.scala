package ru.sbt.bm.ignite.services

import java.util.Calendar

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.{Ignite, IgniteCache}
import org.apache.ignite.lang.{IgniteUuid, IgniteInClosure, IgniteFuture}

class IgniteAsyncLinker(
  ignite: Ignite,
  cache: IgniteCache[String, IgniteUuid],
  startIteration: Integer,
  iterationCount: Integer
) extends Runnable with StrictLogging with Linker {

  def run(): Unit = {
    logger.info("Start async")
    for (i <- startIteration.longValue() until (startIteration + iterationCount)) {
      val now = s"${Calendar.getInstance.getTime}"
      cache.get(now)
      val future = cache.future[IgniteUuid]()
      future.listen(new IgniteInClosure[IgniteFuture[IgniteUuid]] {
        def apply(e: IgniteFuture[IgniteUuid]): Unit = {
          if(e.get() == null) {
            cache.put(now, new IgniteUuid())
            logger.info(s"notified $i")
          }
        }
        logger.info(s"iterate $i")
        counter += 1
      })
    }
    logger.info("Finish async")
  }
}
