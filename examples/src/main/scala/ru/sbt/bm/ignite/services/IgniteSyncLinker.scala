package ru.sbt.bm.ignite.services

import java.util.Calendar

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.transactions.{TransactionIsolation, TransactionConcurrency}
import org.apache.ignite.{Ignite, IgniteCache}
import org.apache.ignite.lang.IgniteUuid

class IgniteSyncLinker (
  ignite: Ignite,
  cache: IgniteCache[String, IgniteUuid],
  startIteration: Integer,
  iterationCount: Integer
) extends Runnable with StrictLogging with Linker {

  def run(): Unit = {
    logger.info("Start sync")

    val transactions = ignite.transactions()

    for (i <- startIteration.longValue() until (startIteration + iterationCount)) {
      for(x <- 0 until 9) {
        val currentTx = transactions.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)
        try {
          val now = s"${Calendar.getInstance.getTime}"
          cache.put(s"$i", new IgniteUuid())
          val value = cache.get(s"$i")
          currentTx.commit()
          counter += 1
        } catch {
          case _: Throwable => currentTx.rollback()
        } finally {
          currentTx.close()
        }
      }
    }
    logger.info("Finish sync")
  }
}
