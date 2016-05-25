package ru.sbt.bm.ignite

import java.util.{UUID, Calendar}

object UUIDGenerationTest extends App {
  val start = Calendar.getInstance().getTimeInMillis
  //try to generate 1000000 uuids by java
  for (i <- 0 until 1000000)
    UUID.randomUUID()

  val stop = Calendar.getInstance().getTimeInMillis

  println(s"Generation time: ${stop - start} ms")

}
