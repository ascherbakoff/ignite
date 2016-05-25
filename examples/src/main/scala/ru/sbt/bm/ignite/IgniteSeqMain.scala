package ru.sbt.bm.ignite

import org.apache.ignite.{Ignition, Ignite}

import scala.io.StdIn

object IgniteSeqMain extends App {
    val ignite: Ignite = Ignition.start("test-seq-client.xml")

    val seqLongMax = ignite.atomicSequence("TestSeqMaxLong", Long.MaxValue, true)
    println(seqLongMax.incrementAndGet())
    println(seqLongMax.incrementAndGet())
    println(seqLongMax.get())
    println(seqLongMax.getAndIncrement())
    println(seqLongMax.get())

    val seqLongZero = ignite.atomicSequence("TestSeqZero", 0, true)
    println(seqLongZero.incrementAndGet())
    println(seqLongZero.incrementAndGet())
    println(seqLongZero.incrementAndGet())

    StdIn.readLine("Press any key to close")
    ignite.close()
}
