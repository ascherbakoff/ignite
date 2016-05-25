package ru.sbt.bm.ignite.services.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

trait Kafka {

  private val kafkaConsumerProperties = new Properties()
  kafkaConsumerProperties.put("bootstrap.servers", "localhost:9096")
  kafkaConsumerProperties.put("group.id", "igniteReader")
  kafkaConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaConsumerProperties.put("enable.auto.commit", "true")
  kafkaConsumerProperties.put("auto.commit.interval.ms", "1000")

  val kafkaProducerProperties = new Properties()
  kafkaProducerProperties.put("bootstrap.servers", "localhost:9096")
  kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  val kafkaConsumer = new KafkaConsumer[String, String](kafkaConsumerProperties)
  kafkaConsumer.subscribe(util.Arrays.asList("CEP.UPS.IN"))

  val kafkaProducer = new KafkaProducer[String, String](kafkaProducerProperties)

  def close(): Unit ={
    kafkaConsumer.commitSync()
    kafkaConsumer.unsubscribe()
    kafkaConsumer.close()
  }
}
