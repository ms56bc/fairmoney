package com.fairmoney


import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, Partitioner, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._

object TransactionProducer {

  def main(args: Array[String]): Unit = {
    val conf = JobConfig.configuration()
    val properties = Map[String, AnyRef](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> conf.producerConfig.bootstrapServers,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[KafkaAvroSerializer],
      ProducerConfig.PARTITIONER_CLASS_CONFIG -> classOf[HashPartitioner].getName,
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> conf.serializerConfig.schemaRegistryUrl
    ).asJava

    val producer = new KafkaProducer[String, Transaction](properties)
    while (true) {
      produceRandomTransaction(producer, conf.producerConfig.topicName)
      Thread.sleep(100)
    }
    producer.flush()
    producer.close()
  }
  private def produceRandomTransaction(producer: KafkaProducer[String, Transaction], topicName: String) {
    try {
      val transaction = createRandomTransaction()
      val record: ProducerRecord[String, Transaction] = new ProducerRecord[String, Transaction](topicName, transaction.getUserId.toString, createRandomTransaction())
      producer.send(record)
    } catch {
      case _: Throwable  => {
        Thread.sleep(1000)
      }
    }
  }
  private def createRandomTransaction(): Transaction = {
    val amount = scala.util.Random.nextInt(10000)
    val userId = getBoundedRandomInt(1, 10)
    val counterpartId = getBoundedRandomInt(1000, 2000)
    Transaction.newBuilder
      .setAmount(amount)
      .setCurrency(getCurrency)
      .setTransactionTimestampMillis(System.currentTimeMillis())
      .setUserId(userId)
      .setCounterpartId(counterpartId)
      .build
  }

  private def getCurrency: String = {
    val currencies = Array("EUR", "USD", "GBP", "CHF", "JPY")
    currencies(scala.util.Random.nextInt(currencies.length))
  }

  private def getBoundedRandomInt(lowerBound: Int, upperBound: Int): Int = {
    lowerBound + scala.util.Random.nextInt(upperBound - lowerBound)
  }

  private class HashPartitioner extends Partitioner {
    override def partition(topic: String, o: Any, bytes: Array[Byte], o1: Any, bytes1: Array[Byte], cluster: Cluster): Int = {
      val key = o.asInstanceOf[String]
      val partitionCount = cluster.partitionCountForTopic(topic)
      (key.hashCode() & Integer.MAX_VALUE) % partitionCount
    }

    override def close(): Unit = {}

    override def configure(configs: java.util.Map[String, _]): Unit = {}
  }
}