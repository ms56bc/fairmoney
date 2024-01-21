package com.fairmoney

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.formats.avro.AvroInputFormat
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.kafka.clients.consumer.OffsetResetStrategy

object TransactionSource {


  def createSource(conf: JobConfiguration, env: StreamExecutionEnvironment): DataStream[Transaction] = {
    if(conf.isBackfill) fileSource(conf, env)
    else kafkaSource(conf, env)
  }
  private def kafkaSource(conf: JobConfiguration, env: StreamExecutionEnvironment): DataStream[Transaction] = {
    val deserializationSchema = configureDeserializerAndGetSchema(conf)
    val source = KafkaSource.builder[Transaction]
      .setBootstrapServers(conf.consumerConfig.bootstrapServers)
      .setTopics(conf.consumerConfig.topicName)
      .setGroupId(conf.consumerConfig.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .setValueOnlyDeserializer(deserializationSchema)
      .build
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source ML features")
  }

  private def fileSource(conf: JobConfiguration, env: StreamExecutionEnvironment): DataStream[Transaction] = {
    val users = new AvroInputFormat[Transaction](new org.apache.flink.core.fs.Path(s"file:///${conf.backfillInputPath}"), classOf[Transaction])
    env.createInput(users).name("File Source ML features")
  }

  private def configureDeserializerAndGetSchema(conf: JobConfiguration): DeserializationSchema[Transaction] = {
    ConfluentRegistryAvroDeserializationSchema.forSpecific(classOf[Transaction], conf.deserializerConfig.schemaRegistryUrl)
  }
}
