package com.fairmoney

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.AvroWriters
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.OffsetResetStrategy

object TransactionsBackupJob {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val conf = JobConfiguration.configuration()
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(conf.checkpointConfig.intervalMilliSeconds, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(conf.checkpointConfig.timeoutSeconds)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(conf.checkpointConfig.tolerableCheckpointFailureNumber)

    val outputBasePath = new Path(conf.sinkConfig.outputPath)

    val schema = ConfluentRegistryAvroDeserializationSchema.forSpecific(classOf[Transaction], conf.deserializerConfig.schemaRegistryUrl)

    val source: KafkaSource[Transaction] = KafkaSource.builder[Transaction]
      .setBootstrapServers(conf.consumerConfig.bootstrapServers)
      .setTopics(conf.consumerConfig.topicName)
      .setGroupId(conf.consumerConfig.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .setValueOnlyDeserializer(schema)
      .build

    val sink: FileSink[Transaction] = FileSink
      .forBulkFormat(outputBasePath, AvroWriters.forSpecificRecord(classOf[Transaction]))
       .withBucketAssigner(new DateTimeBucketAssigner())
       .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Transaction Backup")
      .sinkTo(sink)

    env.execute("Transactions Backup Job")
  }
}


