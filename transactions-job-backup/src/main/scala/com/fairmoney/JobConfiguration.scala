package com.fairmoney

import pureconfig._
import pureconfig.generic.auto._

case class DeserializerConfig(schemaRegistryUrl: String)
case class ConsumerConfig(bootstrapServers: String, topicName: String, groupId: String)
case class CheckpointConfig(intervalMilliSeconds: Int, timeoutSeconds: Int, tolerableCheckpointFailureNumber: Int = 2)
case class JobConfiguration(consumerConfig: ConsumerConfig, deserializerConfig: DeserializerConfig, sinkConfig: SinkConfig, checkpointConfig: CheckpointConfig)

case class SinkConfig(outputPath: String)

object JobConfiguration {
  def configuration(): JobConfiguration = {
    ConfigSource.default.load[JobConfiguration] match {
      case Left(value) => throw new Exception(value.prettyPrint())
      case Right(value) => value
    }
  }
}
