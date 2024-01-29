package com.fairmoney

import pureconfig._
import pureconfig.generic.auto._

case class SerializerConfig(schemaRegistryUrl: String)
case class KafkaProducerConfig(bootstrapServers: String, topicName: String, topicPartitions: Int)
case class JobConfig(producerConfig: KafkaProducerConfig, serializerConfig: SerializerConfig)

object JobConfig {
  def configuration(): JobConfig = {
    ConfigSource.default.load[JobConfig] match {
      case Left(value) => throw new Exception(value.prettyPrint())
      case Right(value) => value
    }
  }
}

