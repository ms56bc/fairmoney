package com.fairmoney

import org.apache.flink.api.java.utils.ParameterTool
import pureconfig._
import pureconfig.generic.auto._

case class DeserializerConfig(schemaRegistryUrl: String)
case class ConsumerConfig(bootstrapServers: String, topicName: String, groupId: String)
case class CheckpointConfig(intervalMilliSeconds: Int, timeoutSeconds: Int, tolerableCheckpointFailureNumber: Int = 2)
case class JobConfiguration(isBackfill: Boolean,
                            backfillInputPath: String,
                            consumerConfig: ConsumerConfig,
                            deserializerConfig: DeserializerConfig,
                            sinkConfig: SinkConfig,
                            checkpointConfig: CheckpointConfig)

case class SinkConfig(outputPath: String, cassandraHost: String, cassandraPort: Int)

object JobConfiguration {
  def configuration(params: ParameterTool): JobConfiguration = {
    ConfigSource.default.load[JobConfiguration] match {
      case Left(value) => throw new Exception(value.prettyPrint())
      case Right(value) => overrideParams(params, value)
    }
  }
  private def overrideParams(params: ParameterTool, conf: JobConfiguration): JobConfiguration = {
    val paramsMap = params.toMap
    val isBackfill: Boolean = paramsMap.getOrDefault("backfill", conf.isBackfill.toString).toBoolean
    val backfillInputPath: String = paramsMap.getOrDefault("backfillpath", conf.backfillInputPath)
    conf.copy(isBackfill = isBackfill, backfillInputPath = backfillInputPath)
  }
}
