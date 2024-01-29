package com.fairmoney


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._

object MLFeatureJob {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val conf = JobConfiguration.configuration(params)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    configureCheckpoint(conf, env)

    val sourceStream: DataStream[Transaction] = TransactionSource.createSource(conf, env)

    val inStream = TransactionByUserFeaturePipeline.calculate(sourceStream)

    if(!conf.isBackfill)
      TransactionSink.executeCassandraSink(inStream, conf)

    val fileSink = TransactionSink.getFileSink(conf)

    inStream.map(_.asUserStatsAvro()).sinkTo(fileSink).name("Parquet Sink ML features")

    env.execute("Transaction ML features")
  }

  private def configureCheckpoint(conf: JobConfiguration, env: StreamExecutionEnvironment): Unit = {
    val checkpointPath = "file:///data/checkpoints"
    env.setStateBackend(new HashMapStateBackend)
    env.getCheckpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath))
    env.getCheckpointConfig.setCheckpointInterval(conf.checkpointConfig.intervalMilliSeconds)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(conf.checkpointConfig.tolerableCheckpointFailureNumber)
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
  }
}
