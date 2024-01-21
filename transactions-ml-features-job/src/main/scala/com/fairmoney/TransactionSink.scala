package com.fairmoney

import com.datastax.driver.core.Cluster
import com.datastax.driver.mapping.Mapper
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.AvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}

object TransactionSink {
  def executeCassandraSink(inStream: DataStream[TransactionCountByUser], conf: JobConfiguration): CassandraSink[TransactionCountByUser] = {
    CassandraSink.addSink(inStream)
      .setClusterBuilder(new ClusterBuilder() {
        override protected def buildCluster(builder: Cluster.Builder): Cluster = {
          builder.addContactPoint(conf.sinkConfig.cassandraHost).build
        }
      })
      .setMapperOptions(() => Array[Mapper.Option](Mapper.Option.saveNullFields(true)))
      .build()
  }

  def getFileSink(conf: JobConfiguration): FileSink[UserStats] = {
    FileSink
      .forBulkFormat(new Path(conf.sinkConfig.outputPath), AvroWriters.forSpecificRecord(classOf[UserStats]))
      .withBucketAssigner(new DateTimeBucketAssigner())
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()
  }
}
