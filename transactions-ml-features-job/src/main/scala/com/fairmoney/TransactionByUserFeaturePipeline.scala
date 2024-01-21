package com.fairmoney

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
object TransactionByUserFeaturePipeline {
    def calculate(datastream: DataStream[Transaction]): DataStream[TransactionCountByUser] = {
        datastream
            .keyBy(_.getUserId)
            .process(new TransactionCountProcessFunction())
    }
}
