package com.fairmoney

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class TransactionCountProcessFunction(eventEmitIntervalSecondsMin: Int = 60,
                                      eventEmitIntervalSecondsMax: Int = 120) extends KeyedProcessFunction[Int, Transaction, TransactionCountByUser] {

  private lazy val state: ValueState[TransactionCountByUser] = getRuntimeContext
    .getState(new ValueStateDescriptor[TransactionCountByUser]("transaction_count", classOf[TransactionCountByUser]))

  private lazy val lastEmitTimestampState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("lastEmitTimestamp", classOf[Long]))
  override def processElement(
                               value: Transaction,
                               ctx: KeyedProcessFunction[Int, Transaction, TransactionCountByUser]#Context,
                               out: Collector[TransactionCountByUser]): Unit = {
    val currentTimeMillis = ctx.timerService().currentProcessingTime()
    val current: Option[TransactionCountByUser] = Option(state.value())
    val updated = current match {
      case Some(value) => new TransactionCountByUser(value.getUserId, value.getCount + 1, currentTimeMillis)
      case None => new TransactionCountByUser(value.getUserId, 1, currentTimeMillis)
    }
    state.update(updated)
    setRandomTriggerWithinBound(ctx, currentTimeMillis)
  }
  private def setRandomTriggerWithinBound(ctx: KeyedProcessFunction[Int, Transaction, TransactionCountByUser]#Context,
                                          currentTimeMillis: Long): Unit = {
    val eventEmitIntervalSeconds = scala.util.Random.nextInt(eventEmitIntervalSecondsMax - eventEmitIntervalSecondsMin) + eventEmitIntervalSecondsMin
    ctx.timerService.registerProcessingTimeTimer(currentTimeMillis + eventEmitIntervalSeconds * 1000)
  }
  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[Int, Transaction, TransactionCountByUser]#OnTimerContext,
                        out: Collector[TransactionCountByUser]): Unit = {
    if(lastEmitTimestampState.value() == null || isEmitThresholdReached(timestamp, lastEmitTimestampState.value())) {
      out.collect(state.value())
      lastEmitTimestampState.update(timestamp)
    }
  }

  private def isEmitThresholdReached(currentTimeMillis: Long, lastEmitTimestampState: Long): Boolean = {
    currentTimeMillis - lastEmitTimestampState > eventEmitIntervalSecondsMin * 1000
  }
}