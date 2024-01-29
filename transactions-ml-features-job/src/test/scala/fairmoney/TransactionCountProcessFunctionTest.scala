package fairmoney

import com.fairmoney.{Transaction, TransactionCountByUser, TransactionCountProcessFunction}
import org.apache.flink.api.common.typeinfo.{TypeInformation}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, ProcessFunctionTestHarnesses}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.time.Instant

class TransactionCountProcessFunctionTest extends AnyFlatSpec with Matchers with BeforeAndAfter {
  var harness: KeyedOneInputStreamOperatorTestHarness[Int, Transaction, TransactionCountByUser] = null
  val min = 60
  val max = 120
  before {
    val statefulFlatMapFunction: KeyedProcessFunction[Int, Transaction, TransactionCountByUser]  = new TransactionCountProcessFunction(min, max)
    harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction[Int, Transaction, TransactionCountByUser](statefulFlatMapFunction, _.getUserId, TypeInformation.of(classOf[Int]))
  }

  "TransactionCountProcessFunction" should "count transactions correctly" in {

    harness.setup()
    harness.open()

    try {
      // Send some transactions
      val time = Instant.now()
      val timeService = harness.getProcessingTimeService
      timeService.setCurrentTime(time.toEpochMilli)
      harness.processElement(createRandomTransaction(time))
      harness.processElement(createRandomTransaction(time))
      harness.processElement(createRandomTransaction(time))
      // advance time within expire interval
      timeService.setCurrentTime(time.plusSeconds(min -1).toEpochMilli)

      val resultTimeNotExpired = harness.extractOutputValues()
      resultTimeNotExpired should have size 0

      // advance time outside expire interval
      timeService.setCurrentTime(time.plusSeconds(max).toEpochMilli)
      val resultTimeExpired = harness.extractOutputValues()
      resultTimeExpired should have size 1
      resultTimeExpired.get(0).getCount should be (3)
    } finally {
      // Close the test harness
      harness.close()
    }
  }
  private def createRandomTransaction(instant: Instant): StreamRecord[Transaction] = {
    val amount = scala.util.Random.nextInt(10000)
    val userId = 1
    val counterpartId = 1
    val t = Transaction.newBuilder
      .setAmount(amount)
      .setCurrency("EUR")
      .setTransactionTimestampMillis(System.currentTimeMillis())
      .setUserId(userId)
      .setCounterpartId(counterpartId)
      .build
    new StreamRecord(t, instant.toEpochMilli)
  }
}

