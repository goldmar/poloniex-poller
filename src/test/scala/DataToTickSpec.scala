import java.time.Instant

import org.scalatest._

import scala.math.BigDecimal.RoundingMode

class DataToTickSpec extends FlatSpec with Matchers {
  val timestamp = Instant.now.toEpochMilli
  val currencyPair = "BTC_XMR"

  "The field bidPriceAvg5" should "contain the correct result" in {
    val bidAskMidpoint = 0.05
    val bids = Seq(
      OrderBookItem(0.05, 20),
      OrderBookItem(0.04, 60),
      OrderBookItem(0.03, 200)
    )
    val asks = Seq()
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)

    tick.bidPriceAvg5 should be(Some(-0.2))
  }

  "The field askPriceAvg5" should "contain the correct result" in {
    val bidAskMidpoint = 0.00000006
    val bids = Seq()
    val asks = Seq(
      OrderBookItem(0.00000007, 1),
      OrderBookItem(0.00000008, 1),
      OrderBookItem(0.00000009, 100),
      OrderBookItem(1, 10)
    )
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)

    val askPriceAvgAbs5 = tick.askPriceAvg5
      .map(a => (1 + a) * bidAskMidpoint)
      .map(_.setScale(8, RoundingMode.HALF_UP))

    askPriceAvgAbs5 should be(Some(0.99999817))
  }
}
