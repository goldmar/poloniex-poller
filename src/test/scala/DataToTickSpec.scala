import java.time.Instant
import org.scalatest._

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
    val asks = Seq(
      OrderBookItem(0.06, 10)
    )
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)

    tick.bidPriceAvg5 should be (Some(-0.2))
  }
}
