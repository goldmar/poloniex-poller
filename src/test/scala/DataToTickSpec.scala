//import java.time.Instant
//
//import org.scalatest._
//
//import scala.math.BigDecimal.RoundingMode
//
//class DataToTickSpec extends FlatSpec with Matchers {
//  val timestamp = Instant.now.toEpochMilli
//  val currencyPair = "BTC_XMR"
//
//  "The field askPriceAvg5" should "contain the correct result in test2" in {
//    val bidAskMidpoint = 0.00000006
//    val bids = Seq()
//    val asks = Seq(
//      OrderBookItem(0.00000007, 1),
//      OrderBookItem(0.00000008, 1),
//      OrderBookItem(0.00000009, 100),
//      OrderBookItem(1, 10)
//    )
//    val offersOption = None
//
//    val tick = PoloniexDataSaverActor.dataToTick(
//      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)
//
//    val askPriceAvgAbs5 = tick.askPriceAvg5
//      .map(a => (1 + a) * bidAskMidpoint)
//      .map(_.setScale(8, RoundingMode.HALF_UP))
//
//    askPriceAvgAbs5 should be(None)
//  }
//
//  "The field askPriceAvg5" should "contain the correct result in test4" in {
//    val bidAskMidpoint = 0.004518
//    val bids = Seq()
//    val asks = Seq(
//      OrderBookItem(0.004540, 20.00688013),
//      OrderBookItem(0.004550, 108.57951253),
//      OrderBookItem(0.004558, 46.61712455),
//      OrderBookItem(0.004559, 0.24837307),
//      OrderBookItem(0.004562, 1.68731409),
//      OrderBookItem(0.004578, 0.47692810),
//      OrderBookItem(0.004579, 1310.63426800)
//    )
//    val offersOption = None
//
//    val tick = PoloniexDataSaverActor.dataToTick(
//      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)
//
//    tick.askPriceAvg5 should be(None)
//  }
//
//  "The field askPriceAvg5" should "contain the correct result in test5" in {
//    val bidAskMidpoint = 0.0000345
//    val bids = Seq()
//    val asks = Seq(
//      OrderBookItem(0.000035, 10000),
//      OrderBookItem(0.000035, 10000),
//      OrderBookItem(0.0000351, 1000000)
//    )
//    val offersOption = None
//
//    val tick = PoloniexDataSaverActor.dataToTick(
//      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)
//
//    tick.askPriceAvg5 should be(None)
//  }
//
//  "The field bidPriceAvg5" should "contain the correct result in test6" in {
//    val bidAskMidpoint = 0.000887
//    val bids = Seq(
//      OrderBookItem(0.000886, 944.2191104),
//      OrderBookItem(0.00087999, 64.3342463),
//      OrderBookItem(0.000615993, 10000)
//    )
//    val asks = Seq()
//    val offersOption = None
//
//    val tick = PoloniexDataSaverActor.dataToTick(
//      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)
//
//    tick.bidPriceAvg5 should be(None)
//  }
//
//  "The field askPriceAvg5" should "contain the correct result in test8" in {
//    val bidAskMidpoint = 0.005
//    val bids = Seq()
//    val asks = Seq(
//      OrderBookItem(0.0052, 500),
//      OrderBookItem(0.0054, 500)
//    )
//    val offersOption = None
//
//    val tick = PoloniexDataSaverActor.dataToTick(
//      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)
//
//    tick.askPriceAvg5 should be(None)
//  }
//
//  "The field askPriceAvg5" should "contain the correct result in test9" in {
//    val bidAskMidpoint = 0.005
//    val bids = Seq()
//    val asks = Seq(
//      OrderBookItem(0.0054, 500),
//      OrderBookItem(0.0058, 500)
//    )
//    val offersOption = None
//
//    val tick = PoloniexDataSaverActor.dataToTick(
//      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)
//
//    tick.askPriceAvg5 should be(None)
//  }
//}
