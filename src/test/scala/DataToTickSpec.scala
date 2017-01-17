import java.time.Instant

import org.scalatest._

import scala.math.BigDecimal.RoundingMode

class DataToTickSpec extends FlatSpec with Matchers {
  val timestamp = Instant.now.toEpochMilli
  val currencyPair = "BTC_XMR"

  "The field askPriceAvg5" should "contain the correct result in test2" in {
    val bidAskMidpoint = 0.00000006
    val bids = Seq()
    val asks = Seq(
      OrderBookItem(0.00000007, 1),
      OrderBookItem(0.00000008, 1),
      OrderBookItem(0.00000009, 100),
      OrderBookItem(1, 10))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)

    tick.askPriceAvg5 should be(None)
  }

  "The field askPriceAvg5" should "contain the correct result in test4" in {
    val bidAskMidpoint = 0.004518
    val bids = Seq()
    val asks = Seq(
      OrderBookItem(0.004540, 20.00688013),
      OrderBookItem(0.004550, 108.57951253),
      OrderBookItem(0.004558, 46.61712455),
      OrderBookItem(0.004559, 0.24837307),
      OrderBookItem(0.004562, 1.68731409),
      OrderBookItem(0.004578, 0.47692810),
      OrderBookItem(0.004579, 1310.63426800))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)

    tick.askPriceAvg5.map(_.setScale(8, RoundingMode.HALF_UP)) should be(Some(0.01251312))
  }

  "The field askPriceAvg5" should "contain the correct result in test5" in {
    val bidAskMidpoint = 0.0000345
    val bids = Seq()
    val asks = Seq(
      OrderBookItem(0.000035, 10000),
      OrderBookItem(0.000035, 10000),
      OrderBookItem(0.0000351, 1000000))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)

    tick.askPriceAvg5.map(_.setScale(8, RoundingMode.HALF_UP)) should be(Some(0.01699130))
  }

  "The field bidPriceAvg5" should "contain the correct result in test6" in {
    val bidAskMidpoint = 0.000887
    val bids = Seq(
      OrderBookItem(0.000886, 944.2191104),
      OrderBookItem(0.00087999, 64.3342463),
      OrderBookItem(0.000615993, 10000))
    val asks = Seq()
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)

    tick.bidPriceAvg5.map(_.setScale(8, RoundingMode.HALF_UP)) should be(Some(-0.25114617))
  }

  "The field askPriceAvg5" should "contain the correct result in test8" in {
    val bidAskMidpoint = 0.005
    val bids = Seq()
    val asks = Seq(
      OrderBookItem(0.0052, 500),
      OrderBookItem(0.0054, 500))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)

    tick.askPriceAvg5 should be(Some(0.06))
  }

  "The field askPriceAvg5" should "contain the correct result in test9" in {
    val bidAskMidpoint = 0.005
    val bids = Seq()
    val asks = Seq(
      OrderBookItem(0.0054, 500),
      OrderBookItem(0.0058, 500))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)

    tick.askPriceAvg5 should be(Some(0.12))
  }

  "The field bidAmountSum5percent" should "contain the correct result in test10" in {
    val bidAskMidpoint = 100
    val bids = Seq(
      OrderBookItem(99, 100),
      OrderBookItem(98, 100),
      OrderBookItem(95, 100),
      OrderBookItem(94.99, 100))
    val asks = Seq()
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)

    tick.bidAmountSum5percent should be(Some(29200))
  }

  "The field bidAmountSum75percent" should "contain the correct result in test11" in {
    val bidAskMidpoint = 0.05
    val bids = Seq(
      OrderBookItem(0.05, 20),
      OrderBookItem(0.01251, 20),
      OrderBookItem(0.0125, 20))
    val asks = Seq()
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)

    tick.bidAmountSum75percent should be(Some(1.5002))
  }

  "The field askAmountSumNC100percent" should "contain the correct result in test12" in {
    val bidAskMidpoint = 0.049
    val bids = Seq()
    val asks = Seq(
      OrderBookItem(0.049685, 0.06682804),
      OrderBookItem(0.05, 2),
      OrderBookItem(0.098, 1000),
      OrderBookItem(0.0981, 1000))
    val offersOption = None

    val specialCSVTick =
      SpecialCSVTick.fromTick(
        CSVTick.fromTick(
          PoloniexDataSaverActor.dataToTick(
            timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)))

    specialCSVTick.askAmountSumNC100percent should be(Some(1002.06682804))
  }

  "The field askAmountSumNC300percent" should "contain the correct result in test13" in {
    val bidAskMidpoint = 0.05
    val bids = Seq()
    val asks = Seq(
      OrderBookItem(0.049, 100),
      OrderBookItem(0.2, 100))
    val offersOption = None

    val specialCSVTick =
      SpecialCSVTick.fromTick(
        CSVTick.fromTick(
          PoloniexDataSaverActor.dataToTick(
            timestamp, currencyPair, bidAskMidpoint, bids, asks, offersOption)))

    specialCSVTick.askAmountSumNC300percent should be(Some(200))
  }
}
