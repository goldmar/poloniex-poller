import java.time.Instant

import scala.math.BigDecimal.RoundingMode
import org.scalatest._

import Schema._

class DataToTickSpec extends FlatSpec with Matchers {
  "The tick" should "not be created without a bidAskMidpoint in test1" in {
    val bids = Vector()
    val asks = Vector(
      OrderBookItem(1, 1))
    val offersOption = None

    val tickOption = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption)

    tickOption should be (None)
  }

  "The field askPriceAvg5" should "contain the correct result in test2" in {
    val bids = Vector(
      OrderBookItem(0.00000005, 1))
    val asks = Vector(
      OrderBookItem(0.00000007, 1),
      OrderBookItem(0.00000008, 1),
      OrderBookItem(0.00000009, 100),
      OrderBookItem(1, 10))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.askPriceAvg5 should be (None)
  }

  "The field askPriceAvg5" should "contain the correct result in test4" in {
    val bids = Vector(
      OrderBookItem(0.004496, 1))
    val asks = Vector(
      OrderBookItem(0.004540, 20.00688013),
      OrderBookItem(0.004550, 108.57951253),
      OrderBookItem(0.004558, 46.61712455),
      OrderBookItem(0.004559, 0.24837307),
      OrderBookItem(0.004562, 1.68731409),
      OrderBookItem(0.004578, 0.47692810),
      OrderBookItem(0.004579, 1310.63426800))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.askPriceAvg5.map(_.setScale(8, RoundingMode.HALF_UP)) should be (Some(0.01251312))
  }

  "The field askPriceAvg5" should "contain the correct result in test5" in {
    val bids = Vector(
      OrderBookItem(0.000034, 1))
    val asks = Vector(
      OrderBookItem(0.000035, 10000),
      OrderBookItem(0.000035, 10000),
      OrderBookItem(0.0000351, 1000000))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.askPriceAvg5.map(_.setScale(8, RoundingMode.HALF_UP)) should be (Some(0.01699130))
  }

  "The field bidPriceAvg5" should "contain the correct result in test6" in {
    val bids = Vector(
      OrderBookItem(0.000886, 944.2191104),
      OrderBookItem(0.00087999, 64.3342463),
      OrderBookItem(0.000615993, 10000))
    val asks = Vector(
      OrderBookItem(0.000888, 1))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.bidPriceAvg5.map(_.setScale(8, RoundingMode.HALF_UP)) should be (Some(-0.25114617))
  }

  "The field askPriceAvg5" should "contain the correct result in test8" in {
    val bids = Vector(
      OrderBookItem(0.0048, 1))
    val asks = Vector(
      OrderBookItem(0.0052, 500),
      OrderBookItem(0.0054, 500))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.askPriceAvg5 should be(Some(0.06))
  }

  "The field askPriceAvg5" should "contain the correct result in test9" in {
    val bids = Vector(
      OrderBookItem(0.0046, 1))
    val asks = Vector(
      OrderBookItem(0.0054, 500),
      OrderBookItem(0.0058, 500))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.askPriceAvg5 should be (Some(0.12))
  }

  "The field bidAmountSum5percent" should "contain the correct result in test10" in {
    val bids = Vector(
      OrderBookItem(99, 100),
      OrderBookItem(98, 100),
      OrderBookItem(95, 100),
      OrderBookItem(94.99, 100))
    val asks = Vector(
      OrderBookItem(101, 1))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.bidAmountSum5percent should be (Some(29200))
  }

  "The field bidAmountSum75percent" should "contain the correct result in test11" in {
    val bids = Vector(
      OrderBookItem(0.05, 20),
      OrderBookItem(0.01251, 20),
      OrderBookItem(0.0125, 20))
    val asks = Vector(
      OrderBookItem(0.05, 1))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.bidAmountSum75percent should be (Some(1.5002))
  }

  "The field askAmountSumNC100percent" should "contain the correct result in test12" in {
    val bids = Vector(
      OrderBookItem(0.048315, 1))
    val asks = Vector(
      OrderBookItem(0.049685, 0.06682804),
      OrderBookItem(0.05, 2),
      OrderBookItem(0.098, 1000),
      OrderBookItem(0.0981, 1000))
    val offersOption = None

    val specialCSVTick =
      SpecialCSVTick.fromTick(
        PoloniexDataSaverActor.dataToTick(
          Tick.empty, bids, asks, offersOption).get)

    specialCSVTick.askAmountSumNC100percent should be (Some(1002.06682804))
  }

  "The field askAmountSumNC300percent" should "contain the correct result in test13" in {
    val bids = Vector(
      OrderBookItem(0.051, 1))
    val asks = Vector(
      OrderBookItem(0.049, 100),
      OrderBookItem(0.2, 100))
    val offersOption = None

    val specialCSVTick =
      SpecialCSVTick.fromTick(
        PoloniexDataSaverActor.dataToTick(
          Tick.empty, bids, asks, offersOption).get)

    specialCSVTick.askAmountSumNC300percent should be (Some(200))
  }

  "The field loanOfferRateAvg1" should "contain the correct result in test14" in {
    val bids = Vector(
      OrderBookItem(0.049, 1))
    val asks = Vector(
      OrderBookItem(0.051, 1))
    val offersOption = Some(Vector(
      LoanOrderBookItem(0.005, 10, 1, 1),
      LoanOrderBookItem(0.010, 20, 1, 1)
    ))

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.loanOfferRateAvg1 should be (Some(0.0075))
  }

  "The field loanOfferRateAvg10" should "contain the correct result in test15" in {
    val bids = Vector(
      OrderBookItem(0.049, 1))
    val asks = Vector(
      OrderBookItem(0.051, 1))
    val offersOption = Some(Vector(
      LoanOrderBookItem(0.20, 10, 1, 1),
      LoanOrderBookItem(0.50, 50, 1, 1),
      LoanOrderBookItem(1.00, 1000, 1, 1)
    ))

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.loanOfferRateAvg10 should be (Some(0.835))
  }

  "The field loanOfferRateAvg1" should "contain the correct result in test16" in {
    val bids = Vector(
      OrderBookItem(0.049, 1))
    val asks = Vector(
      OrderBookItem(0.051, 1))
    val offersOption = Some(Vector(
      LoanOrderBookItem(0.0000001, 5, 1, 1),
      LoanOrderBookItem(0.0000002, 5, 1, 1),
      LoanOrderBookItem(0.0000004, 1000, 1, 1)
    ))

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.loanOfferRateAvg1 should be (Some(0.000000275))
  }

  "The field bidAskMidpoint" should "contain the correct result in test17" in {
    val bids = Vector(
      OrderBookItem(0.00000005, 1))
    val asks = Vector(
      OrderBookItem(0.00000006, 1))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.bidAskMidpoint should be (Some(0.000000055))
  }

  "The field bidAskMidpoint" should "contain the correct result in test18" in {
    val bids = Vector(
      OrderBookItem(0.5, 1))
    val asks = Vector(
      OrderBookItem(0.5, 1))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.bidAskMidpoint should be (Some(0.5))
  }

  "The field bidAskMidpoint" should "contain the correct result in test19" in {
    val bids = Vector(
      OrderBookItem(0.51, 1))
    val asks = Vector(
      OrderBookItem(0.49, 1))
    val offersOption = None

    val tick = PoloniexDataSaverActor.dataToTick(
      Tick.empty, bids, asks, offersOption).get

    tick.bidAskMidpoint should be (Some(0.5))
  }
}
