import java.sql.Timestamp

import scala.collection.immutable.{Map, Seq, SortedSet}
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonReader}
import Schema.Tick

case class CSVLine(line: String)

case class TickerJson(last: String, isFrozen: String)

case class ChartDataCandle(timestamp: Long, open: BigDecimal, high: BigDecimal,
                           low: BigDecimal, close: BigDecimal, volume: BigDecimal)

case object ChartDataEmptyCandle

case class ChartDataCandleJson(date: Long, open: BigDecimal, high: BigDecimal,
                               low: BigDecimal, close: BigDecimal, volume: BigDecimal)

case class OrderBook(asks: SortedSet[OrderBookItem], bids: SortedSet[OrderBookItem], seq: Long)

case class OrderBookItem(price: BigDecimal, amount: BigDecimal)

case class OrderBookJson(asks: Seq[Seq[JsValue]], bids: Seq[Seq[JsValue]], seq: Long)

case class LoanOrderBook(offers: SortedSet[LoanOrderBookItem], demands: SortedSet[LoanOrderBookItem])

case class LoanOrderBookItem(rate: BigDecimal, amount: BigDecimal, rangeMin: Int, rangeMax: Int)

case class LoanOrderBookJson(offers: Seq[LoanOrderBookItemJson], demands: Seq[LoanOrderBookItemJson])

case class LoanOrderBookItemJson(rate: String, amount: String, rangeMin: Int, rangeMax: Int)

trait JsonProtocols extends DefaultJsonProtocol {
  implicit val tickerJsonFormat = jsonFormat2(TickerJson.apply)
  implicit val chartdataCandleJsonFormat = jsonFormat6(ChartDataCandleJson.apply)
  implicit val orderBookJsonFormat = jsonFormat3(OrderBookJson.apply)
  implicit val loanOrderBookItemJsonFormat = jsonFormat4(LoanOrderBookItemJson.apply)
  implicit val loanOrderBookJsonFormat = jsonFormat2(LoanOrderBookJson.apply)

  implicit object OrderBookMapRead extends RootJsonReader[Map[String, OrderBook]] {
    def read(value: JsValue) = {
      value.asJsObject.fields map {
        case (c, o) =>
          val orderBook = o.convertTo[OrderBookJson]
          val asks = orderBook.asks.map(i =>
            OrderBookItem(BigDecimal(i(0).convertTo[String]), i(1).convertTo[BigDecimal]))
          val bids = orderBook.bids.map(i =>
            OrderBookItem(BigDecimal(i(0).convertTo[String]), i(1).convertTo[BigDecimal]))
          c -> OrderBook(
            SortedSet(asks: _*)(Ordering.by[OrderBookItem, BigDecimal](_.price)),
            SortedSet(bids: _*)(Ordering.by[OrderBookItem, BigDecimal](_.price).reverse),
            orderBook.seq)
      }
    }
  }

}

case class CSVTick(timestamp: Timestamp, currencyPair: String,
                   open: Option[BigDecimal], high: Option[BigDecimal],
                   low: Option[BigDecimal], close: Option[BigDecimal],
                   volume: Option[BigDecimal], volumeAvgPast7Days: Option[BigDecimal],
                   bidAskMidpoint: Option[BigDecimal],
                   bidPriceAvg1: Option[BigDecimal], bidPriceAvg10: Option[BigDecimal],
                   bidPriceAvg25: Option[BigDecimal], bidPriceAvg50: Option[BigDecimal],
                   bidPriceAvg100: Option[BigDecimal], bidPriceAvg500: Option[BigDecimal],
                   bidPriceAvg1000: Option[BigDecimal], bidPriceAvg2500: Option[BigDecimal],
                   bidPriceAvg5000: Option[BigDecimal], bidPriceAvg10000: Option[BigDecimal],
                   askPriceAvg1: Option[BigDecimal], askPriceAvg10: Option[BigDecimal],
                   askPriceAvg25: Option[BigDecimal], askPriceAvg50: Option[BigDecimal],
                   askPriceAvg100: Option[BigDecimal], askPriceAvg500: Option[BigDecimal],
                   askPriceAvg1000: Option[BigDecimal], askPriceAvg2500: Option[BigDecimal],
                   askPriceAvg5000: Option[BigDecimal], askPriceAvg10000: Option[BigDecimal],
                   bidAmountSum5percent: Option[BigDecimal], bidAmountSum10percent: Option[BigDecimal],
                   bidAmountSum25percent: Option[BigDecimal], bidAmountSum50percent: Option[BigDecimal],
                   bidAmountSum75percent: Option[BigDecimal], bidAmountSum85percent: Option[BigDecimal],
                   bidAmountSum100percent: Option[BigDecimal], askAmountSum5percent: Option[BigDecimal],
                   askAmountSum10percent: Option[BigDecimal], askAmountSum25percent: Option[BigDecimal],
                   askAmountSum50percent: Option[BigDecimal], askAmountSum75percent: Option[BigDecimal],
                   askAmountSum85percent: Option[BigDecimal], askAmountSum100percent: Option[BigDecimal],
                   askAmountSum200percent: Option[BigDecimal],
                   loanOfferRateAvg1: Option[BigDecimal], loanOfferRateAvg10: Option[BigDecimal],
                   loanOfferRateAvg25: Option[BigDecimal], loanOfferRateAvg50: Option[BigDecimal],
                   loanOfferRateAvg100: Option[BigDecimal], loanOfferRateAvg500: Option[BigDecimal],
                   loanOfferRateAvg1000: Option[BigDecimal], loanOfferRateAvg2500: Option[BigDecimal],
                   loanOfferRateAvg5000: Option[BigDecimal], loanOfferRateAvg10000: Option[BigDecimal],
                   loanOfferAmountSum: Option[BigDecimal], loanOfferAmountSumRelToVol: Option[BigDecimal]) {

  def withFractionsAsPercent: CSVTick = this.copy(
    bidPriceAvg1 = bidPriceAvg1.map(100 * _), bidPriceAvg10 = bidPriceAvg10.map(100 * _),
    bidPriceAvg25 = bidPriceAvg25.map(100 * _), bidPriceAvg50 = bidPriceAvg50.map(100 * _),
    bidPriceAvg100 = bidPriceAvg100.map(100 * _), bidPriceAvg500 = bidPriceAvg500.map(100 * _),
    bidPriceAvg1000 = bidPriceAvg1000.map(100 * _), bidPriceAvg2500 = bidPriceAvg2500.map(100 * _),
    bidPriceAvg5000 = bidPriceAvg5000.map(100 * _), bidPriceAvg10000 = bidPriceAvg10000.map(100 * _),
    askPriceAvg1 = askPriceAvg1.map(100 * _), askPriceAvg10 = askPriceAvg10.map(100 * _),
    askPriceAvg25 = askPriceAvg25.map(100 * _), askPriceAvg50 = askPriceAvg50.map(100 * _),
    askPriceAvg100 = askPriceAvg100.map(100 * _), askPriceAvg500 = askPriceAvg500.map(100 * _),
    askPriceAvg1000 = askPriceAvg1000.map(100 * _), askPriceAvg2500 = askPriceAvg2500.map(100 * _),
    askPriceAvg5000 = askPriceAvg5000.map(100 * _), askPriceAvg10000 = askPriceAvg10000.map(100 * _),
    loanOfferRateAvg1 = loanOfferRateAvg1.map(100 * _), loanOfferRateAvg10 = loanOfferRateAvg10.map(100 * _),
    loanOfferRateAvg25 = loanOfferRateAvg25.map(100 * _), loanOfferRateAvg50 = loanOfferRateAvg50.map(100 * _),
    loanOfferRateAvg100 = loanOfferRateAvg100.map(100 * _), loanOfferRateAvg500 = loanOfferRateAvg500.map(100 * _),
    loanOfferRateAvg1000 = loanOfferRateAvg1000.map(100 * _), loanOfferRateAvg2500 = loanOfferRateAvg2500.map(100 * _),
    loanOfferRateAvg5000 = loanOfferRateAvg5000.map(100 * _), loanOfferRateAvg10000 = loanOfferRateAvg10000.map(100 * _))
}

object CSVTick {
  def fromTick(tick: Tick): CSVTick =
    CSVTick(timestamp = tick.timestamp, currencyPair = tick.currencyPair,
      open = tick.open, high = tick.high,
      low = tick.low, close = tick.close,
      volume = tick.volume, volumeAvgPast7Days = None,
      bidAskMidpoint = tick.bidAskMidpoint,
      bidPriceAvg1 = tick.bidPriceAvg1, bidPriceAvg10 = tick.bidPriceAvg10,
      bidPriceAvg25 = tick.bidPriceAvg25, bidPriceAvg50 = tick.bidPriceAvg50,
      bidPriceAvg100 = tick.bidPriceAvg100, bidPriceAvg500 = tick.bidPriceAvg500,
      bidPriceAvg1000 = tick.bidPriceAvg1000, bidPriceAvg2500 = tick.bidPriceAvg2500,
      bidPriceAvg5000 = tick.bidPriceAvg5000, bidPriceAvg10000 = tick.bidPriceAvg10000,
      askPriceAvg1 = tick.askPriceAvg1, askPriceAvg10 = tick.askPriceAvg10,
      askPriceAvg25 = tick.askPriceAvg25, askPriceAvg50 = tick.askPriceAvg50,
      askPriceAvg100 = tick.askPriceAvg100, askPriceAvg500 = tick.askPriceAvg500,
      askPriceAvg1000 = tick.askPriceAvg1000, askPriceAvg2500 = tick.askPriceAvg2500,
      askPriceAvg5000 = tick.askPriceAvg5000, askPriceAvg10000 = tick.askPriceAvg10000,
      bidAmountSum5percent = tick.bidAmountSum5percent, bidAmountSum10percent = tick.bidAmountSum10percent,
      bidAmountSum25percent = tick.bidAmountSum25percent, bidAmountSum50percent = tick.bidAmountSum50percent,
      bidAmountSum75percent = tick.bidAmountSum75percent, bidAmountSum85percent = tick.bidAmountSum85percent,
      bidAmountSum100percent = tick.bidAmountSum100percent, askAmountSum5percent = tick.askAmountSum5percent,
      askAmountSum10percent = tick.askAmountSum10percent, askAmountSum25percent = tick.askAmountSum25percent,
      askAmountSum50percent = tick.askAmountSum50percent, askAmountSum75percent = tick.askAmountSum75percent,
      askAmountSum85percent = tick.askAmountSum85percent, askAmountSum100percent = tick.askAmountSum100percent,
      askAmountSum200percent = tick.askAmountSum200percent,
      loanOfferRateAvg1 = tick.loanOfferRateAvg1, loanOfferRateAvg10 = tick.loanOfferRateAvg10,
      loanOfferRateAvg25 = tick.loanOfferRateAvg25, loanOfferRateAvg50 = tick.loanOfferRateAvg50,
      loanOfferRateAvg100 = tick.loanOfferRateAvg100, loanOfferRateAvg500 = tick.loanOfferRateAvg500,
      loanOfferRateAvg1000 = tick.loanOfferRateAvg1000, loanOfferRateAvg2500 = tick.loanOfferRateAvg2500,
      loanOfferRateAvg5000 = tick.loanOfferRateAvg5000, loanOfferRateAvg10000 = tick.loanOfferRateAvg10000,
      loanOfferAmountSum = tick.loanOfferAmountSum, loanOfferAmountSumRelToVol = None)
}

case class SpecialCSVTick(datum: Timestamp,
                          open: Option[BigDecimal], high: Option[BigDecimal],
                          low: Option[BigDecimal], close: Option[BigDecimal],
                          volume: Option[BigDecimal], volumeAvgPast7Days: Option[BigDecimal],
                          bidAskMidpoint: Option[BigDecimal],
                          bidPriceAvg1: Option[BigDecimal],
                          currencyPair: String, bidPriceAvg10: Option[BigDecimal],
                          bidPriceAvg25: Option[BigDecimal], bidPriceAvg50: Option[BigDecimal],
                          bidPriceAvg100: Option[BigDecimal], bidPriceAvg500: Option[BigDecimal],
                          bidPriceAvg1000: Option[BigDecimal], bidPriceAvg2500: Option[BigDecimal],
                          bidPriceAvg5000: Option[BigDecimal], bidPriceAvg10000: Option[BigDecimal],
                          askPriceAvg1: Option[BigDecimal], askPriceAvg10: Option[BigDecimal],
                          askPriceAvg25: Option[BigDecimal], askPriceAvg50: Option[BigDecimal],
                          askPriceAvg100: Option[BigDecimal], askPriceAvg500: Option[BigDecimal],
                          askPriceAvg1000: Option[BigDecimal], askPriceAvg2500: Option[BigDecimal],
                          askPriceAvg5000: Option[BigDecimal], askPriceAvg10000: Option[BigDecimal],
                          bidAmountSum5percent: Option[BigDecimal], bidAmountSum10percent: Option[BigDecimal],
                          bidAmountSum25percent: Option[BigDecimal], bidAmountSum50percent: Option[BigDecimal],
                          bidAmountSum75percent: Option[BigDecimal], bidAmountSum85percent: Option[BigDecimal],
                          bidAmountSum100percent: Option[BigDecimal], askAmountSum5percent: Option[BigDecimal],
                          askAmountSum10percent: Option[BigDecimal], askAmountSum25percent: Option[BigDecimal],
                          askAmountSum50percent: Option[BigDecimal], askAmountSum75percent: Option[BigDecimal],
                          askAmountSum85percent: Option[BigDecimal], askAmountSum100percent: Option[BigDecimal],
                          askAmountSum200percent: Option[BigDecimal],
                          loanOfferRateAvg1: Option[BigDecimal], loanOfferRateAvg10: Option[BigDecimal],
                          loanOfferRateAvg25: Option[BigDecimal], loanOfferRateAvg50: Option[BigDecimal],
                          loanOfferRateAvg100: Option[BigDecimal], loanOfferRateAvg500: Option[BigDecimal],
                          loanOfferRateAvg1000: Option[BigDecimal], loanOfferRateAvg2500: Option[BigDecimal],
                          loanOfferRateAvg5000: Option[BigDecimal], loanOfferRateAvg10000: Option[BigDecimal],
                          loanOfferAmountSum: Option[BigDecimal], loanOfferAmountSumRelToVol: Option[BigDecimal])

object SpecialCSVTick {
  def fromTick(csvTick: CSVTick): SpecialCSVTick =
    SpecialCSVTick(datum = csvTick.timestamp, currencyPair = csvTick.currencyPair,
      open = csvTick.open, high = csvTick.high,
      low = csvTick.low, close = csvTick.close,
      volume = csvTick.volume, volumeAvgPast7Days = None,
      bidAskMidpoint = csvTick.bidAskMidpoint,
      bidPriceAvg1 = csvTick.bidPriceAvg1, bidPriceAvg10 = csvTick.bidPriceAvg10,
      bidPriceAvg25 = csvTick.bidPriceAvg25, bidPriceAvg50 = csvTick.bidPriceAvg50,
      bidPriceAvg100 = csvTick.bidPriceAvg100, bidPriceAvg500 = csvTick.bidPriceAvg500,
      bidPriceAvg1000 = csvTick.bidPriceAvg1000, bidPriceAvg2500 = csvTick.bidPriceAvg2500,
      bidPriceAvg5000 = csvTick.bidPriceAvg5000, bidPriceAvg10000 = csvTick.bidPriceAvg10000,
      askPriceAvg1 = csvTick.askPriceAvg1, askPriceAvg10 = csvTick.askPriceAvg10,
      askPriceAvg25 = csvTick.askPriceAvg25, askPriceAvg50 = csvTick.askPriceAvg50,
      askPriceAvg100 = csvTick.askPriceAvg100, askPriceAvg500 = csvTick.askPriceAvg500,
      askPriceAvg1000 = csvTick.askPriceAvg1000, askPriceAvg2500 = csvTick.askPriceAvg2500,
      askPriceAvg5000 = csvTick.askPriceAvg5000, askPriceAvg10000 = csvTick.askPriceAvg10000,
      bidAmountSum5percent = csvTick.bidAmountSum5percent, bidAmountSum10percent = csvTick.bidAmountSum10percent,
      bidAmountSum25percent = csvTick.bidAmountSum25percent, bidAmountSum50percent = csvTick.bidAmountSum50percent,
      bidAmountSum75percent = csvTick.bidAmountSum75percent, bidAmountSum85percent = csvTick.bidAmountSum85percent,
      bidAmountSum100percent = csvTick.bidAmountSum100percent, askAmountSum5percent = csvTick.askAmountSum5percent,
      askAmountSum10percent = csvTick.askAmountSum10percent, askAmountSum25percent = csvTick.askAmountSum25percent,
      askAmountSum50percent = csvTick.askAmountSum50percent, askAmountSum75percent = csvTick.askAmountSum75percent,
      askAmountSum85percent = csvTick.askAmountSum85percent, askAmountSum100percent = csvTick.askAmountSum100percent,
      askAmountSum200percent = csvTick.askAmountSum200percent,
      loanOfferRateAvg1 = csvTick.loanOfferRateAvg1, loanOfferRateAvg10 = csvTick.loanOfferRateAvg10,
      loanOfferRateAvg25 = csvTick.loanOfferRateAvg25, loanOfferRateAvg50 = csvTick.loanOfferRateAvg50,
      loanOfferRateAvg100 = csvTick.loanOfferRateAvg100, loanOfferRateAvg500 = csvTick.loanOfferRateAvg500,
      loanOfferRateAvg1000 = csvTick.loanOfferRateAvg1000, loanOfferRateAvg2500 = csvTick.loanOfferRateAvg2500,
      loanOfferRateAvg5000 = csvTick.loanOfferRateAvg5000, loanOfferRateAvg10000 = csvTick.loanOfferRateAvg10000,
      loanOfferAmountSum = csvTick.loanOfferAmountSum, loanOfferAmountSumRelToVol = None)
}
