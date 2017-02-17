import java.sql.Timestamp

import scala.collection.immutable.{Map, SortedSet}
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonReader}
import Schema.Tick

case class CSVLine(line: String)

case class TickerJson(last: String, isFrozen: String)

case class ChartData(timestamp: Long, open: BigDecimal, high: BigDecimal,
                     low: BigDecimal, close: BigDecimal, volume: BigDecimal)

case class ChartDataJson(date: Long, open: BigDecimal, high: BigDecimal,
                         low: BigDecimal, close: BigDecimal, volume: BigDecimal)

case class OrderBook(asks: SortedSet[OrderBookItem], bids: SortedSet[OrderBookItem], seq: Long)

case class OrderBookItem(price: BigDecimal, amount: BigDecimal)

case class OrderBookJson(asks: Vector[Vector[JsValue]], bids: Vector[Vector[JsValue]], seq: Long)

case class LoanOrderBook(offers: SortedSet[LoanOrderBookItem], demands: SortedSet[LoanOrderBookItem])

case class LoanOrderBookItem(rate: BigDecimal, amount: BigDecimal, rangeMin: Int, rangeMax: Int)

case class LoanOrderBookJson(offers: Vector[LoanOrderBookItemJson], demands: Vector[LoanOrderBookItemJson])

case class LoanOrderBookItemJson(rate: String, amount: String, rangeMin: Int, rangeMax: Int)

object JsonProtocols extends DefaultJsonProtocol {
  implicit val tickerJsonFormat = jsonFormat2(TickerJson.apply)
  implicit val chartdataCandleJsonFormat = jsonFormat6(ChartDataJson.apply)
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

case class SpecialCSVTick(datum: Timestamp,
                          open: Option[BigDecimal], high: Option[BigDecimal],
                          low: Option[BigDecimal], close: Option[BigDecimal],
                          volume: Option[BigDecimal], bidAskMidpoint: Option[BigDecimal],
                          bidPriceAvg1: Option[BigDecimal], currencyPair: String,
                          bidPriceAvg5: Option[BigDecimal], bidPriceAvg10: Option[BigDecimal],
                          bidPriceAvg25: Option[BigDecimal], bidPriceAvg50: Option[BigDecimal],
                          bidPriceAvg100: Option[BigDecimal], bidPriceAvg500: Option[BigDecimal],
                          bidPriceAvg1000: Option[BigDecimal], bidPriceAvg2500: Option[BigDecimal],
                          bidPriceAvg5000: Option[BigDecimal], bidPriceAvg10000: Option[BigDecimal],
                          askPriceAvg1: Option[BigDecimal], askPriceAvg5: Option[BigDecimal],
                          askPriceAvg10: Option[BigDecimal], askPriceAvg25: Option[BigDecimal],
                          askPriceAvg50: Option[BigDecimal], askPriceAvg100: Option[BigDecimal],
                          askPriceAvg500: Option[BigDecimal], askPriceAvg1000: Option[BigDecimal],
                          askPriceAvg2500: Option[BigDecimal], askPriceAvg5000: Option[BigDecimal],
                          askPriceAvg10000: Option[BigDecimal],
                          bidAmountSum5percent: Option[BigDecimal], bidAmountSum10percent: Option[BigDecimal],
                          bidAmountSum25percent: Option[BigDecimal], bidAmountSum50percent: Option[BigDecimal],
                          bidAmountSum75percent: Option[BigDecimal], bidAmountSum85percent: Option[BigDecimal],
                          bidAmountSum100percent: Option[BigDecimal], askAmountSum5percent: Option[BigDecimal],
                          askAmountSum10percent: Option[BigDecimal], askAmountSum25percent: Option[BigDecimal],
                          askAmountSum50percent: Option[BigDecimal], askAmountSum75percent: Option[BigDecimal],
                          askAmountSum85percent: Option[BigDecimal], askAmountSum100percent: Option[BigDecimal],
                          askAmountSum200percent: Option[BigDecimal], askAmountSum300percent: Option[BigDecimal],
                          askAmountSum900percent: Option[BigDecimal], askAmountSumAll: Option[BigDecimal],
                          loanOfferRateAvg1: Option[BigDecimal], loanOfferRateAvg5: Option[BigDecimal],
                          loanOfferRateAvg10: Option[BigDecimal], loanOfferRateAvg25: Option[BigDecimal],
                          loanOfferRateAvg50: Option[BigDecimal], loanOfferRateAvg100: Option[BigDecimal],
                          loanOfferRateAvg500: Option[BigDecimal], loanOfferRateAvg1000: Option[BigDecimal],
                          loanOfferRateAvg2500: Option[BigDecimal], loanOfferRateAvg5000: Option[BigDecimal],
                          loanOfferRateAvg10000: Option[BigDecimal], loanOfferRateAvgAll: Option[BigDecimal],
                          loanOfferAmountSum: Option[BigDecimal],
                          bidPriceAvgAbs5: Option[BigDecimal], bidPriceAvgAbs10: Option[BigDecimal],
                          bidPriceAvgAbs25: Option[BigDecimal],
                          askPriceAvgAbs5: Option[BigDecimal], askPriceAvgAbs10: Option[BigDecimal],
                          askPriceAvgAbs25: Option[BigDecimal],
                          askAmountSumNC50percent: Option[BigDecimal], askAmountSumNC100percent: Option[BigDecimal],
                          askAmountSumNC200percent: Option[BigDecimal], askAmountSumNC300percent: Option[BigDecimal],
                          askAmountSumNC900percent: Option[BigDecimal], askAmountSumNCAll: Option[BigDecimal])

object SpecialCSVTick {
  def fromTick(tick: Tick): SpecialCSVTick = {
    SpecialCSVTick(
      datum = tick.timestamp, currencyPair = tick.currencyPair,
      open = tick.open.map(_ * 1000), high = tick.high.map(_ * 1000),
      low = tick.low.map(_ * 1000), close = tick.close.map(_ * 1000),
      volume = tick.volume, bidAskMidpoint = tick.bidAskMidpoint.map(_ * 1000),
      bidPriceAvg1 = tick.bidPriceAvg1.map(_ * 100), bidPriceAvg5 = tick.bidPriceAvg5.map(_ * 100),
      bidPriceAvg10 = tick.bidPriceAvg10.map(_ * 100), bidPriceAvg25 = tick.bidPriceAvg25.map(_ * 100),
      bidPriceAvg50 = tick.bidPriceAvg50.map(_ * 100), bidPriceAvg100 = tick.bidPriceAvg100.map(_ * 100),
      bidPriceAvg500 = tick.bidPriceAvg500.map(_ * 100), bidPriceAvg1000 = tick.bidPriceAvg1000.map(_ * 100),
      bidPriceAvg2500 = tick.bidPriceAvg2500.map(_ * 100), bidPriceAvg5000 = tick.bidPriceAvg5000.map(_ * 100),
      bidPriceAvg10000 = tick.bidPriceAvg10000.map(_ * 100),
      askPriceAvg1 = tick.askPriceAvg1.map(_ * 100), askPriceAvg5 = tick.askPriceAvg5.map(_ * 100),
      askPriceAvg10 = tick.askPriceAvg10.map(_ * 100), askPriceAvg25 = tick.askPriceAvg25.map(_ * 100),
      askPriceAvg50 = tick.askPriceAvg50.map(_ * 100), askPriceAvg100 = tick.askPriceAvg100.map(_ * 100),
      askPriceAvg500 = tick.askPriceAvg500.map(_ * 100), askPriceAvg1000 = tick.askPriceAvg1000.map(_ * 100),
      askPriceAvg2500 = tick.askPriceAvg2500.map(_ * 100), askPriceAvg5000 = tick.askPriceAvg5000.map(_ * 100),
      askPriceAvg10000 = tick.askPriceAvg10000.map(_ * 100),
      bidAmountSum5percent = tick.bidAmountSum5percent, bidAmountSum10percent = tick.bidAmountSum10percent,
      bidAmountSum25percent = tick.bidAmountSum25percent, bidAmountSum50percent = tick.bidAmountSum50percent,
      bidAmountSum75percent = tick.bidAmountSum75percent, bidAmountSum85percent = tick.bidAmountSum85percent,
      bidAmountSum100percent = tick.bidAmountSum100percent, askAmountSum5percent = tick.askAmountSum5percent,
      askAmountSum10percent = tick.askAmountSum10percent, askAmountSum25percent = tick.askAmountSum25percent,
      askAmountSum50percent = tick.askAmountSum50percent, askAmountSum75percent = tick.askAmountSum75percent,
      askAmountSum85percent = tick.askAmountSum85percent, askAmountSum100percent = tick.askAmountSum100percent,
      askAmountSum200percent = tick.askAmountSum200percent, askAmountSum300percent = tick.askAmountSum300percent,
      askAmountSum900percent = tick.askAmountSum900percent, askAmountSumAll = tick.askAmountSumAll,
      loanOfferRateAvg1 = tick.loanOfferRateAvg1.map(_ * 100),
      loanOfferRateAvg5 = tick.loanOfferRateAvg5.map(_ * 100),
      loanOfferRateAvg10 = tick.loanOfferRateAvg10.map(_ * 100),
      loanOfferRateAvg25 = tick.loanOfferRateAvg25.map(_ * 100),
      loanOfferRateAvg50 = tick.loanOfferRateAvg50.map(_ * 100),
      loanOfferRateAvg100 = tick.loanOfferRateAvg100.map(_ * 100),
      loanOfferRateAvg500 = tick.loanOfferRateAvg500.map(_ * 100),
      loanOfferRateAvg1000 = tick.loanOfferRateAvg1000.map(_ * 100),
      loanOfferRateAvg2500 = tick.loanOfferRateAvg2500.map(_ * 100),
      loanOfferRateAvg5000 = tick.loanOfferRateAvg5000.map(_ * 100),
      loanOfferRateAvg10000 = tick.loanOfferRateAvg10000.map(_ * 100),
      loanOfferRateAvgAll = tick.loanOfferRateAvgAll.map(_ * 100),
      loanOfferAmountSum = tick.loanOfferAmountSum,
      bidPriceAvgAbs5 = for {
        bam <- tick.bidAskMidpoint
        bpa <- tick.bidPriceAvg5
      } yield bam * (1 + bpa) * 1000,
      bidPriceAvgAbs10 = for {
        bam <- tick.bidAskMidpoint
        bpa <- tick.bidPriceAvg10
      } yield bam * (1 + bpa) * 1000,
      bidPriceAvgAbs25 = for {
        bam <- tick.bidAskMidpoint
        bpa <- tick.bidPriceAvg25
      } yield bam * (1 + bpa) * 1000,
      askPriceAvgAbs5 = for {
        bam <- tick.bidAskMidpoint
        apa <- tick.askPriceAvg5
      } yield bam * (1 + apa) * 1000,
      askPriceAvgAbs10 = for {
        bam <- tick.bidAskMidpoint
        apa <- tick.askPriceAvg10
      } yield bam * (1 + apa) * 1000,
      askPriceAvgAbs25 = for {
        bam <- tick.bidAskMidpoint
        apa <- tick.askPriceAvg25
      } yield bam * (1 + apa) * 1000,
      askAmountSumNC50percent = for {
        bam <- tick.bidAskMidpoint
        aas <- tick.askAmountSum50percent
      } yield aas / bam,
      askAmountSumNC100percent = for {
        bam <- tick.bidAskMidpoint
        aas <- tick.askAmountSum100percent
      } yield aas / bam,
      askAmountSumNC200percent = for {
        bam <- tick.bidAskMidpoint
        aas <- tick.askAmountSum200percent
      } yield aas / bam,
        askAmountSumNC300percent = for {
        bam <- tick.bidAskMidpoint
        aas <- tick.askAmountSum300percent
      } yield aas / bam,
        askAmountSumNC900percent = for {
        bam <- tick.bidAskMidpoint
        aas <- tick.askAmountSum900percent
      } yield aas / bam,
        askAmountSumNCAll = for {
        bam <- tick.bidAskMidpoint
        aas <- tick.askAmountSumAll
      } yield aas / bam)
  }
}
