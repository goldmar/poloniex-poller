import java.sql.Timestamp
import java.time.Instant

import scala.collection.immutable.{Map, Seq, SortedSet}
import slick.jdbc.MySQLProfile.api._
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonReader}
import Schema.Tick
import shapeless.Generic
import slick.jdbc.GetResult

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

case class CSVTick(id: Long, timestamp: Timestamp, currencyPair: String,
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
  implicit val getCSVTickResult = GetResult(r => CSVTick(
    r.nextLong, r.nextTimestamp(), r.nextString, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption,
    r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption,
    r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption,
    r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption,
    r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption,
    r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption,
    r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption,
    r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption,
    r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption,
    r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption,
    r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption, r.nextBigDecimalOption,
    r.nextBigDecimalOption)
  )

  def getAllTicks(limit: Int, offset: Int) =
    sql"""select tick.id, tick.timestamp, tick.currency_pair, tick.open, tick.high, tick.low, tick.close, tick.volume, avg(past.volume),
                 tick.bid_ask_midpoint, tick.bid_price_avg_1, tick.bid_price_avg_10, tick.bid_price_avg_25, tick.bid_price_avg_50,
                 tick.bid_price_avg_100, tick.bid_price_avg_500, tick.bid_price_avg_1000, tick.bid_price_avg_2500, tick.bid_price_avg_5000,
                 tick.bid_price_avg_10000, tick.ask_price_avg_1, tick.ask_price_avg_10, tick.ask_price_avg_25, tick.ask_price_avg_50, tick.ask_price_avg_100,
                 tick.ask_price_avg_500, tick.ask_price_avg_1000, tick.ask_price_avg_2500, tick.ask_price_avg_5000, tick.ask_price_avg_10000,
                 tick.bid_amount_sum_5percent, tick.bid_amount_sum_10percent, tick.bid_amount_sum_25percent, tick.bid_amount_sum_50percent,
                 tick.bid_amount_sum_75percent, tick.bid_amount_sum_85percent, tick.bid_amount_sum_100percent,
                 tick.ask_amount_sum_5percent, tick.ask_amount_sum_10percent, tick.ask_amount_sum_25percent, tick.ask_amount_sum_50percent,
                 tick.ask_amount_sum_75percent, tick.ask_amount_sum_85percent, tick.ask_amount_sum_100percent, tick.ask_amount_sum_200percent,
                 tick.loan_offer_rate_avg_1, tick.loan_offer_rate_avg_10, tick.loan_offer_rate_avg_25, tick.loan_offer_rate_avg_50, tick.loan_offer_rate_avg_100,
                 tick.loan_offer_rate_avg_500, tick.loan_offer_rate_avg_1000, tick.loan_offer_rate_avg_2500, tick.loan_offer_rate_avg_5000,
                 tick.loan_offer_rate_avg_10000, tick.loan_offer_amount_sum, tick.loan_offer_amount_sum / (288 * avg(past.volume))
          from ticks tick, ticks past
          where tick.chart_data_final and past.chart_data_final and
                past.timestamp > date_sub(tick.timestamp, interval 7 day) and
                past.timestamp <= tick.timestamp and past.currency_pair = tick.currency_pair
          group by tick.timestamp, tick.currency_pair
          order by tick.timestamp asc, tick.currency_pair asc
          limit $limit offset $offset""".as[CSVTick]

  def getTicksForCurrency(currencyPair: String)(limit: Int, offset: Int) =
    sql"""select tick.id, tick.timestamp, tick.currency_pair, tick.open, tick.high, tick.low, tick.close, tick.volume, avg(past.volume),
                 tick.bid_ask_midpoint, tick.bid_price_avg_1, tick.bid_price_avg_10, tick.bid_price_avg_25, tick.bid_price_avg_50,
                 tick.bid_price_avg_100, tick.bid_price_avg_500, tick.bid_price_avg_1000, tick.bid_price_avg_2500, tick.bid_price_avg_5000,
                 tick.bid_price_avg_10000, tick.ask_price_avg_1, tick.ask_price_avg_10, tick.ask_price_avg_25, tick.ask_price_avg_50, tick.ask_price_avg_100,
                 tick.ask_price_avg_500, tick.ask_price_avg_1000, tick.ask_price_avg_2500, tick.ask_price_avg_5000, tick.ask_price_avg_10000,
                 tick.bid_amount_sum_5percent, tick.bid_amount_sum_10percent, tick.bid_amount_sum_25percent, tick.bid_amount_sum_50percent,
                 tick.bid_amount_sum_75percent, tick.bid_amount_sum_85percent, tick.bid_amount_sum_100percent,
                 tick.ask_amount_sum_5percent, tick.ask_amount_sum_10percent, tick.ask_amount_sum_25percent, tick.ask_amount_sum_50percent,
                 tick.ask_amount_sum_75percent, tick.ask_amount_sum_85percent, tick.ask_amount_sum_100percent, tick.ask_amount_sum_200percent,
                 tick.loan_offer_rate_avg_1, tick.loan_offer_rate_avg_10, tick.loan_offer_rate_avg_25, tick.loan_offer_rate_avg_50, tick.loan_offer_rate_avg_100,
                 tick.loan_offer_rate_avg_500, tick.loan_offer_rate_avg_1000, tick.loan_offer_rate_avg_2500, tick.loan_offer_rate_avg_5000,
                 tick.loan_offer_rate_avg_10000, tick.loan_offer_amount_sum, tick.loan_offer_amount_sum / (288 * avg(past.volume))
          from ticks tick, ticks past
          where tick.currency_pair = $currencyPair and tick.chart_data_final and past.chart_data_final and
                past.timestamp > date_sub(tick.timestamp, interval 7 day) and
                past.timestamp <= tick.timestamp and past.currency_pair = tick.currency_pair
          group by tick.timestamp, tick.currency_pair
          order by tick.timestamp asc, tick.currency_pair asc
          limit $limit offset $offset""".as[CSVTick]
}
