import java.sql.Timestamp
import java.time._
import java.time.temporal.{ChronoField, ChronoUnit}

import scala.language.postfixOps
import scala.concurrent._
import scala.concurrent.duration._

import slick.jdbc.MySQLProfile.api._
import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.stream._
import akka.util.Timeout
import sext._
import Schema._

case class RequestUpdateOldCandles(until: Long)

case object RequestScheduledUpdateOldCandles

case class RequestInsertOldCandles(from: Option[Long], until: Long)

class PoloniexDataSaverActor extends Actor with ActorLogging {
  implicit val system = context.system
  implicit val dispatcher = context.dispatcher

  val decider: Supervision.Decider = { e =>
    log.error("Unhandled exception in stream", e)
    Supervision.Stop
  }

  val materializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  implicit val materializer = ActorMaterializer(materializerSettings)

  implicit val timeout = Timeout(1 minute)

  val poller = system.actorOf(Props[PoloniexPollerActor])

  def aggregateItems(seq: Seq[(BigDecimal, BigDecimal)], depth: BigDecimal): Option[BigDecimal] = {
    val size = seq.size
    var acc: BigDecimal = 0
    var remaining = depth
    var i = 0
    while (remaining > 0 && i < size) {
      val amount = remaining min seq(i)._2
      acc += amount * seq(i)._1 / depth
      remaining -= amount
      i += 1
    }

    remaining match {
      case r if r == 0 => Some(acc)
      case _ => None
    }
  }

  def aggregateLoanOffers(c: String, los: Map[String, LoanOrderBook], btcPrice: BigDecimal, depth: BigDecimal): Option[BigDecimal] = {
    los.get(c).flatMap(lo => aggregateItems(lo.offers.toSeq.map(i => i.rate -> i.amount * btcPrice), depth))
  }

  override def receive = {
    case Poll =>
      poller ! Poll

    case UpdateCurrencyList =>
      poller ! UpdateCurrencyList

    case InsertCandles(timestamp, ts, obs, los) =>
      val inserts = for (c <- ts.keys) yield {
        val btcPrice = ts(c)
        val bids: Seq[OrderBookItem] = obs(c).bids.toSeq
        val asks: Seq[OrderBookItem] = obs(c).asks.toSeq
        val bidAskMidpoint = (for {
          bid <- bids.headOption
          ask <- asks.headOption
        } yield (bid.price + ask.price) / 2).getOrElse(btcPrice)

        Tick(
          id = -1,
          timestamp = Timestamp.from(Instant.ofEpochSecond(timestamp)),
          currencyPair = c,
          open = None,
          high = None,
          low = None,
          close = None,
          volume = None,
          chartDataFinal = false,
          bidAskMidpoint = Some(bidAskMidpoint),
          bidPriceAvg1 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 1).map(_ / bidAskMidpoint - 1),
          bidPriceAvg10 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 10).map(_ / bidAskMidpoint - 1),
          bidPriceAvg25 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 25).map(_ / bidAskMidpoint - 1),
          bidPriceAvg50 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 50).map(_ / bidAskMidpoint - 1),
          bidPriceAvg100 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 100).map(_ / bidAskMidpoint - 1),
          bidPriceAvg500 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 500).map(_ / bidAskMidpoint - 1),
          bidPriceAvg1000 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 1000).map(_ / bidAskMidpoint - 1),
          bidPriceAvg2500 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 2500).map(_ / bidAskMidpoint - 1),
          bidPriceAvg5000 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 5000).map(_ / bidAskMidpoint - 1),
          bidPriceAvg10000 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 10000).map(_ / bidAskMidpoint - 1),
          askPriceAvg1 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 1).map(_ / bidAskMidpoint - 1),
          askPriceAvg10 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 10).map(_ / bidAskMidpoint - 1),
          askPriceAvg25 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 25).map(_ / bidAskMidpoint - 1),
          askPriceAvg50 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 50).map(_ / bidAskMidpoint - 1),
          askPriceAvg100 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 100).map(_ / bidAskMidpoint - 1),
          askPriceAvg500 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 500).map(_ / bidAskMidpoint - 1),
          askPriceAvg1000 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 1000).map(_ / bidAskMidpoint - 1),
          askPriceAvg2500 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 2500).map(_ / bidAskMidpoint - 1),
          askPriceAvg5000 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 5000).map(_ / bidAskMidpoint - 1),
          askPriceAvg10000 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 10000).map(_ / bidAskMidpoint - 1),
          bidAmountSum5percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.95).map(b => b.price * b.amount).sum),
          bidAmountSum10percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.90).map(b => b.price * b.amount).sum),
          bidAmountSum25percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.75).map(b => b.price * b.amount).sum),
          bidAmountSum50percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.50).map(b => b.price * b.amount).sum),
          bidAmountSum75percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.25).map(b => b.price * b.amount).sum),
          bidAmountSum85percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.15).map(b => b.price * b.amount).sum),
          bidAmountSum100percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.00).map(b => b.price * b.amount).sum),
          askAmountSum5percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.05).map(_.amount).sum * bidAskMidpoint),
          askAmountSum10percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.10).map(_.amount).sum * bidAskMidpoint),
          askAmountSum25percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.25).map(_.amount).sum * bidAskMidpoint),
          askAmountSum50percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.50).map(_.amount).sum * bidAskMidpoint),
          askAmountSum75percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.75).map(_.amount).sum * bidAskMidpoint),
          askAmountSum85percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.85).map(_.amount).sum * bidAskMidpoint),
          askAmountSum100percent = Some(asks.filter(_.price <= bidAskMidpoint * 2.00).map(_.amount).sum * bidAskMidpoint),
          askAmountSum200percent = Some(asks.filter(_.price <= bidAskMidpoint * 3.00).map(_.amount).sum * bidAskMidpoint),
          loanOfferRateAvg1 = aggregateLoanOffers(c, los, bidAskMidpoint, 1),
          loanOfferRateAvg10 = aggregateLoanOffers(c, los, bidAskMidpoint, 10),
          loanOfferRateAvg25 = aggregateLoanOffers(c, los, bidAskMidpoint, 25),
          loanOfferRateAvg50 = aggregateLoanOffers(c, los, bidAskMidpoint, 50),
          loanOfferRateAvg100 = aggregateLoanOffers(c, los, bidAskMidpoint, 100),
          loanOfferRateAvg500 = aggregateLoanOffers(c, los, bidAskMidpoint, 500),
          loanOfferRateAvg1000 = aggregateLoanOffers(c, los, bidAskMidpoint, 1000),
          loanOfferRateAvg2500 = aggregateLoanOffers(c, los, bidAskMidpoint, 2500),
          loanOfferRateAvg5000 = aggregateLoanOffers(c, los, bidAskMidpoint, 5000),
          loanOfferRateAvg10000 = aggregateLoanOffers(c, los, bidAskMidpoint, 10000),
          loanOfferRateAvgAll = los.get(c).map(_.offers.foldLeft((BigDecimal(0), BigDecimal(0))) {
            case ((weightedRateSum, amountSum), next) =>
              weightedRateSum + next.amount * next.rate -> (amountSum + next.amount)
          }).collect { case (weightedRateSum, amountSum) if amountSum > 0 => weightedRateSum / amountSum },
          loanOfferAmountSum = los.get(c).map(_.offers.map(_.amount).sum).map(_ * bidAskMidpoint)
        )
      }

      val result = DB.get.run(DBIO.seq(
        ticks ++= inserts
      ))

      result onSuccess { case _ =>
        log.info(s"Inserted new data at timestamp $timestamp")
      }

      result onFailure { case e =>
        log.error(e, s"Could not insert new data at timestamp $timestamp")
      }

    case UpsertCandleChartData(timestamp, cds) =>
      val instant = Instant.ofEpochSecond(timestamp)
      val sqlTimestamp = Timestamp.from(instant)
      val sqlTimstamp5MinAgo = Timestamp.from(instant.minus(5, ChronoUnit.MINUTES))

      val updatesFuture = Future.sequence(cds.map { case (c, cdcOption) =>
        (cdcOption match {
          case Some(candle) =>
            Future(Some(candle))
          case None =>
            DB.get.run(
              ticks.filter(t =>
                t.timestamp === sqlTimstamp5MinAgo && t.currencyPair === c &&
                  t.chartDataFinal === true).result.headOption
            ).map(tickOption =>
              tickOption.flatMap(tick =>
                tick.close.map(previousClose =>
                  ChartDataCandle(timestamp, previousClose, previousClose, previousClose, previousClose, 0))))
        }).map { cdcOption =>
          for {
            rowsAffected <- ticks
              .filter(t => t.timestamp === sqlTimestamp && t.currencyPair === c && t.chartDataFinal === false)
              .map(t =>
                (t.open, t.high, t.low, t.close, t.volume, t.chartDataFinal))
              .update(
                cdcOption.map(_.open), cdcOption.map(_.high), cdcOption.map(_.low),
                cdcOption.map(_.close), cdcOption.map(_.volume), cdcOption.map(_ => true).getOrElse(false))
            result <- rowsAffected match {
              case 0 => ticks += Tick.empty().copy(
                timestamp = sqlTimestamp,
                currencyPair = c,
                open = cdcOption.map(_.open),
                high = cdcOption.map(_.high),
                low = cdcOption.map(_.low),
                close = cdcOption.map(_.close),
                volume = cdcOption.map(_.volume),
                chartDataFinal = cdcOption.map(_ => true).getOrElse(false))
              case 1 => DBIO.successful(1)
              case n => DBIO.failed(new RuntimeException(
                s"Expected 0 or 1 change, not $n at timestamp $timestamp for currency pair $c"))
            }
          } yield result
        }
      })

      val result = updatesFuture.map(updates =>
        DB.get.run(DBIO.seq(updates.toSeq: _*)))

      result onSuccess { case _ =>
        log.info(s"Upserted candles at timestamp $timestamp")
      }

      result onFailure { case e =>
        log.error(e, s"Could not upsert candles at $timestamp")
      }

    case RequestScheduledUpdateOldCandles =>
      self ! RequestUpdateOldCandles(
        Instant.now.minus(Config.updateDelay, ChronoUnit.MINUTES).getEpochSecond)

    case RequestUpdateOldCandles(until) =>
      val untilSqlTimestamp = Timestamp.from(Instant.ofEpochSecond(until))
      val query = ticks
        .filter(t => t.timestamp <= untilSqlTimestamp && t.chartDataFinal === false)
        .sortBy(_.timestamp.asc)
        .map(_.timestamp)
        .take(1)

      DB.get.run(query.result.headOption).map {
        case Some(timestamp) =>
          val start = timestamp.toInstant.getEpochSecond
          val end = until

          log.info(s"Updating old candles since $start")

          (poller ? FetchOldChartData(start, end)).mapTo[OldCandleChartData] onSuccess { case OldCandleChartData(cds) =>
            val query = ticks
              .filter(t => t.timestamp <= untilSqlTimestamp && t.chartDataFinal === false)
              .sortBy(_.timestamp.asc)
              .map(t => (t.timestamp, t.currencyPair))

            DB.get.run(query.result).foreach { r =>
              val tuples = r.map { case (sqlTimestamp, c) =>
                val timestamp = sqlTimestamp.toInstant.getEpochSecond
                cds.get(timestamp).flatMap(_.get(c)) match {
                  case Some(candle) =>
                    (timestamp, c, Some(candle))
                  case None =>
                    (timestamp, c, None)
                }
              }

              val candles = tuples.groupBy(_._1).map { case (t, subTuple) =>
                t -> subTuple.groupBy(_._2).map { case (c, subSubTuple) =>
                  c -> subSubTuple.head._3
                }
              }

              for (t <- candles.keys.toSeq.sorted) {
                self ! UpsertCandleChartData(t, candles(t))
              }
            }
          }

        case None =>
          log.info("All candles are up-to-date")
      }

    case RequestInsertOldCandles(fromOption, until) =>
      val fromFuture = fromOption match {
        case Some(from) =>
          Future(from)

        case None =>
          val query = ticks
            .sortBy(_.timestamp.desc)
            .map(_.timestamp)
            .take(1)

          DB.get.run(query.result.head).map(timestamp =>
            timestamp.toInstant.plus(5, ChronoUnit.MINUTES).getEpochSecond)
      }

      fromFuture.foreach { from =>
        val allCurrencies = (poller ? ListAllCurrencies).mapTo[Seq[String]]
        val oldChartData = (poller ? FetchOldChartData(from, until)).mapTo[OldCandleChartData]
        for {
          allCs <- allCurrencies
          ocd <- oldChartData
        } yield {
          val cds = ocd.cds
          for (t <- cds.keys.toSeq.sorted if t >= from && t <= until) {
            self ! UpsertCandleChartData(t, allCs.map(c => c -> cds(t).get(c)).toMap)
          }
        }
      }
  }
}
