import java.sql.Timestamp
import java.time._
import java.time.temporal.{ChronoField, ChronoUnit}

import scala.language.postfixOps
import scala.concurrent._
import scala.concurrent.duration._

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.stream._
import akka.util.Timeout
import sext._
import Schema._
import DB.config.profile.api._

case class RequestUpdateOldChartData(until: Long)

case object RequestScheduledUpdateOldChartData

case class RequestInsertOldChartData(from: Option[Long], until: Long)

class PoloniexDataSaverActor extends Actor with ActorLogging {
  implicit val system = context.system
  implicit val dispatcher = context.dispatcher

  val decider: Supervision.Decider = { e =>
    log.error("Unhandled exception in stream", e)
    Supervision.Stop
  }

  val materializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  implicit val materializer = ActorMaterializer(materializerSettings)

  implicit val timeout = Timeout(5 minute)

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

    if (remaining == 0) Some(acc)
    else None
  }

  def aggregateLoanOffers(c: String, lobs: Map[String, LoanOrderBook], btcPrice: BigDecimal, depth: BigDecimal): Option[BigDecimal] = {
    lobs.get(c).flatMap(lob => aggregateItems(lob.offers.toSeq.map(i => i.rate -> i.amount * btcPrice), depth))
  }

  override def receive = {
    case Poll =>
      poller ! UpdateCurrencyList
      poller ! Poll

    case InsertData(timestamp, ts, obs, lobs) =>
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
          chartDataFinal = bidAskMidpoint == 0, // cope with new currencies
          bidAskMidpoint = Option(bidAskMidpoint).filter(_ > 0),
          bidPriceAvg1 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 1).map(_ / bidAskMidpoint - 1),
          bidPriceAvg5 = aggregateItems(bids.map(i => i.price -> i.amount * bidAskMidpoint), 5).map(_ / bidAskMidpoint - 1),
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
          askPriceAvg5 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 5).map(_ / bidAskMidpoint - 1),
          askPriceAvg10 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 10).map(_ / bidAskMidpoint - 1),
          askPriceAvg25 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 25).map(_ / bidAskMidpoint - 1),
          askPriceAvg50 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 50).map(_ / bidAskMidpoint - 1),
          askPriceAvg100 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 100).map(_ / bidAskMidpoint - 1),
          askPriceAvg500 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 500).map(_ / bidAskMidpoint - 1),
          askPriceAvg1000 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 1000).map(_ / bidAskMidpoint - 1),
          askPriceAvg2500 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 2500).map(_ / bidAskMidpoint - 1),
          askPriceAvg5000 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 5000).map(_ / bidAskMidpoint - 1),
          askPriceAvg10000 = aggregateItems(asks.map(i => i.price -> i.amount * bidAskMidpoint), 10000).map(_ / bidAskMidpoint - 1),
          bidAmountSum5percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.95).map(i => i.price * i.amount).sum),
          bidAmountSum10percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.90).map(i => i.price * i.amount).sum),
          bidAmountSum25percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.75).map(i => i.price * i.amount).sum),
          bidAmountSum50percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.50).map(i => i.price * i.amount).sum),
          bidAmountSum75percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.25).map(i => i.price * i.amount).sum),
          bidAmountSum85percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.15).map(i => i.price * i.amount).sum),
          bidAmountSum100percent = Some(bids.filter(_.price >= bidAskMidpoint * 0.00).map(i => i.price * i.amount).sum),
          askAmountSum5percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.05).map(_.amount).sum * bidAskMidpoint),
          askAmountSum10percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.10).map(_.amount).sum * bidAskMidpoint),
          askAmountSum25percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.25).map(_.amount).sum * bidAskMidpoint),
          askAmountSum50percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.50).map(_.amount).sum * bidAskMidpoint),
          askAmountSum75percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.75).map(_.amount).sum * bidAskMidpoint),
          askAmountSum85percent = Some(asks.filter(_.price <= bidAskMidpoint * 1.85).map(_.amount).sum * bidAskMidpoint),
          askAmountSum100percent = Some(asks.filter(_.price <= bidAskMidpoint * 2.00).map(_.amount).sum * bidAskMidpoint),
          askAmountSum200percent = Some(asks.filter(_.price <= bidAskMidpoint * 3.00).map(_.amount).sum * bidAskMidpoint),
          loanOfferRateAvg1 = aggregateLoanOffers(c, lobs, bidAskMidpoint, 1),
          loanOfferRateAvg5 = aggregateLoanOffers(c, lobs, bidAskMidpoint, 5),
          loanOfferRateAvg10 = aggregateLoanOffers(c, lobs, bidAskMidpoint, 10),
          loanOfferRateAvg25 = aggregateLoanOffers(c, lobs, bidAskMidpoint, 25),
          loanOfferRateAvg50 = aggregateLoanOffers(c, lobs, bidAskMidpoint, 50),
          loanOfferRateAvg100 = aggregateLoanOffers(c, lobs, bidAskMidpoint, 100),
          loanOfferRateAvg500 = aggregateLoanOffers(c, lobs, bidAskMidpoint, 500),
          loanOfferRateAvg1000 = aggregateLoanOffers(c, lobs, bidAskMidpoint, 1000),
          loanOfferRateAvg2500 = aggregateLoanOffers(c, lobs, bidAskMidpoint, 2500),
          loanOfferRateAvg5000 = aggregateLoanOffers(c, lobs, bidAskMidpoint, 5000),
          loanOfferRateAvg10000 = aggregateLoanOffers(c, lobs, bidAskMidpoint, 10000),
          loanOfferRateAvgAll = lobs.get(c).map(_.offers.foldLeft((BigDecimal(0), BigDecimal(0))) {
            case ((weightedRateSum, amountSum), next) =>
              weightedRateSum + next.amount * next.rate -> (amountSum + next.amount)
          }).collect { case (weightedRateSum, amountSum) if amountSum > 0 => weightedRateSum / amountSum },
          loanOfferAmountSum = lobs.get(c).map(_.offers.map(_.amount).sum).map(_ * bidAskMidpoint)
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

    case UpsertChartData(timestamp, cds) =>
      val instant = Instant.ofEpochSecond(timestamp)
      val sqlTimestamp = Timestamp.from(instant)
      val sqlTimstamp5MinAgo = Timestamp.from(instant.minus(5, ChronoUnit.MINUTES))

      val updatesFuture = Future.sequence(cds.map { case (c, cdOption) =>
        (cdOption match {
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
                  ChartData(timestamp, previousClose, previousClose, previousClose, previousClose, 0))))
        }).map {
          case Some(cd) =>
            for {
              rowsAffected <- ticks
                .filter(t => t.timestamp === sqlTimestamp && t.currencyPair === c && t.chartDataFinal === false)
                .map(t => (t.open, t.high, t.low, t.close, t.volume, t.chartDataFinal))
                .update(Some(cd.open), Some(cd.high), Some(cd.low), Some(cd.close), Some(cd.volume), true)
              result <- rowsAffected match {
                case 0 => ticks += Tick.empty().copy(
                  timestamp = sqlTimestamp,
                  currencyPair = c,
                  open = Some(cd.open),
                  high = Some(cd.high),
                  low = Some(cd.low),
                  close = Some(cd.close),
                  volume = Some(cd.volume),
                  chartDataFinal = true)
                case 1 => DBIO.successful(1)
                case n => DBIO.failed(new RuntimeException(
                  s"Expected 0 or 1 change, not $n at timestamp $timestamp for currency pair $c"))
              }
            } yield result
          case None =>
            DBIO.successful(0)
        }
      })

      val result = updatesFuture.flatMap(updates =>
        DB.get.run(DBIO.sequence(updates.toSeq)))

      result onSuccess { case seq if seq.contains(1) =>
        log.info(s"Upserted chart data at timestamp $timestamp")
      }

      result onFailure { case e =>
        log.error(e, s"Could not upsert chart data at $timestamp")
      }

    case RequestScheduledUpdateOldChartData =>
      val sqlTimestamp1DayAgo = Timestamp.from(Instant.now.minus(1, ChronoUnit.DAYS))
      val deleteOldTicks = ticks
        .filter(t => t.timestamp <= sqlTimestamp1DayAgo && t.chartDataFinal === false && t.bidAskMidpoint.isEmpty)
        .delete
      val finalizeOldTicks = ticks
        .filter(t => t.timestamp <= sqlTimestamp1DayAgo && t.chartDataFinal === false && t.bidAskMidpoint.isDefined)
        .map(_.chartDataFinal)
        .update(true)
      val query = deleteOldTicks andThen finalizeOldTicks
      DB.get.run(query)
      self ! RequestUpdateOldChartData(
        Instant.now.minus(Config.updateDelay, ChronoUnit.MINUTES).getEpochSecond)

    case RequestUpdateOldChartData(until) =>
      val untilSqlTimestamp = Timestamp.from(Instant.ofEpochSecond(until))
      val query = ticks
        .filter(t => t.timestamp <= untilSqlTimestamp && t.chartDataFinal === false)
        .sortBy(_.timestamp.asc)
        .map(t => (t.timestamp, t.currencyPair))

      DB.get.run(query.result).foreach { case seq =>
        if (seq.nonEmpty) {
          log.info("Updating chart data of old candles")
        } else {
          log.info("All chart data is up-to-date")
        }

        seq.grouped(288).foreach { group =>
          val start = group.head._1.toInstant.getEpochSecond
          val end = group.last._1.toInstant.getEpochSecond
          (poller ? FetchOldChartData(start, end)).mapTo[OldChartData] onSuccess { case OldChartData(cds) =>
            val tuples = group.map { case (sqlTimestamp, c) =>
              val timestamp = sqlTimestamp.toInstant.getEpochSecond
              (timestamp, c, cds.get(timestamp).flatMap(_.get(c)))
            }

            val candles = tuples.groupBy(_._1).map { case (t, subTuple) =>
              t -> subTuple.groupBy(_._2).map { case (c, subSubTuple) =>
                c -> subSubTuple.head._3
              }
            }

            for (t <- candles.keys.toSeq.sorted) {
              self ! UpsertChartData(t, candles(t))
            }
          }
        }
      }

    case RequestInsertOldChartData(fromOption, until) =>
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
        val oldChartData = (poller ? FetchOldChartData(from, until)).mapTo[OldChartData]
        for {
          allCs <- allCurrencies
          ocd <- oldChartData
        } yield {
          val cds = ocd.cds
          for (t <- cds.keys.toSeq.sorted if t >= from && t <= until) {
            self ! UpsertChartData(t, allCs.map(c => c -> cds(t).get(c)).toMap)
          }
        }
      }
  }

}
