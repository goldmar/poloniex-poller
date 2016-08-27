import java.sql.Timestamp
import java.time._
import java.time.temporal.{ChronoField, ChronoUnit}

import scala.concurrent._

import slick.jdbc.MySQLProfile.api._
import akka.actor.{Actor, ActorLogging, Props}
import akka.stream._
import akka.stream.scaladsl._
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import sext._
import Schema._

case class RequestUpdateOldCandles(until: Long)

class PoloniexDataSaverActor extends Actor with ActorLogging {
  implicit val system = context.system
  implicit val dispatcher = context.dispatcher
  implicit val materializer = ActorMaterializer()

  val config = ConfigFactory.load()

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
          bidAmountSum5percent = bids.filter(_.price >= bidAskMidpoint * 0.95).map(_.amount).sum * bidAskMidpoint,
          bidAmountSum10percent = bids.filter(_.price >= bidAskMidpoint * 0.90).map(_.amount).sum * bidAskMidpoint,
          bidAmountSum25percent = bids.filter(_.price >= bidAskMidpoint * 0.75).map(_.amount).sum * bidAskMidpoint,
          bidAmountSum50percent = bids.filter(_.price >= bidAskMidpoint * 0.50).map(_.amount).sum * bidAskMidpoint,
          bidAmountSum75percent = bids.filter(_.price >= bidAskMidpoint * 0.25).map(_.amount).sum * bidAskMidpoint,
          bidAmountSum85percent = bids.filter(_.price >= bidAskMidpoint * 0.15).map(_.amount).sum * bidAskMidpoint,
          bidAmountSum100percent = bids.filter(_.price >= bidAskMidpoint * 0.00).map(_.amount).sum * bidAskMidpoint,
          askAmountSum5percent = asks.filter(_.price <= bidAskMidpoint * 1.05).map(_.amount).sum * bidAskMidpoint,
          askAmountSum10percent = asks.filter(_.price <= bidAskMidpoint * 1.10).map(_.amount).sum * bidAskMidpoint,
          askAmountSum25percent = asks.filter(_.price <= bidAskMidpoint * 1.25).map(_.amount).sum * bidAskMidpoint,
          askAmountSum50percent = asks.filter(_.price <= bidAskMidpoint * 1.50).map(_.amount).sum * bidAskMidpoint,
          askAmountSum75percent = asks.filter(_.price <= bidAskMidpoint * 1.75).map(_.amount).sum * bidAskMidpoint,
          askAmountSum85percent = asks.filter(_.price <= bidAskMidpoint * 1.85).map(_.amount).sum * bidAskMidpoint,
          askAmountSum100percent = asks.filter(_.price <= bidAskMidpoint * 2.00).map(_.amount).sum * bidAskMidpoint,
          askAmountSum200percent = asks.filter(_.price <= bidAskMidpoint * 3.00).map(_.amount).sum * bidAskMidpoint,
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

    case UpdateCandles(timestamp, cds) =>
      val sqlTimestamp = Timestamp.from(Instant.ofEpochSecond(timestamp))

      val updates = cds.map { case (c, cdcOption) =>
        ticks
          .filter(t => t.timestamp === sqlTimestamp && t.currencyPair === c)
          .map(t =>
            (t.open, t.high, t.low, t.close, t.volume, t.chartDataFinal))
          .update(
            cdcOption.map(_.open), cdcOption.map(_.high), cdcOption.map(_.low),
            cdcOption.map(_.close), cdcOption.map(_.volume), true)
      }

      val result = DB.get.run(DBIO.seq(updates.toSeq: _*))

      result onSuccess { case _ =>
        log.info(s"Updated candles at timestamp $timestamp")
      }

      result onFailure { case e =>
        log.error(e, s"Could not update candles at $timestamp")
      }

    case RequestUpdateOldCandles(until) =>
      val sqlTimestamp = Timestamp.from(Instant.ofEpochSecond(until))
      val query = ticks
        .filter(t => t.timestamp <= sqlTimestamp && t.chartDataFinal === false)
        .sortBy(_.timestamp.asc)
        .map(_.timestamp)
        .take(1)

      DB.get.run(query.result.headOption).map {
        case Some(timestamp) =>
          val start = timestamp.toInstant.getEpochSecond
          val end = until
          log.info(s"Updating old candles since $start")
          poller ! FetchOldChartData(start, end)
        case None =>
          log.info("All candles are up-to-date")
      }

    case UpdateOldCandles(from, until, cds) =>
      val endTimestamp = Timestamp.from(Instant.ofEpochSecond(until))
      val query = ticks
        .filter(t => t.chartDataFinal === false && t.timestamp < endTimestamp)
        .sortBy(_.timestamp.asc)
        .map(t => (t.timestamp, t.currencyPair))

      val results = DB.get.run(query.result)

      results onSuccess { case r =>
        val tuples = r map { case (sqlTimestamp, c) =>
          val timestamp = sqlTimestamp.toInstant.getEpochSecond
          cds.get(timestamp).flatMap(_.get(c)) match {
            case Some(candle) =>
              (timestamp, c, Some(candle))
            case None =>
              (timestamp, c, None)
          }
        }

        val candles = tuples.groupBy(_._1).map { case (t, tuple) =>
          t -> tuple.groupBy(_._2).map { case (c, tuple) =>
            c -> tuple.head._3
          }
        }

        for (t <- candles.keys.toSeq.sorted) {
          self ! UpdateCandles(t, candles(t))
        }
      }
  }

}


