import java.io.IOException
import java.time._
import java.time.temporal.{ChronoField, ChronoUnit}

import scala.language.postfixOps
import scala.collection.immutable.{Map, SortedSet}
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import akka.actor.{Actor, ActorLogging}
import akka.pattern.{after, pipe}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import sext._

case object Poll

case class FetchOldChartData(start: Long, end: Long)

case class InsertCandles(timestamp: Long, t: Map[String, BigDecimal],
                         obs: Map[String, OrderBook], los: Map[String, LoanOrderBook])

case class UpdateCandles(timestamp: Long, cds: Map[String, Option[ChartDataCandle]])

case class UpdateOldCandles(from: Long, until: Long, cds: Map[Long, Map[String, ChartDataCandle]])

class PoloniexPollerActor extends Actor with ActorLogging with JsonProtocols {
  implicit val system = context.system
  implicit val dispatcher = context.dispatcher
  implicit val materializer = ActorMaterializer()

  val config = ConfigFactory.load()
  lazy val allCurrencies = Await.result(fetchMarkets(), 10 seconds)
  val marginCurrencies = config.as[Seq[String]]("poloniex.margin-currencies")

  val poloniexConnectionFlow: Flow[HttpRequest, HttpResponse, Any] =
    Http().outgoingConnectionHttps("poloniex.com")

  def poloniexRequest(request: HttpRequest): Future[HttpResponse] =
    Source.single(request).via(poloniexConnectionFlow).runWith(Sink.head)

  def fetchMarkets(): Future[Seq[String]] = {
    poloniexRequest(RequestBuilding.Get(
      s"/public?command=returnTicker")
    ).flatMap { response =>
      response.status match {
        case OK =>
          Unmarshal(response.entity).to[Map[String, TickerJson]].map(_.collect {
            case (c, t) if c.startsWith("BTC_") && t.isFrozen == "0" => c
          }.toSeq)
        case _ => Unmarshal(response.entity).to[String].flatMap { entity =>
          val error = s"Poloniex market request failed with status code ${response.status} and entity $entity"
          log.error(error)
          Future.failed(new IOException(error))
        }
      }
    }
  }

  def fetchTickers(): Future[Map[String, BigDecimal]] = {
    poloniexRequest(RequestBuilding.Get(
      s"/public?command=returnTicker")
    ).flatMap { response =>
      response.status match {
        case OK =>
          Unmarshal(response.entity).to[Map[String, TickerJson]].map(_.filter { case (c, t) =>
            c.startsWith("BTC_")
          }.map { case (c, t) =>
            c -> BigDecimal(t.last)
          })
        case _ => Unmarshal(response.entity).to[String].flatMap { entity =>
          val error = s"Poloniex ticker request failed with status code ${response.status} and entity $entity"
          log.error(error)
          Future.failed(new IOException(error))
        }
      }
    }
  }

  def fetchChartData(start: Long): Future[Map[String, Seq[ChartDataCandle]]] = {
    val requests = for (c <- allCurrencies) yield
      poloniexRequest(RequestBuilding.Get(
        s"/public?command=returnChartData&currencyPair=$c&start=$start&end=9999999999&period=300")
      ).map(c -> _)
    Future.sequence(requests).flatMap { seq =>
      val mappedSeq: Seq[Future[(String, Seq[ChartDataCandle])]] =
        seq.map { case (c: String, response: HttpResponse) =>
          response.status match {
            case OK =>
              Unmarshal(response.entity).to[Seq[ChartDataCandleJson]].map(cdjSeq =>
                c -> cdjSeq.map(j => ChartDataCandle(j.date, j.open, j.high, j.low, j.close, j.volume)))
            case _ => Unmarshal(response.entity).to[String].flatMap { entity =>
              val error = s"Poloniex chart data request failed with status code ${response.status} and entity $entity"
              log.error(error)
              Future.failed(new IOException(error))
            }
          }
        }
      Future.sequence(mappedSeq).map(_.toMap)
    }
  }

  def fetchOrderBooks(): Future[Map[String, OrderBook]] = {
    poloniexRequest(RequestBuilding.Get(
      s"/public?command=returnOrderBook&currencyPair=all&depth=999999")
    ).flatMap { response =>
      response.status match {
        case OK =>
          Unmarshal(response.entity).to[Map[String, OrderBook]].map(_.filter { case (c, ob) =>
            c.startsWith("BTC_")
          })
        case _ => Unmarshal(response.entity).to[String].flatMap { entity =>
          val error = s"Poloniex order book request failed with status code ${response.status} and entity $entity"
          log.error(error)
          Future.failed(new IOException(error))
        }
      }
    }
  }

  def fetchLoanOrders(): Future[Map[String, LoanOrderBook]] = {
    def convertJsonItems(lobj: Seq[LoanOrderBookItemJson]) = {
      lobj.map(i => LoanOrderBookItem(
        BigDecimal(i.rate),
        BigDecimal(i.amount),
        i.rangeMin,
        i.rangeMax))
    }

    val requests = for (c <- marginCurrencies) yield
      poloniexRequest(RequestBuilding.Get(
        s"/public?command=returnLoanOrders&currency=$c&limit=999999")
      ).map(s"BTC_$c" -> _)
    Future.sequence(requests).flatMap { seq =>
      val mappedSeq: Seq[Future[(String, LoanOrderBook)]] =
        seq.map { case (c: String, response: HttpResponse) =>
          response.status match {
            case OK =>
              Unmarshal(response.entity).to[LoanOrderBookJson].map { lobj =>
                val offers = convertJsonItems(lobj.offers)
                val demands = convertJsonItems(lobj.demands)
                c -> LoanOrderBook(
                  SortedSet(offers: _*)(Ordering.by[LoanOrderBookItem, BigDecimal](_.rate)),
                  SortedSet(demands: _*)(Ordering.by[LoanOrderBookItem, BigDecimal](_.rate).reverse)
                )
              }
            case _ => Unmarshal(response.entity).to[String].flatMap { entity =>
              val error = s"Poloniex loan order book request failed with status code ${response.status} and entity $entity"
              log.error(error)
              Future.failed(new IOException(error))
            }
          }
        }
      Future.sequence(mappedSeq).map(_.toMap)
    }
  }

  override def receive = {
    case Poll =>
      val s = sender()

      val now = ZonedDateTime.now(ZoneOffset.UTC)
      val rem = now.get(ChronoField.MINUTE_OF_HOUR) % 5
      val timestamp = (now minus(rem, ChronoUnit.MINUTES))
        .`with`(ChronoField.MICRO_OF_SECOND, 0)
        .`with`(ChronoField.SECOND_OF_MINUTE, 0)
        .toEpochSecond

      for {
        ts <- fetchTickers()
        obs <- fetchOrderBooks()
        los <- fetchLoanOrders()
      } yield {
        s ! InsertCandles(timestamp, ts, obs, los)
      }

      after(6 minutes, using = system.scheduler)(fetchChartData(timestamp)).onComplete {
        case Success(cds) =>
          val candleOptions = cds.map { case (c, candleSeq) =>
            c -> candleSeq.head
          }.map {
            case (c, candle) if candle.timestamp == timestamp => c -> Some(candle)
            case (c, candle) => c -> None
          }
          s ! UpdateCandles(timestamp, candleOptions)
        case Failure(e) =>
          log.error(e, "Poloniex Volume Update failed")
      }

    case FetchOldChartData(start, end) =>
      val s = sender()
      fetchChartData(start).map(_.toSeq.flatMap { case (c, cdSeq) =>
        cdSeq.map(cdc => (c, cdc))
      }.groupBy(_._2.timestamp).map { case (timestamp, candles) =>
        timestamp -> candles.groupBy(_._1).map { case (c, tuples) =>
          c -> tuples.head._2
        }
      }).map { cds =>
        UpdateOldCandles(start, end, cds)
      } pipeTo s
  }
}