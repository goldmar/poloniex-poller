import java.io.IOException
import java.time._
import java.time.temporal.{ChronoField, ChronoUnit}

import scala.language.postfixOps
import scala.collection.immutable.{Map, SortedSet}
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import akka.actor.{Actor, ActorLogging}
import akka.pattern.{after, pipe}
import akka.stream._
import akka.stream.scaladsl._
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import sext._
import JsonProtocols._

case object Poll

case object UpdateCurrencyList

case class UpdatedCurrencyList(cs: Seq[String])

case class FetchOldChartData(start: Long, end: Long)

case class InsertData(timestamp: Long, obs: Map[String, OrderBook], lobs: Map[String, LoanOrderBook])

case class UpsertChartData(timestamp: Long, cds: Map[String, Option[ChartData]])

case class OldChartData(cds: Map[Long, Map[String, ChartData]])

case object ListAllCurrencies

class PoloniexPollerActor extends Actor with ActorLogging {
  implicit val system = context.system
  implicit val dispatcher = context.dispatcher

  val decider: Supervision.Decider = { e =>
    log.error("Unhandled exception in stream", e)
    Supervision.Resume
  }

  val materializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  implicit val materializer = ActorMaterializer(materializerSettings)

  var allCurrencies = Seq.empty[String]

  val poloniexPoolFlow: Flow[(HttpRequest, String), (Try[HttpResponse], String), HostConnectionPool] =
    Http().cachedHostConnectionPoolHttps[String]("poloniex.com")

  override def preStart(): Unit = {
    val currencies = fetchMarkets().map(_.sorted)

    currencies onSuccess { case cs =>
      allCurrencies = cs
      log.info(s"Tracking the following currencies: $cs")
    }

    currencies onFailure { case e =>
      log.error(e, "Could not fetch Poloniex markets")
    }

    Await.result(currencies, 10 seconds)
  }

  def retry[T](op: => Future[T], retries: Int): Future[T] = {
    op recoverWith {
      case _ if retries > 0 =>
        log.info("Retrying operation")
        retry(op, retries - 1)
    }
  }

  def poloniexSingleRequest(request: HttpRequest): Future[HttpResponse] = {
    Source.single(request -> "unique").via(poloniexPoolFlow).runWith(Sink.head).flatMap {
      case (Success(r: HttpResponse), _) ⇒ Future.successful(r)
      case (Failure(f), _) ⇒ Future.failed(f)
    }
  }

  def fetchMarkets(): Future[Seq[String]] = {
    poloniexSingleRequest(RequestBuilding.Get(
      s"/public?command=returnTicker")
    ).flatMap { response =>
      response.status match {
        case OK =>
          Unmarshal(response.entity).to[Map[String, TickerJson]].recoverWith {
            // see https://github.com/akka/akka-http/issues/17
            case e =>
              log.error(e, "Could not unmarshall market response")
              response.entity.dataBytes.runWith(Sink.ignore).flatMap(_ => Future.failed(e))
          }.map(_.collect {
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

  def fetchChartData(start: Long, end: Long = 9999999999L): Future[Map[String, Seq[ChartData]]] = {
    val requests = for (c <- allCurrencies) yield
      RequestBuilding.Get(
        s"/public?command=returnChartData&currencyPair=$c&start=$start&end=$end&period=300"
      ) -> c
    Source(requests.to[collection.immutable.Iterable])
      .via(poloniexPoolFlow)
      .mapAsyncUnordered(200) {
        case (Success(response: HttpResponse), c) =>
          response.status match {
            case OK =>
              Unmarshal(response.entity).to[Seq[ChartDataJson]].recoverWith {
                // see https://github.com/akka/akka-http/issues/17
                case e =>
                  log.error(e, s"Could not unmarshall chart data response for currency $c")
                  response.entity.dataBytes.runWith(Sink.ignore).flatMap(_ => Future.failed(e))
              }.map(cdjSeq =>
                c -> cdjSeq.map(j => ChartData(j.date, j.open, j.high, j.low, j.close, j.volume)))
            case _ =>
              Unmarshal(response.entity).to[String].flatMap { entity =>
                val error = s"Poloniex chart data request for currency $c failed with status code ${response.status} and entity $entity"
                log.error(error)
                Future.failed(new IOException(error))
              }
          }

        case (Failure(e), c) =>
          log.error(e, "Poloniex chart data request failed")
          Future.failed(e)
      }
      .runFold(Map.empty[String, Seq[ChartData]]) { case (m, (c, cdSeq)) =>
        m + (c -> cdSeq)
      }
  }

  def fetchChartDataAsNestedMap(start: Long, end: Long): Future[Map[Long, Map[String, ChartData]]] = {
    fetchChartData(start, end).map(_.toSeq.flatMap { case (c, cdSeq) =>
      cdSeq.map(cdc => (c, cdc))
    }.groupBy(_._2.timestamp).map { case (timestamp, candles) =>
      timestamp -> candles.groupBy(_._1).map { case (c, tuples) =>
        c -> tuples.head._2
      }
    })
  }

  def fetchOrderBooks(): Future[Map[String, OrderBook]] = {
    poloniexSingleRequest(RequestBuilding.Get(
      s"/public?command=returnOrderBook&currencyPair=all&depth=999999")
    ).flatMap { response =>
      response.status match {
        case OK =>
          Unmarshal(response.entity).to[Map[String, OrderBook]].recoverWith {
            // see https://github.com/akka/akka-http/issues/17
            case e =>
              log.error(e, "Could not unmarshall order book response")
              response.entity.dataBytes.runWith(Sink.ignore).flatMap(_ => Future.failed(e))
          }.map(_.filterKeys(allCurrencies.contains(_)))
        case _ => Unmarshal(response.entity).to[String].flatMap { entity =>
          val error = s"Poloniex order book request failed with status code ${response.status} and entity $entity"
          log.error(error)
          Future.failed(new IOException(error))
        }
      }
    }
  }

  def fetchLoanOrders(): Future[Map[String, LoanOrderBook]] = {
    def convertJsonItems(lobj: Seq[LoanOrderBookItemJson]): Seq[LoanOrderBookItem] = {
      lobj.map(i => LoanOrderBookItem(
        BigDecimal(i.rate),
        BigDecimal(i.amount),
        i.rangeMin,
        i.rangeMax))
    }

    val requests: () => Iterator[(HttpRequest, String)] = () =>
      for (c <- Config.marginCurrencies.iterator) yield
        RequestBuilding.Get(
          s"/public?command=returnLoanOrders&currency=$c&limit=999999"
        ) -> s"BTC_$c"

    Source.fromIterator(requests)
      .via(poloniexPoolFlow)
      .mapAsyncUnordered(200) {
        case (Success(response: HttpResponse), c) =>
          response.status match {
            case OK =>
              Unmarshal(response.entity).to[LoanOrderBookJson].recoverWith {
                // see https://github.com/akka/akka-http/issues/17
                case e =>
                  log.error(e, s"Could not unmarshall loan order book response for currency $c")
                  response.entity.dataBytes.runWith(Sink.ignore).flatMap(_ => Future.failed(e))
              }.map { lobj =>
                val offers = convertJsonItems(lobj.offers)
                val demands = convertJsonItems(lobj.demands)
                c -> LoanOrderBook(
                  SortedSet(offers: _*)(Ordering.by[LoanOrderBookItem, BigDecimal](_.rate)),
                  SortedSet(demands: _*)(Ordering.by[LoanOrderBookItem, BigDecimal](_.rate).reverse)
                )
              }
            case _ => Unmarshal(response.entity).to[String].flatMap { entity =>
              val error = s"Poloniex loan order book request for currency $c failed with status code ${response.status} and entity $entity"
              log.error(error)
              Future.failed(new IOException(error))
            }
          }

        case (Failure(e), c) =>
          log.error(e, s"Poloniex loan order book request for currency $c failed")
          Future.failed(e)
      }
      .runFold(Map.empty[String, LoanOrderBook]) { case (m, (c, lob)) =>
        m + (c -> lob)
      }
  }

  override def receive = {
    case UpdateCurrencyList =>
      val currencies = fetchMarkets().map(_.sorted)

      currencies onSuccess { case cs =>
        if (allCurrencies.toSet != cs.toSet) {
          self ! UpdatedCurrencyList(cs)
        }
      }

      currencies onFailure { case e =>
        log.error(e, "Could not fetch Poloniex markets")
      }

    case UpdatedCurrencyList(cs) =>
      allCurrencies = cs
      log.info(s"Updated list of tracked currencies: $cs")

    case Poll =>
      val s = sender

      val now = ZonedDateTime.now(ZoneOffset.UTC)
      val rem = now.get(ChronoField.MINUTE_OF_HOUR) % 5
      val timestamp = (now minus(rem, ChronoUnit.MINUTES))
        .`with`(ChronoField.MICRO_OF_SECOND, 0)
        .`with`(ChronoField.SECOND_OF_MINUTE, 0)
        .toEpochSecond

      val insertCandles = for {
        obs <- retry(fetchOrderBooks(), 3).recover(Map())
        los <- retry(fetchLoanOrders(), 3).recover(Map())
      } yield {
        s ! InsertData(timestamp, obs, los)
      }

      insertCandles onFailure { case e =>
        throw e
      }

      after(
        Config.updateDelay minutes,
        using = system.scheduler)(fetchChartData(timestamp)
      ).onComplete {
        case Success(cds) =>
          val candleOptions = cds.map { case (c, candleSeq) =>
            c -> Option(candleSeq.head).filter(_.timestamp == timestamp)
          }
          s ! UpsertChartData(timestamp, candleOptions)
        case Failure(e) =>
          log.error(e, "Poloniex chart data update failed")
      }

    case FetchOldChartData(start, end) =>
      val s = sender
      fetchChartDataAsNestedMap(start, end).map { cds =>
        OldChartData(cds)
      } pipeTo s

    case ListAllCurrencies =>
      sender ! allCurrencies
  }
}