import java.sql.Timestamp
import java.time._
import java.time.temporal.ChronoUnit
import java.time.format.DateTimeFormatter

import scala.concurrent.ExecutionContextExecutor
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.ExceptionHandler
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.marshalling.{Marshaller, ToResponseMarshallable}
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import purecsv.safe._
import purecsv.safe.converter.StringConverter
import JsonProtocols._
import CSVStringConverters._
import Schema._
import DB.config.profile.api._
import slick.basic.DatabasePublisher

import scala.util.Try

trait Service {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContextExecutor
  implicit val materializer: Materializer

  val log: LoggingAdapter

  implicit def exceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case BadRequestException(msg) =>
        complete(BadRequest -> msg)
    }

  implicit val csvMarshaller =
    Marshaller.withFixedContentType[CSVLine, ByteString](ContentTypes.`text/csv(UTF-8)`) {
      case CSVLine(line) => ByteString(line)
    }

  implicit val csvStreamingSupport = EntityStreamingSupport.csv()
    .withParallelMarshalling(parallelism = 8, unordered = false)

  val csvHeader = {
    import shapeless._
    import shapeless.ops.record._
    val label = LabelledGeneric[Tick]
    val keys = Keys[label.Repr].apply
    keys.toList.map(_.name).mkString(",")
  }

  val specialCSVHeader = {
    import shapeless._
    import shapeless.ops.record._
    val label = LabelledGeneric[SpecialCSVTick]
    val keys = Keys[label.Repr].apply
    keys.toList.map(_.name).mkString(",")
  }

  val germanTimestampConverter = new StringConverter[Timestamp] {
    val formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")

    override def tryFrom(str: String): Try[Timestamp] =
      Try(Timestamp.valueOf(LocalDateTime.parse(str, formatter)))

    override def to(timestamp: Timestamp): String =
      timestamp.toInstant.atZone(ZoneOffset.UTC).format(formatter)
  }

  case class TickInstantVolume(instant: Instant, volume: Option[BigDecimal])

  def streamCSV(query: Query[Ticks, Tick, Seq], special: Boolean): ToResponseMarshallable = {
    val tickPublisher: DatabasePublisher[Tick] = DB.get.stream(
      query.result.withStatementParameters(statementInit = DB.enableStream))

    val tickSource = Source.fromPublisher(tickPublisher)

    val csvSource = if (special) {
      implicit val converter = germanTimestampConverter
      val specialHeaderSource = Source.single(CSVLine(specialCSVHeader))
      val specialCSVLineSource = tickSource.map {
        case tick if tick.volume.getOrElse(BigDecimal(0)) > 0 =>
          CSVLine(SpecialCSVTick.fromTick(tick).toCSV())
        case tick =>
          CSVLine(SpecialCSVTick.fromTick(tick.copy(
            open = None, high = None, low = None, close = None
          )).toCSV())
      }
      Source.combine(specialHeaderSource, specialCSVLineSource)(Concat(_))
    } else {
      val headerSource = Source.single(CSVLine(csvHeader))
      val csvLineSource = tickSource.map(t => CSVLine(t.toCSV()))
      Source.combine(headerSource, csvLineSource)(Concat(_))
    }

    csvSource
  }

  val routes = {
    logRequestResult("poloniex-webservice") {
      encodeResponse {
        pathPrefix("csv") {
          pathSingleSlash {
            get {
              parameters('since ? 0L, 'special ? false) { (since, special) =>
                complete {
                  val instant = Instant.ofEpochSecond(since)
                  val sqlTimestamp = Timestamp.from(instant)
                  val query = ticks
                    .filter(t => t.timestamp >= sqlTimestamp && t.chartDataFinal === true)
                    .sortBy(t => (t.currencyPair.asc, t.timestamp.asc))

                  streamCSV(query, special)
                }
              }
            }
          } ~
          path(Segment) { currencyPair =>
            get {
              parameters('since ? 0L, 'special ? false) { (since, special) =>
                complete {
                  val instant = Instant.ofEpochSecond(since)
                  val sqlTimestamp = Timestamp.from(instant)
                  val query = ticks
                    .filter(t =>
                      t.currencyPair === currencyPair &&
                        t.timestamp >= sqlTimestamp &&
                        t.chartDataFinal === true)
                    .sortBy(_.timestamp.asc)

                  streamCSV(query, special)
                }
              }
            }
          }
        }
      }
    }
  }
}
