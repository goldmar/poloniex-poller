import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter

import scala.concurrent.{ExecutionContextExecutor, Future}
import slick.jdbc.MySQLProfile.api._
import slick.sql.SqlStreamingAction
import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.marshalling.{Marshaller, ToResponseMarshallable}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.ExceptionHandler
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.config.Config
import purecsv.safe._
import purecsv.safe.converter.StringConverter
import CSVStringConverters._
import Schema._
import CSVTick._

import scala.util.Try

trait Service extends JsonProtocols {
  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: Materializer

  def config: Config

  val log: LoggingAdapter

  implicit def exceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case BadRequestException(msg) =>
        complete(BadRequest -> msg)
    }

  implicit def csvMarshaller =
    Marshaller.withFixedContentType(ContentTypes.`text/csv(UTF-8)`) { source: Source[CSVLine, NotUsed] =>
      HttpEntity.Chunked.fromData(ContentTypes.`text/csv(UTF-8)`, source.map(l => ByteString(l.line + "\n")))
    }

  val csvHeader = {
    import shapeless._
    import shapeless.ops.record._
    val label = LabelledGeneric[CSVTick]
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

  def streamCSV(countQuery: Rep[Int], sqlAction: (Int, Int) => SqlStreamingAction[Vector[CSVTick], CSVTick, Effect],
                dateFormat: String, fractionsAsPercent: Boolean): ToResponseMarshallable = {

    val limit = 100

    val headerSource = Source.single(CSVLine(csvHeader))

    val countResult = DB.get.run(countQuery.result)

    var tickSource = Source.fromFuture(countResult)
      .flatMapConcat(count => Source(0 to count / limit))
      .mapAsync(1)(i => DB.get.run(sqlAction(limit, i * limit)))
      .mapConcat(identity)

    tickSource = if (fractionsAsPercent)
      tickSource.map(_.withFractionsAsPercent)
    else
      tickSource

    val csvLineSource = dateFormat match {
      case "german" =>
        implicit val converter = germanTimestampConverter
        tickSource.map(t => CSVLine(t.toCSV()))
      case _ =>
        tickSource.map(t => CSVLine(t.toCSV()))
    }

    Source.combine(headerSource, csvLineSource)(Concat(_))
  }

  val routes = {
    logRequestResult("poloniex-webservice") {
      encodeResponse {
        pathPrefix("csv") {
          pathSingleSlash {
            get {
              parameters(
                Symbol("date-format") ? "default",
                Symbol("fractions-as-percent") ? false) { (dateFormat, fractionsAsPercent) =>
                complete {
                  val countQuery = ticks
                    .filter(_.chartDataFinal)
                    .length

                  streamCSV(countQuery, CSVTick.getAllTicks, dateFormat, fractionsAsPercent)
                }
              }
            }
          } ~
          path(Segment) { currencyPair =>
            get {
              parameters(
                Symbol("date-format") ? "default",
                Symbol("fractions-as-percent") ? false) { (dateFormat, fractionsAsPercent) =>
                complete {
                  val countQuery = ticks
                    .filter(t => t.chartDataFinal && t.currencyPair === currencyPair)
                    .length

                  streamCSV(countQuery, CSVTick.getTicksForCurrency(currencyPair), dateFormat, fractionsAsPercent)
                }
              }
            }
          }
        }
      }
    }
  }
}
