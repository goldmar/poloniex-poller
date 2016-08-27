import java.sql.Timestamp
import java.time.LocalDateTime
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
      timestamp.toLocalDateTime.format(formatter)
  }

  def streamCSV(sqlAction: SqlStreamingAction[Vector[CSVTick], CSVTick, Effect], format: String): ToResponseMarshallable = {
    val tickPublisher = DB.get.stream(
      sqlAction.withStatementParameters(statementInit = DB.enableStream)
    )

    val headerSource = Source.single(CSVLine(csvHeader))

    val tickSource = format match {
      case "special" =>
        implicit val converter = germanTimestampConverter
        Source.fromPublisher(tickPublisher).map(t => CSVLine(t.toCSV()))

      case _ =>
        Source.fromPublisher(tickPublisher).map(t => CSVLine(t.toCSV()))
    }

    Source.combine(headerSource, tickSource)(Concat(_))
  }

  val routes = {
    logRequestResult("poloniex-webservice") {
      encodeResponse {
        pathPrefix("csv") {
          pathSingleSlash {
            get {
              parameters('format ? "default") { format =>
                complete {
                  streamCSV(CSVTick.getAllTicks, format)
                }
              }
            }
          } ~
          path(Segment) { currencyPair =>
            get {
              parameters('format ? "default") { format =>
                complete {
                  streamCSV(CSVTick.getTicksForCurrency(currencyPair), format)
                }
              }
            }
          }
        }
      }
    }
  }
}
