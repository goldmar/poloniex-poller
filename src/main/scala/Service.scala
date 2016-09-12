import java.sql.Timestamp
import java.time._
import java.time.temporal.ChronoUnit
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
import purecsv.safe._
import purecsv.safe.converter.StringConverter
import CSVStringConverters._
import Schema._

import scala.util.Try

trait Service extends JsonProtocols {
  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: Materializer

  val log: LoggingAdapter

  implicit def exceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case BadRequestException(msg) =>
        complete(BadRequest -> msg)
    }

  implicit def csvMarshaller: Marshaller[Source[CSVLine, NotUsed], HttpEntity.Chunked] =
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
    val tickPublisher = DB.get.stream(
      query.result.withStatementParameters(statementInit = DB.enableStream))

    val tickWithVolumeSource = Source.fromPublisher(tickPublisher)
      .scan(Tick.empty(), false, "", BigDecimal(0), Vector.empty[TickInstantVolume]) {
        case ((tick, weekIsFull, currencyPair, volumeSum, window), next) =>
          val nextC = next.currencyPair
          val nextInstant = next.timestamp.toInstant
          val nextVolume = next.volume
          val oneWeekAgo = nextInstant.minus(7, ChronoUnit.DAYS)
          (currencyPair, window.headOption) match {
            case (c, Some(t)) if c == nextC && t.instant.compareTo(oneWeekAgo) <= 0 =>
              (next, true, c,
                volumeSum - window.head.volume.getOrElse(0) + nextVolume.getOrElse(0),
                window.tail :+ TickInstantVolume(nextInstant, nextVolume))
            case (c, _) if c == nextC =>
              (next, false, c,
                volumeSum + nextVolume.getOrElse(0),
                window :+ TickInstantVolume(nextInstant, nextVolume))
            case _ =>
              (next, false, nextC,
                nextVolume.getOrElse(0),
                Vector(TickInstantVolume(nextInstant, nextVolume)))
          }
      }.drop(1).map { case (tick, weekIsFull, currencyPair, volumeSum, window) =>
      (weekIsFull, window.flatMap(_.volume).length) match {
        case (true, l) if l > 0 => (tick, Some(volumeSum / l))
        case _ => (tick, None)
      }
    }.filter { case (tick, _) =>
      tick.bidAskMidpoint.nonEmpty
    }

    val csvTickSource = tickWithVolumeSource.map { case (tick, avgVolumeOption) =>
      CSVTick.fromTick(tick).copy(
        volumeAvgPast7Days = avgVolumeOption,
        loanOfferAmountSumRelToAvgVol =
          for {
            loanOfferAmountSum <- tick.loanOfferAmountSum
            avgVolume <- avgVolumeOption if avgVolume > 0
          } yield loanOfferAmountSum / avgVolume)
    }

    special match {
      case false =>
        val headerSource = Source.single(CSVLine(csvHeader))
        val csvLineSource = csvTickSource.map(t => CSVLine(t.toCSV()))
        Source.combine(headerSource, csvLineSource)(Concat(_))
      case true =>
        implicit val converter = germanTimestampConverter
        val specialHeaderSource = Source.single(CSVLine(specialCSVHeader))
        val specialCSVLineSource = csvTickSource.map {
          case tick@t if t.volume.getOrElse(BigDecimal(0)) > 0 =>
            CSVLine(SpecialCSVTick.fromTick(tick).toCSV())
          case tick@t =>
            CSVLine(SpecialCSVTick.fromTick(tick.copy(
              open = None, high = None, low = None, close = None
            )).toCSV())
        }
        Source.combine(specialHeaderSource, specialCSVLineSource)(Concat(_))
    }

  }

  val routes = {
    logRequestResult("poloniex-webservice") {
      encodeResponse {
        pathPrefix("csv") {
          pathSingleSlash {
            get {
              complete("")
              parameters(
                Symbol("special") ? false) { special =>
                complete {
                  val query = ticks
                    .filter(t => t.chartDataFinal === true)
                    .sortBy(t => (t.currencyPair.asc, t.timestamp.asc))

                  streamCSV(query, special)
                }
              }
            }
          } ~
          path(Segment) { currencyPair =>
            get {
              parameters(
                Symbol("special") ? false) { special =>
                complete {
                  val query = ticks
                    .filter(t => t.currencyPair === currencyPair && t.chartDataFinal === true)
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
