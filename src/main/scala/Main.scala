import java.sql.SQLSyntaxErrorException
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import slick.jdbc.meta.MTable
import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl._
import akka.stream._
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import Schema._

object Main extends App with Service {
  override implicit val system = ActorSystem()
  override implicit val executor = system.dispatcher

  override val log = Logging(system, getClass)

  val decider: Supervision.Decider = { e =>
    log.error("Unhandled exception in stream", e)
    Supervision.Stop
  }

  val materializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  override implicit val materializer = ActorMaterializer(materializerSettings)

  val scheduler = QuartzSchedulerExtension(system)

  val poloniexDataSaver = system.actorOf(Props[PoloniexDataSaverActor])

  scheduler.schedule("PollEvery5Minutes", poloniexDataSaver, Poll)
  scheduler.schedule("UpdateOldCandlesEveryHour", poloniexDataSaver, RequestScheduledUpdateOldChartData)

  val now = Instant.now
  val oneWeekAgo = now.minus(7, ChronoUnit.DAYS)

  DB.get.run(
    MTable.getTables.map(tables => tables.map(_.name.name))
  ) map { tables =>
    if (tables.contains(ticks.baseTableRow.tableName)) {
      log.info("Table ticks already exists")
      log.info("Filling in missing chart data")
      poloniexDataSaver ! RequestUpdateOldChartData(now.minus(Config.updateDelay, ChronoUnit.MINUTES).getEpochSecond)
      poloniexDataSaver ! RequestInsertOldChartData(None, now.getEpochSecond)
      system.scheduler.scheduleOnce(Config.updateDelay minutes,
        poloniexDataSaver, RequestUpdateOldChartData(now.getEpochSecond))
    } else {
      val result = DB.get.run(DBIO.seq(
        ticks.schema.create
      ))

      result onComplete {
        case Success(_) =>
          log.info("Successfully created table ticks")
          log.info("Filling in previous chart data")
          poloniexDataSaver ! RequestInsertOldChartData(Some(oneWeekAgo.getEpochSecond), now.getEpochSecond)
        case Failure(e) =>
          log.error(e, "Could not create table ticks")
      }
    }
  }

  Http().bindAndHandle(routes, Config.httpInterface, Config.httpPort)
}

object Config {
  val config = ConfigFactory.load()
  val httpInterface = config.as[String]("http.interface")
  val httpPort = config.as[Int]("http.port")
  val updateDelay = config.as[Int]("poloniex.update-delay-in-minutes")
  val marginCurrencies = config.as[Seq[String]]("poloniex.margin-currencies")
}

object DB {
  val config = DatabaseConfig.forConfig[JdbcProfile]("database")
  val get = config.db

  /**
    * Use this for streaming from a MySQL database
    *
    * Example: val stream = sourceDb.stream(query.result.withStatementParameters(statementInit = enableStream))
    */
  def enableStream(statement: java.sql.Statement): Unit = {
    statement match {
      case s if s.isWrapperFor(classOf[com.mysql.jdbc.StatementImpl]) =>
        s.unwrap(classOf[com.mysql.jdbc.StatementImpl]).enableStreamingResults()
      case _ => // do nothing
    }
  }
}