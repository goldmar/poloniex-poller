import java.sql.SQLSyntaxErrorException
import java.time.{ZoneOffset, ZonedDateTime}

import slick.jdbc.MySQLProfile.api._
import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl._
import akka.stream._
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import Schema._
import slick.jdbc.meta.MTable

object Main extends App with Service {
  override implicit val system = ActorSystem()
  override implicit val executor = system.dispatcher

  override val config = ConfigFactory.load()
  override val log = Logging(system, getClass)

  val decider: Supervision.Decider = { e =>
    log.error("Unhandled exception in stream", e)
    Supervision.Stop
  }

  val materializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  override implicit val materializer = ActorMaterializer(materializerSettings)

  val scheduler = QuartzSchedulerExtension(system)

  DB.get.run(
    MTable.getTables.map(tables => tables.map(_.name.name))
  ) map { tables =>
    if (tables.contains(ticks.baseTableRow.tableName)) {
      log.info("Table ticks already exists")
    } else {
      val result = DB.get.run(DBIO.seq(
        ticks.schema.create
      ))

      result onComplete {
        case Success(_) =>
          log.info("Successfully created table ticks")
        case Failure(e) =>
          log.error(e, "Could not create table ticks")
      }
    }
  }

  val poloniexDataSaver = system.actorOf(Props[PoloniexDataSaverActor])
  val now = ZonedDateTime.now(ZoneOffset.UTC)
  //system.scheduler.scheduleOnce(6 minutes, poloniexDataSaver, RequestUpdateOldCandles(now.toEpochSecond))
  poloniexDataSaver ! RequestUpdateOldCandles(now.toEpochSecond)

  scheduler.schedule("Every5Minutes", poloniexDataSaver, Poll)

  Http().bindAndHandle(routes, config.as[String]("http.interface"), config.as[Int]("http.port"))
}

object DB {
  private val db = Database.forConfig("db")

  def get = db

  /**
    * Use this for streaming from a MySQL database
    *
    * Example: val stream = sourceDb.stream(query.result.withStatementParameters(statementInit = enableStream))
    */
  def enableStream(statement: java.sql.Statement): Unit = {
    statement match {
      case s: com.mysql.jdbc.StatementImpl => s.enableStreamingResults()
      case _ =>
    }
  }
}