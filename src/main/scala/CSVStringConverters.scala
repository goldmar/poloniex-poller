import java.sql.Timestamp
import java.time.Instant

import purecsv.safe.converter.StringConverter

import scala.util.Try

/**
  * Created by secret on 28/08/16.
  */
object CSVStringConverters {
  implicit val bigDecimalStringConverter = new StringConverter[BigDecimal] {
    override def tryFrom(str: String): Try[BigDecimal] = {
      Try(BigDecimal(str))
    }

    override def to(bigDecimal: BigDecimal): String = {
      bigDecimal.toString()
    }
  }

  implicit val timestampStringConverter = new StringConverter[Timestamp] {
    override def tryFrom(str: String): Try[Timestamp] = {
      Try(Timestamp.from(Instant.ofEpochSecond(str.toLong)))
    }

    override def to(timestamp: Timestamp): String = {
      (timestamp.toInstant.toEpochMilli / 1000).toString
    }
  }
}
