import java.sql.Timestamp

import slick.jdbc.MySQLProfile.api._
import shapeless.{Generic, HList, HNil}
import slick.sql.SqlProfile.ColumnOption.SqlType
import slickless._

object Schema {

  case class MySQLInterval()

  val subtractDays = SimpleExpression.binary[Timestamp, Int, Timestamp] { (timestamp, days, qb) =>
    qb.sqlBuilder += "date_sub("
    qb.expr(timestamp)
    qb.sqlBuilder += ", interval "
    qb.expr(days)
    qb.sqlBuilder += " day)"
  }

  case class Tick(id: Long, timestamp: Timestamp, currencyPair: String,
                  open: Option[BigDecimal], high: Option[BigDecimal],
                  low: Option[BigDecimal], close: Option[BigDecimal],
                  volume: Option[BigDecimal], chartDataFinal: Boolean,
                  bidAskMidpoint: Option[BigDecimal],
                  bidPriceAvg1: Option[BigDecimal], bidPriceAvg10: Option[BigDecimal],
                  bidPriceAvg25: Option[BigDecimal], bidPriceAvg50: Option[BigDecimal],
                  bidPriceAvg100: Option[BigDecimal], bidPriceAvg500: Option[BigDecimal],
                  bidPriceAvg1000: Option[BigDecimal], bidPriceAvg2500: Option[BigDecimal],
                  bidPriceAvg5000: Option[BigDecimal], bidPriceAvg10000: Option[BigDecimal],
                  askPriceAvg1: Option[BigDecimal], askPriceAvg10: Option[BigDecimal],
                  askPriceAvg25: Option[BigDecimal], askPriceAvg50: Option[BigDecimal],
                  askPriceAvg100: Option[BigDecimal], askPriceAvg500: Option[BigDecimal],
                  askPriceAvg1000: Option[BigDecimal], askPriceAvg2500: Option[BigDecimal],
                  askPriceAvg5000: Option[BigDecimal], askPriceAvg10000: Option[BigDecimal],
                  bidAmountSum5percent: BigDecimal, bidAmountSum10percent: BigDecimal,
                  bidAmountSum25percent: BigDecimal, bidAmountSum50percent: BigDecimal,
                  bidAmountSum75percent: BigDecimal, bidAmountSum85percent: BigDecimal,
                  bidAmountSum100percent: BigDecimal, askAmountSum5percent: BigDecimal,
                  askAmountSum10percent: BigDecimal, askAmountSum25percent: BigDecimal,
                  askAmountSum50percent: BigDecimal, askAmountSum75percent: BigDecimal,
                  askAmountSum85percent: BigDecimal, askAmountSum100percent: BigDecimal,
                  askAmountSum200percent: BigDecimal,
                  loanOfferRateAvg1: Option[BigDecimal], loanOfferRateAvg10: Option[BigDecimal],
                  loanOfferRateAvg25: Option[BigDecimal], loanOfferRateAvg50: Option[BigDecimal],
                  loanOfferRateAvg100: Option[BigDecimal], loanOfferRateAvg500: Option[BigDecimal],
                  loanOfferRateAvg1000: Option[BigDecimal], loanOfferRateAvg2500: Option[BigDecimal],
                  loanOfferRateAvg5000: Option[BigDecimal], loanOfferRateAvg10000: Option[BigDecimal],
                  loanOfferAmountSum: Option[BigDecimal])

  class Ticks(tag: Tag) extends Table[Tick](tag, "ticks") {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

    def timestamp = column[Timestamp]("timestamp", SqlType("timestamp not null default CURRENT_TIMESTAMP"))

    def currencyPair = column[String]("currency_pair", O.SqlType("VARCHAR(10)"))

    def open = column[Option[BigDecimal]]("open", O.SqlType("DECIMAL(20,8)"))

    def high = column[Option[BigDecimal]]("high", O.SqlType("DECIMAL(20,8)"))

    def low = column[Option[BigDecimal]]("low", O.SqlType("DECIMAL(20,8)"))

    def close = column[Option[BigDecimal]]("close", O.SqlType("DECIMAL(20,8)"))

    def volume = column[Option[BigDecimal]]("volume", O.SqlType("DECIMAL(20,8)"))

    def chartDataFinal = column[Boolean]("chart_data_final", O.Default(false))

    def bidAskMidpoint = column[Option[BigDecimal]]("bid_ask_midpoint", O.SqlType("DECIMAL(20,8)"))

    def bidPriceAvg1 = column[Option[BigDecimal]]("bid_price_avg_1", O.SqlType("DECIMAL(20,8)"))

    def bidPriceAvg10 = column[Option[BigDecimal]]("bid_price_avg_10", O.SqlType("DECIMAL(20,8)"))

    def bidPriceAvg25 = column[Option[BigDecimal]]("bid_price_avg_25", O.SqlType("DECIMAL(20,8)"))

    def bidPriceAvg50 = column[Option[BigDecimal]]("bid_price_avg_50", O.SqlType("DECIMAL(20,8)"))

    def bidPriceAvg100 = column[Option[BigDecimal]]("bid_price_avg_100", O.SqlType("DECIMAL(20,8)"))

    def bidPriceAvg500 = column[Option[BigDecimal]]("bid_price_avg_500", O.SqlType("DECIMAL(20,8)"))

    def bidPriceAvg1000 = column[Option[BigDecimal]]("bid_price_avg_1000", O.SqlType("DECIMAL(20,8)"))

    def bidPriceAvg2500 = column[Option[BigDecimal]]("bid_price_avg_2500", O.SqlType("DECIMAL(20,8)"))

    def bidPriceAvg5000 = column[Option[BigDecimal]]("bid_price_avg_5000", O.SqlType("DECIMAL(20,8)"))

    def bidPriceAvg10000 = column[Option[BigDecimal]]("bid_price_avg_10000", O.SqlType("DECIMAL(20,8)"))

    def askPriceAvg1 = column[Option[BigDecimal]]("ask_price_avg_1", O.SqlType("DECIMAL(20,8)"))

    def askPriceAvg10 = column[Option[BigDecimal]]("ask_price_avg_10", O.SqlType("DECIMAL(20,8)"))

    def askPriceAvg25 = column[Option[BigDecimal]]("ask_price_avg_25", O.SqlType("DECIMAL(20,8)"))

    def askPriceAvg50 = column[Option[BigDecimal]]("ask_price_avg_50", O.SqlType("DECIMAL(20,8)"))

    def askPriceAvg100 = column[Option[BigDecimal]]("ask_price_avg_100", O.SqlType("DECIMAL(20,8)"))

    def askPriceAvg500 = column[Option[BigDecimal]]("ask_price_avg_500", O.SqlType("DECIMAL(20,8)"))

    def askPriceAvg1000 = column[Option[BigDecimal]]("ask_price_avg_1000", O.SqlType("DECIMAL(20,8)"))

    def askPriceAvg2500 = column[Option[BigDecimal]]("ask_price_avg_2500", O.SqlType("DECIMAL(20,8)"))

    def askPriceAvg5000 = column[Option[BigDecimal]]("ask_price_avg_5000", O.SqlType("DECIMAL(20,8)"))

    def askPriceAvg10000 = column[Option[BigDecimal]]("ask_price_avg_10000", O.SqlType("DECIMAL(20,8)"))

    def bidAmountSum5percent = column[BigDecimal]("bid_amount_sum_5percent", O.SqlType("DECIMAL(20,8)"))

    def bidAmountSum10percent = column[BigDecimal]("bid_amount_sum_10percent", O.SqlType("DECIMAL(20,8)"))

    def bidAmountSum25percent = column[BigDecimal]("bid_amount_sum_25percent", O.SqlType("DECIMAL(20,8)"))

    def bidAmountSum50percent = column[BigDecimal]("bid_amount_sum_50percent", O.SqlType("DECIMAL(20,8)"))

    def bidAmountSum75percent = column[BigDecimal]("bid_amount_sum_75percent", O.SqlType("DECIMAL(20,8)"))

    def bidAmountSum85percent = column[BigDecimal]("bid_amount_sum_85percent", O.SqlType("DECIMAL(20,8)"))

    def bidAmountSum100percent = column[BigDecimal]("bid_amount_sum_100percent", O.SqlType("DECIMAL(20,8)"))

    def askAmountSum5percent = column[BigDecimal]("ask_amount_sum_5percent", O.SqlType("DECIMAL(20,8)"))

    def askAmountSum10percent = column[BigDecimal]("ask_amount_sum_10percent", O.SqlType("DECIMAL(20,8)"))

    def askAmountSum25percent = column[BigDecimal]("ask_amount_sum_25percent", O.SqlType("DECIMAL(20,8)"))

    def askAmountSum50percent = column[BigDecimal]("ask_amount_sum_50percent", O.SqlType("DECIMAL(20,8)"))

    def askAmountSum75percent = column[BigDecimal]("ask_amount_sum_75percent", O.SqlType("DECIMAL(20,8)"))

    def askAmountSum85percent = column[BigDecimal]("ask_amount_sum_85percent", O.SqlType("DECIMAL(20,8)"))

    def askAmountSum100percent = column[BigDecimal]("ask_amount_sum_100percent", O.SqlType("DECIMAL(20,8)"))

    def askAmountSum200percent = column[BigDecimal]("ask_amount_sum_200percent", O.SqlType("DECIMAL(20,8)"))

    def loanOfferRateAvg1 = column[Option[BigDecimal]]("loan_offer_rate_avg_1", O.SqlType("DECIMAL(20,8)"))

    def loanOfferRateAvg10 = column[Option[BigDecimal]]("loan_offer_rate_avg_10", O.SqlType("DECIMAL(20,8)"))

    def loanOfferRateAvg25 = column[Option[BigDecimal]]("loan_offer_rate_avg_25", O.SqlType("DECIMAL(20,8)"))

    def loanOfferRateAvg50 = column[Option[BigDecimal]]("loan_offer_rate_avg_50", O.SqlType("DECIMAL(20,8)"))

    def loanOfferRateAvg100 = column[Option[BigDecimal]]("loan_offer_rate_avg_100", O.SqlType("DECIMAL(20,8)"))

    def loanOfferRateAvg500 = column[Option[BigDecimal]]("loan_offer_rate_avg_500", O.SqlType("DECIMAL(20,8)"))

    def loanOfferRateAvg1000 = column[Option[BigDecimal]]("loan_offer_rate_avg_1000", O.SqlType("DECIMAL(20,8)"))

    def loanOfferRateAvg2500 = column[Option[BigDecimal]]("loan_offer_rate_avg_2500", O.SqlType("DECIMAL(20,8)"))

    def loanOfferRateAvg5000 = column[Option[BigDecimal]]("loan_offer_rate_avg_5000", O.SqlType("DECIMAL(20,8)"))

    def loanOfferRateAvg10000 = column[Option[BigDecimal]]("loan_offer_rate_avg_10000", O.SqlType("DECIMAL(20,8)"))

    def loanOfferAmountSum = column[Option[BigDecimal]]("loan_offer_amount_sum", O.SqlType("DECIMAL(20,8)"))

    def idx1 = index("idx1", (timestamp, currencyPair), unique = true)

    def idx2 = index("idx2", timestamp)

    def idx3 = index("idx3", currencyPair)

    def idx4 = index("idx4", chartDataFinal)

    def * = {
      val tickGeneric = Generic[Tick]
      (id :: timestamp :: currencyPair :: open :: high :: low :: close :: volume :: chartDataFinal ::
        bidAskMidpoint :: bidPriceAvg1 :: bidPriceAvg10 :: bidPriceAvg25 :: bidPriceAvg50 ::
        bidPriceAvg100 :: bidPriceAvg500 :: bidPriceAvg1000 :: bidPriceAvg2500 :: bidPriceAvg5000 ::
        bidPriceAvg10000 :: askPriceAvg1 :: askPriceAvg10 :: askPriceAvg25 :: askPriceAvg50 :: askPriceAvg100 ::
        askPriceAvg500 :: askPriceAvg1000 :: askPriceAvg2500 :: askPriceAvg5000 :: askPriceAvg10000 ::
        bidAmountSum5percent :: bidAmountSum10percent :: bidAmountSum25percent :: bidAmountSum50percent ::
        bidAmountSum75percent :: bidAmountSum85percent :: bidAmountSum100percent ::
        askAmountSum5percent :: askAmountSum10percent :: askAmountSum25percent :: askAmountSum50percent ::
        askAmountSum75percent :: askAmountSum85percent :: askAmountSum100percent :: askAmountSum200percent ::
        loanOfferRateAvg1 :: loanOfferRateAvg10 :: loanOfferRateAvg25 :: loanOfferRateAvg50 :: loanOfferRateAvg100 ::
        loanOfferRateAvg500 :: loanOfferRateAvg1000 :: loanOfferRateAvg2500 :: loanOfferRateAvg5000 ::
        loanOfferRateAvg10000 :: loanOfferAmountSum :: HNil) <> (
        (dbRow: tickGeneric.Repr) => tickGeneric.from(dbRow),
        (caseClass: Tick) => Some(tickGeneric.to(caseClass))
        )
    }
  }

  val ticks = TableQuery[Ticks]
}