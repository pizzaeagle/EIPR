package mm.ip.functions

import mm.ip.contracts._
import mm.ip.spark
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object RangeFunctions {

  import spark.implicits._

  val calculateMutuallyExclusivePeriodsUDF: UserDefinedFunction = udf((data: Seq[Row]) => {
    val d = data.map { case Row(start: Long, end: Long) => IpRangeLong(start, end) }
    calculateMutuallyExclusivePeriods(d)
  })

  def calculateMutuallyExclusivePeriods(s: Seq[IpRangeLong]): Seq[IpRangeLong] = {
    val d = getExclusivePeriods(s)
    mutuallyExclusivePeriods(s, d)
  }

  def getExclusivePeriods(d: Seq[IpRangeLong]): Seq[IpRangeLong] = {
    d.foldLeft(Seq(): Seq[Position])((s, x) => s ++ Seq(Start(x.start), End(x.end)))
      .sortBy(_.value)
      .distinct
      .sliding(2)
      .map(
        x =>
          (x.head, x.last) match {
            case (Start(v), Start(v1)) => IpRangeLong(v, v1 - 1)
            case (End(v), Start(v1))   => IpRangeLong(v + 1, v1 - 1)
            case (Start(v), End(v1))   => IpRangeLong(v, v1)
            case (End(v), End(v1))     => IpRangeLong(v + 1, v1)
          }
      )
      .toSeq
  }

  def mutuallyExclusivePeriods(periods: Seq[IpRangeLong], exclusivePeriods: Seq[IpRangeLong]): Seq[IpRangeLong] = {
    exclusivePeriods
      .map(p => {
        val sum = periods
          .map(s => if ((p.start <= s.end && p.start >= s.start) || (p.end <= s.end && p.end >= s.start)) 1 else 0)
          .sum
        RangeCount(p, sum)
      })
      .filter(_.count == 1)
      .map(_.range)
  }

  def assignGroupIdsToIpRanges(ds: Dataset[IpRangeLong]): Dataset[GroupIpRangeLong] = {
    val win1 = Window.orderBy(col("start"), col("end"))
    val win2 = Window.partitionBy(col("groupID"))

    def iterate(ds: DataFrame, groupCount: Long): DataFrame = {
      val res = ds
        .withColumn(
          "groupID",
          when(
            col("start").between(lag(col("start_min"), 1).over(win1), lag(col("end_max"), 1).over(win1)),
            null
          ).otherwise(monotonically_increasing_id)
        )
        .withColumn(
          "groupID",
          last(col("groupID"), ignoreNulls = true).over(win1.rowsBetween(Window.unboundedPreceding, 0))
        )
        .withColumn("start_min", min(col("start")).over(win2))
        .withColumn("end_max", max(col("end")).over(win2))
        .orderBy("start", "end")
      val curCount = res.select("groupID").distinct.count
      if (curCount == groupCount) {
        res
      } else {
        iterate(res, curCount)
      }
    }

    val transformed = ds
      .withColumn("groupID", lit(0))
      .withColumn("start_min", col("start"))
      .withColumn("end_max", col("end"))
    iterate(transformed, transformed.select("groupID").distinct.count)
      .select(col("start"), col("end"), col("groupID"))
      .as[GroupIpRangeLong]
  }

  def aggregateGroups(ds: Dataset[GroupIpRangeLong]): Dataset[AggregatedRanges] = {
    ds.select(struct(col("start"), col("end")).as("period"), col("groupID"))
      .groupBy("groupID")
      .agg(collect_list(col("period")).as("periods"))
      .drop("groupID")
      .as[AggregatedRanges]
  }

  def calculateExclusiveIpRanges(ds: Dataset[AggregatedRanges]): Dataset[IpRangeLong] = {
    ds.withColumn("periods", calculateMutuallyExclusivePeriodsUDF(col("periods")))
      .select(explode(col("periods")).as("periods"))
      .select(col("periods.start"), col("periods.end"))
      .as[IpRangeLong]
  }
}
