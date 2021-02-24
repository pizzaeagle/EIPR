package mm.ip

import mm.ip.Grouping.grouping
import mm.ip.Period.calculateMutuallyExclusivePeriodsUDF
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.elasticsearch.spark.sql._

object Main extends App{
  import spark.implicits._

  val df = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306")
    .option("dbtable", "test_db.ips_2")
    .option("user", "root")
    .option("password", "root")
    .load()
    .toDF("start","end")
    .withColumn("start_long_ip", lit(0L))
    .withColumn("end_long_ip", lit(0L))

  val result = (0 to 3).foldLeft(df)((df,i) => {
    df
      .withColumn("start_long_ip", col("start_long_ip") + split(col("start"), "\\.")(i).cast(LongType)  * lit(scala.math.pow(2, 8 * (3 - i))).cast(LongType))
      .withColumn("end_long_ip", col("end_long_ip")  + split(col("end"), "\\.")(i).cast(LongType)  * lit(scala.math.pow(2, 8 * (3 - i))).cast(LongType))
  })
    .drop("start", "end").toDF("start","end")

  val x = grouping(result)
    .select(struct(col("start"),col("end")).as("period"),
    col("group_id"))
    .groupBy("group_id")
    .agg(collect_list(col("period")).as("periods"))
    .drop("group_id")
    .as[Periods]
    .withColumn("periods", calculateMutuallyExclusivePeriodsUDF(col("periods")))
      .select(explode(col("periods")).as("periods"))
      .select(col("periods.start"),
        col("periods.end"))

  x.withColumn(
    "start_ip",
    concat_ws(
      ".",
      (shiftRight(col("start"), 8*3) % 256).cast("string"),
      (shiftRight(col("start"), 8*2) % 256).cast("string"),
      (shiftRight(col("start"), 8) % 256).cast("string"),
      (col("start") % 256).cast("string")
    )
  ).withColumn(
    "end_ip",
    concat_ws(
      ".",
      (shiftRight(col("end"), 8*3) % 256).cast("string"),
      (shiftRight(col("end"), 8*2) % 256).cast("string"),
      (shiftRight(col("end"), 8) % 256).cast("string"),
      (col("end") % 256).cast("string")
    )
  )
    .select("start_ip", "end_ip")
    .saveToEs("spark/ips", Map("es.nodes" -> "localhost",  "es.nodes.wan.only" -> "true"))
}

case class Periods(periods: Seq[Period])



// check range for first digit