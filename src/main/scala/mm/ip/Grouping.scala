package mm.ip

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Grouping {
  val win1 = Window.orderBy(col("start"), col("end"))
  val win2 = Window.partitionBy(col("group_id"))

  def grouping(df: DataFrame): DataFrame={
    def iterate(df: DataFrame, groupCount: Long): DataFrame = {
      val res = df
        .withColumn("group_id", when(
          col("start").between(lag(col("start_min"), 1).over(win1), lag(col("end_max"), 1).over(win1)), null
        ).otherwise(monotonically_increasing_id)
        ).
        withColumn("group_id", last(col("group_id"), ignoreNulls=true).
          over(win1.rowsBetween(Window.unboundedPreceding, 0))
        ).
        withColumn("start_min", min(col("start")).over(win2)).
        withColumn("end_max", max(col("end")).over(win2)).
        orderBy( "start", "end")
      val curCount = res.select("group_id").distinct.count
      if (curCount == groupCount) {
        res
      } else {
        iterate(res, curCount)
      }
    }
    val transfrmed = df
      .withColumn("group_id", lit(0))
      .withColumn("start_min", col("start"))
      .withColumn("end_max", col("end"))
    iterate(transfrmed,transfrmed.select("group_id").distinct.count)
      .drop("start_min", "end_max")
  }
}

object Test extends App {
  import spark.implicits._
  import Grouping._
  val df = Seq((0,7),(2,3), (6,9), (8,10), (12,14)).toDF("start", "end")

  grouping(df).show(false)

}