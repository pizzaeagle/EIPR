package mm.ip.functions

import mm.ip.contracts.{IpRangeLong, IpRangeString}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import mm.ip.spark

object IpConverter {
  import spark.implicits._

  def convertIpToLong(ds: Dataset[IpRangeString]): Dataset[IpRangeLong] = {
    ds.withColumn("start", split(col("start"), "\\."))
      .withColumn(
        "start",
        col("start")(0).cast(LongType) * lit(scala.math.pow(2, 8 * 3)).cast(LongType) +
          col("start")(1).cast(LongType) * lit(scala.math.pow(2, 8 * 2)).cast(LongType) +
          col("start")(2).cast(LongType) * lit(scala.math.pow(2, 8 * 1)).cast(LongType) +
          col("start")(3).cast(LongType) * lit(scala.math.pow(2, 8 * 0)).cast(LongType)
      )
      .withColumn("end", split(col("end"), "\\."))
      .withColumn(
        "end",
        col("end")(0).cast(LongType) * lit(scala.math.pow(2, 8 * 3)).cast(LongType) +
          col("end")(1).cast(LongType) * lit(scala.math.pow(2, 8 * 2)).cast(LongType) +
          col("end")(2).cast(LongType) * lit(scala.math.pow(2, 8 * 1)).cast(LongType) +
          col("end")(3).cast(LongType) * lit(scala.math.pow(2, 8 * 0)).cast(LongType)
      )
      .as[IpRangeLong]
  }

  def convertLongToIp(ds: Dataset[IpRangeLong]): Dataset[IpRangeString] = {
    ds.withColumn(
        "start",
        concat_ws(
          ".",
          (shiftRight(col("start"), 8 * 3) % 256).cast("string"),
          (shiftRight(col("start"), 8 * 2) % 256).cast("string"),
          (shiftRight(col("start"), 8) % 256).cast("string"),
          (col("start") % 256).cast("string")
        )
      )
      .withColumn(
        "end",
        concat_ws(
          ".",
          (shiftRight(col("end"), 8 * 3) % 256).cast("string"),
          (shiftRight(col("end"), 8 * 2) % 256).cast("string"),
          (shiftRight(col("end"), 8) % 256).cast("string"),
          (col("end") % 256).cast("string")
        )
      )
      .as[IpRangeString]
  }
}
