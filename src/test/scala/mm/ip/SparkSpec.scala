package mm.ip

import org.apache.spark.sql.SparkSession

trait SparkSpec {

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()
}
