package mm

import org.apache.spark.sql.SparkSession

package object ip {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local") //todo change
    .config("es.index.auto.create", "true")
    .getOrCreate()
}
