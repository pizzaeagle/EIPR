package mm.ip

import com.typesafe.config.ConfigFactory
import mm.ip.contracts.IpRangeString
import org.elasticsearch.spark.sql._

import scala.util.{Failure, Success}

object Main extends App {
  import spark.implicits._
  val config = Configuration(ConfigFactory.load("config.properties"))
  val inputDF = spark.read
    .format("jdbc")
    .option("url", config.sqlUrl)
    .option("dbtable", config.sqlDBTable)
    .option("user", config.user)
    .option("password", config.password)
    .option("driver", config.sqlDriver)
    .load()
    .as[IpRangeString]

  val results = Application.run(inputDF)

  results match {
    case Success(finalDS) =>
      finalDS.saveToEs(config.esIndex, Map("es.nodes" -> config.esNodes, "es.nodes.wan.only" -> "true"))
    case Failure(exception) => throw exception
  }
}
