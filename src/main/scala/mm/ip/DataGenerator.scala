package mm.ip

import com.typesafe.config.ConfigFactory
import mm.ip.contracts.IpRangeLong
import mm.ip.functions.IpConverter

import scala.util.{Failure, Success, Try}

object DataGenerator extends App {
  import spark.implicits._

  Try {
    val config = Configuration(ConfigFactory.load("config.properties"))
    val generatedRangesDS = DataGeneratorUtil.generateOverlappingRanges(config.numberOfGeneratedRanges).toDS
    IpConverter
      .convertLongToIp(generatedRangesDS)
      .write
      .mode("overwrite")
      .format("jdbc")
      .option("url", config.sqlUrl)
      .option("dbtable", config.sqlDBTable)
      .option("user", config.user)
      .option("driver", config.sqlDriver)
      .option("password", config.password)
      .save()
    generatedRangesDS.count
  } match {
    case Success(count)     => println(count)
    case Failure(exception) => throw exception
  }
}

object DataGeneratorUtil {
  val MAXNUMBER = 4294967295L
  val r = scala.util.Random
  def generateOverlappingRanges(numberOfRanges: Int): Seq[IpRangeLong] = {
    (1 to numberOfRanges).flatMap(_ => {
      val start_one = math.abs(r.nextLong()) % MAXNUMBER
      val start_two = start_one + r.nextInt(255)
      val end_one = start_two + r.nextInt(255)
      val end_two = end_one + r.nextInt(255)
      Seq(IpRangeLong(start_one, start_two), IpRangeLong(end_one, end_two))
    })
  }
}
