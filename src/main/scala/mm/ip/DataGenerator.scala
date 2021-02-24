package mm.ip

object DataGenerator extends App{
  import spark.implicits._
  val MAXNUMBER = 4294967295L
  val r = scala.util.Random
  def generateOverlappingRanges(): Seq[Period] = {
    val start_one = math.abs(r.nextLong()) % MAXNUMBER
    val start_two = start_one + r.nextInt(255)
    val end_one = start_two + r.nextInt(255)
    val end_two = end_one + r.nextInt(255)
    Seq(Period(start_one, start_two), Period(end_one, end_two))
  }

  val x = (1 to 10).flatMap(_ => generateOverlappingRanges)
  IpConverter.convertLongToIp(x.toDS)
    .write
    .mode("overwrite")
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306")
    .option("dbtable", "test_db.ips_2")
    .option("user", "root")
    .option("password", "root")
    .save()

}
