package mm.ip

import mm.ip.contracts.IpRangeString
import org.scalatest.{FlatSpec, Matchers}

class ApplicationTest extends FlatSpec with Matchers with SparkSpec {
  import spark.implicits._

  "Application.run" should "return same data as in execercise description" in {
    val inputDS = Seq(
      IpRangeString("197.203.0.0", "197.206.9.255"),
      IpRangeString("197.204.0.0", "197.204.0.24"),
      IpRangeString("201.233.7.160", "201.233.7.168"),
      IpRangeString("201.233.7.164", "201.233.7.168"),
      IpRangeString("201.233.7.167", "201.233.7.167"),
      IpRangeString("203.133.0.0", "203.133.255.255")
    ).toDS
    val expectedResults = Seq(
      IpRangeString("197.203.0.0", "197.203.255.255"),
      IpRangeString("197.204.0.25", "197.206.9.255"),
      IpRangeString("201.233.7.160", "201.233.7.163"),
      IpRangeString("203.133.0.0", "203.133.255.255")
    )

    val results = Application.run(inputDS).get
    results.collect should contain theSameElementsAs expectedResults
  }
}
