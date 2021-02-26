package mm.ip

import mm.ip.contracts.IpRangeString
import mm.ip.functions.IpConverter.{convertIpToLong, convertLongToIp}
import mm.ip.functions.RangeFunctions.{aggregateGroups, assignGroupIdsToIpRanges, calculateExclusiveIpRanges}
import org.apache.spark.sql.Dataset

import scala.util.Try

object Application {
  def run(inputDF: Dataset[IpRangeString]): Try[Dataset[IpRangeString]] = {
    Try {
      val ipLongDF = convertIpToLong(inputDF)
      val ipLongWithGroupIDsDF = assignGroupIdsToIpRanges(ipLongDF)
      val aggregatedIpRangesDS = aggregateGroups(ipLongWithGroupIDsDF)
      val exclusiveRangesDS = calculateExclusiveIpRanges(aggregatedIpRangesDS)
      convertLongToIp(exclusiveRangesDS)
    }
  }
}
