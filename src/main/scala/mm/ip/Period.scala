package mm.ip

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{array, array_union, arrays_overlap, col, collect_list, size, struct, udf}


trait Pos{
  val value:Long
}
case class Start(value:Long) extends Pos
case class End(value:Long) extends Pos

case class Period(start:Long, end:Long)

case class PeriodCounts(period:Period, count: Int)

object Period{
  val  calculateMutuallyExclusivePeriodsUDF = udf((data: Seq[Row]) => {
    val d = data.map{case Row(start:Long, end:Long) => Period(start, end)}
    calculateMutuallyExclusivePeriods(d)
  })

  def calculateMutuallyExclusivePeriods(s: Seq[Period]): Seq[Period] = {
    val d = getExclusivePeriods(s)
    mutuallyExclusivePeriods(s, d)
  }

  def getExclusivePeriods(d: Seq[Period]): Seq[Period] = {
    d.foldLeft(Seq(): Seq[Pos])((s,x) => s ++ Seq(Start(x.start), End(x.end))).sortBy(_.value)
      .distinct.sliding(2)
      .map(x => (x.head, x.last) match {
        case (Start(v), Start(v1)) => Period(v, v1-1)
        case (End(v), Start(v1)) => Period(v +1 , v1-1)
        case (Start(v), End(v1)) => Period(v , v1)
        case (End(v), End(v1)) => Period(v +1 , v1)
      }).toSeq
  }

  def mutuallyExclusivePeriods(periods: Seq[Period], exclusivePeriods: Seq[Period]): Seq[Period] = {
    exclusivePeriods.map(p => {
      val sum = periods.map(s => if ((p.start <= s.end && p.start >= s.start) || (p.end <= s.end && p.end >= s.start))  1 else 0).sum
      PeriodCounts(p,sum)
    }).filter(_.count == 1).map(_.period)
  }

  object Test2 extends App {
    val x = Seq(Period(2,3),
      Period(6,9),
      Period(0,7)).sortBy(_.start)

    val d = getExclusivePeriods(x)

    mutuallyExclusivePeriods(x, d).foreach(println)

  }

}