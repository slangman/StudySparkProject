import org.apache.spark.{SparkConf, SparkContext}
import java.lang.Double.isNaN

object StudySpark {
  def main(args: Array[String]): Unit = {
    val sc = initializeSparkContext()
    if (sc != null) {
      println("SparkContext initialized successfully")
    }
    val rawblocks = sc.textFile("c:\\linkage")
    val head = rawblocks.take(10)
    val noheader = rawblocks.filter(x => !isHeader(x))
    val mds = head.filter(x => !isHeader(x)).map(x => parse(x))
    val parsed = noheader.map(line => parse(line))
    parsed.cache()
    parsed.take(10).foreach(e => println(e))

    //val grouped = mds.groupBy(md => md.matched)
    //grouped.mapValues(x => x.size).foreach(println)

    //val matchCounts = parsed.map(md => md.matched).countByValue()
    //val matchCountsSeq = matchCounts.toSeq

    //matchCountsSeq.sortBy(_._1).foreach(println)

    //val stats = parsed.map(md => md.scores(0)).filter(!isNaN(_)).stats()

    //println(stats)

    val nasRDD = parsed.map(md => {
      md.scores.map(d => NAStatCounter(d))
    })

    val reduced = nasRDD.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
    reduced.foreach(println)

  }

  def initializeSparkContext(): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    new SparkContext(conf)
  }

  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  }

  def toDouble(s: String) = {
    if ("?".equals(s)) Double.NaN else s.toDouble
  }

  case class MatchData(id1: Int, id2: Int,
                       scores: Array[Double], matched: Boolean)

  def parse(line: String) = {
    val pieces = line.split(',')
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2, 11).map(toDouble)
    val matched = pieces(11).toBoolean
    MatchData(id1, id2, scores, matched)
  }

}
