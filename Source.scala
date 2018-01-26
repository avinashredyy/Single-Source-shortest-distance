package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection._


object Source {
  def  lineParseFuncline (line :String):(Long, (Long,Long)) ={
    val a = line.split(",")
    (a(0).toLong, (a(1).toLong, a(2).toLong))
  }
  def assignInitaldistance(x:(Long,(Long,Long))):List[(Long,Long)] ={
    val pair1 = if (x._1 == 0L) (x._1, 0L) else (x._1, Long.MaxValue)
    val pair2 = if (x._2._2 == 0L) (x._2._2, 0L) else (x._2._2, Long.MaxValue)
    List(pair1, pair2)
  }
  def reduceFunc(kv1:(Long,Long), kv2:(Long,Long)):(Long,Long)  = {

    if (kv1._2 == kv2._2) if (kv1._1 < kv2._1) kv1 else kv2
    else if (kv1._2 < kv2._2){
      if ( kv2._2 != Long.MaxValue) {
        if ( (kv1._1 + kv1._2 )< (kv2._1 + kv2._2)) kv1 else kv2
      }
      else  kv1
    }
    else {
      if ( kv1._2 != Long.MaxValue) {
        if (kv1._1 + kv1._2 < kv2._1 + kv2._2) kv1 else kv2
      }
      else  kv2
    }
  }

  def main(args :Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ShortestPath")
    val sc = new SparkContext(conf)

    val inputRDD = sc.textFile(args(0)).map { lineParseFuncline }
    var distanceRDD = inputRDD.flatMap(assignInitaldistance) .distinct()
    for ( index <- 1 to 4 ) {
      val vertexDistanceRDD=  inputRDD.join(distanceRDD)
      val invertedRDD =vertexDistanceRDD.map(attr => (attr._2._1._2,(attr._2._1._1,attr._2._2)))
      val computedDistBetweenVertRDD =invertedRDD.reduceByKey(reduceFunc )
      val joinedRDD = computedDistBetweenVertRDD.join(distanceRDD)

      distanceRDD =joinedRDD.map(kv => {
        if (kv._2._2 == Long.MaxValue )(kv._1,kv._2._1._1 + kv._2._1._2)
        else if (kv._2._1._2 == Long.MaxValue) (kv._1,kv._2._2)
        else if (kv._2._2 > kv._2._1._1 + kv._2._1._2) (kv._1,kv._2._1._1 + kv._2._1._2) else (kv._1,kv._2._2)
      })

    }
    println(distanceRDD.collect().mkString("\n"))
  }
}

