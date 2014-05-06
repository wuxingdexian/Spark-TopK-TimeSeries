package spark.hdfs.topk

import java.util.Random
object LSH {
  // LSH 计算Array[String]的长度个数（维度）之后，将其拼接到一个串
  def LSHTranst(value: String): Array[Int] = {
    val backStrArr = new Array[Int](value.split(" ").length)
    for (i <- 0 to backStrArr.length - 1) {

      //for (i <- 0 to backStrArr.length) {
      //backStrArr(i) = timeSeriesLSHComputing(StringArr2DoubleArr(value.split(" ")), 4).toString()
      backStrArr(i) = timeSeriesLSHComputing(value.split(" ").map(_.toDouble), 4)
      //}
    }
    backStrArr
  }
  
  // 用一个时间序列开展一个LSH计算
  def timeSeriesLSHComputing(v: Array[Double], r: Int): Int = {
    val a = new Array[Double](v.length)
    val b = Math.random() * r
    for (i <- 0 to v.length - 1) {
      a(i) = new Random().nextGaussian()
    }
    if (a.length != v.length)
      0
    else {
      var sum = 0.0
      for (i <- 0 to v.length - 1) {
        sum += (a(i) * v(i))
      }
      ((sum + b) / r).toInt
    }
    
  }
  /*
   // Array[String]数组转换为Array[Double]数组
  def StringArr2DoubleArr(StrArr: Array[String]) = {
    // here are some codes that read strings from a file to the strArray
    val doubleArray: Array[Double] = new Array[Double](StrArr.length)
    var i = 0
    for (eachstr <- StrArr) { doubleArray(i) = eachstr.toDouble; i += 1 }
    doubleArray
  }
*/

}


