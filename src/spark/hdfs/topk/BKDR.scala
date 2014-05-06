package spark.hdfs.topk

object BKDR {
  // 对字符串的BKDRHash
  def BKDRHash(Coordinate: Array[Int], bucketNum: Int): Int = {
    var sum = 0
    val tmpStr = Coordinate.map(_.toString).flatten
    for (i <- 0 to tmpStr.length() - 1) {
      sum += sum * 31 + tmpStr.charAt(i)
    }
    
    sum % bucketNum // 取多少个桶
  }
}