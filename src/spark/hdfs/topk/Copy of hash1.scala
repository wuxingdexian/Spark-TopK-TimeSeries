package spark.hdfs.topk

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import SparkContext._
import java.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import shen.top_k
import java.util.Date

object hash1_back {

  /**
   * 直接将桶的两两结合后（key，value）
   * key为经过hash后值的组合，一般取模5000,如： （100,-1343）
   * value为数据点的序列（Int数组形式），每个点以各个维度值表示，如三个二维点序列：（8 45 1, -3 88 900, -1213 42 0）
   * 其(key,value)：（（100,-1343），（8 45 1, -3 88 900, -1213 42 0））
   * 
   */
  def main(args: Array[String]): Unit = {
    
    System.out.println("启动计算：" + new Date(System.currentTimeMillis()));//方法一：默认方式输出现在时间

    val sc = new SparkContext(args(0), "hash1",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_TEST_JAR")))
    val file = sc.textFile(args(1))
    
    // 从每一行记录提取记录
    val line = file.flatMap(_.split("\n")).map(l => (l.split("#").head, l.split("#").tail.head)) //.foreach(println)  //.foreach(println)

    // 进行LSH
    val LSH_calculate = line.map { case (key, value: String) => 
      (value.split(" ").map(_.toInt), LSH.LSHTranst(value)) }// value.split(" ").map(_.toInt)
    println("LSH计算完毕！总共有记录行数：" + line.count)
    System.out.println("现在时间：" + new Date(System.currentTimeMillis()));
    
    // 进行BKDRHash
    val BKDR_hash = LSH_calculate.map { case (key, value) => (BKDR.BKDRHash(value, 5000), key) }
    
    // BKDR的hash值相同的点归到同一个桶
    val BKDR_group = BKDR_hash.groupByKey
    println("桶生成完毕！总共有桶：" + BKDR_group.count)
    System.out.println("现在时间：" + new Date(System.currentTimeMillis()));//方法一：默认方式输出现在时间
    
    // 求两个桶的笛卡尔积（全连接）  然后过滤掉桶hash值相同的记录
    val bucket_combine01 = BKDR_group.cartesian(BKDR_group).filter{case (key,value) => (key._1 > value._1)}

    // 将桶的hash值组合，并作为新的key
    val bucket_combine02 = bucket_combine01.map{case(key,value) => 
      if(key._1 > value._1)((key._1, value._1),(key._2 ++ value._2)) 
      else((value._1, key._1),(key._2 ++ value._2))}
    bucket_combine02.saveAsTextFile("outputBuck2")
    /*
    // 去掉桶的hash值组合出现重复的情况
    val bucket_combine03 = bucket_combine02.reduceByKey((value1,value2) => (value1))
    bucket_combine03.saveAsTextFile("outputBuck1")
    */
    println("桶合并完毕！总共有行数：" + bucket_combine02.count)
    System.out.println("现在时间：" + new Date(System.currentTimeMillis()));//方法一：默认方式输出现在时间
    
    // 计算top-k 取top-80
    val Topk_caculate = bucket_combine02.map{case(key,value:Seq[Array[Int]]) => 
      (key,shen.top_k.CallTop_k.pointPair(value.toArray, 80))}
    //Topk_caculate.cache
    val distance = Topk_caculate.map{case (key,value) => (shen.top_k.Top_KD.DistanceOfTwoPoint(value(0),value(1)),value)}
    
    /*
    // 输出所有结果 globle
    for(a <- distance){//.map{case(key,value) => (value)}
      println(a._1 + " ")
      for(b <- a._2){
        //print(a._1 + " ")
        for(c <- b){
          print(c + " ")
        }
        println
      }
      
    }*/
    //println(testTop_k.count)
    println("Top-k计算完毕！总共行数：" + distance.count)
    System.out.println("结束时间：" + new Date(System.currentTimeMillis()));//方法一：默认方式输出现在时间
    
    /* 测试调用java所写的top-k
    val matrix = Array.ofDim[Int](3, 4) // 三行，四列
    val topk = shen.top_k.CallTop_k.pointPair(matrix)
    topk.foreach(_.foreach(print))
	*/
    /*
     * 怎么打印出来的keyvalHash2_1和keyvalHash2_2,原来的数据点对应的BKDRHash会不一样呢？？？
     * 至此，上面步骤已经生成了每个时间序列的多维点的LSH（针对点的每个维度进行生成），然后拼接为一个串，再对这个串进行BKDRHash，然后合并相同BKDRHash值的点。
     * 下面步骤应该是spark stream登场的时候到了。包括桶的两两组合，然后对组合桶展开top-k计算
     *
     */ 
    //val BKDRGroupBucketStream = new StreamingContext(keyvalBKDRGroupBucket,Seconds(1))
    
    
  }
  
  

}
  
 

//}
/**
 * case 在RDD里面的作用？
 * 
 */

