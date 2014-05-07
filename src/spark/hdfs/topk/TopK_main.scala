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

object topk_main {

  /**
   * 直接将桶的两两结合后（key，value）
   * key为经过hash后值的组合，一般取模5000,如： （100,-1343）
   * value为数据点的序列（Int数组形式），每个点以各个维度值表示，如三个二维点序列：（8 45 1, -3 88 900, -1213 42 0）
   * 其(key,value)：（（100,-1343），（8 45 1, -3 88 900, -1213 42 0））
   * 
   */
  def main(args: Array[String]): Unit = {
    
    val starttime = System.currentTimeMillis()
    //System.out.println("启动计算：" + new Date(starttime));//方法一：默认方式输出现在时间

    val sc = new SparkContext(args(0), "hash1",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_TEST_JAR")))
    val file = sc.textFile(args(1))
    
    // 从每一行记录提取记录
    val line = file.flatMap(_.split("\n")).map(l => (l.split("#").head, l.split("#").tail.head)) //.foreach(println)  //.foreach(println)

    // 进行LSH
    val LSH_calculate = line.map { case (key, value: String) => 
      (value.split(" ").map(_.toInt), LSH.LSHTranst(value)) }// value.split(" ").map(_.toInt)
    println("LSH计算完毕！总共有记录行数：" + LSH_calculate.count)
    val LSHtime = System.currentTimeMillis()
    //System.out.println("现在时间：" + new Date(LSHtime));
    
    // 进行BKDRHash  这里控制桶个数，以模多少方式进行
    val BKDR_hash = LSH_calculate.map { case (key, value) => (BKDR.BKDRHash(value, 5000), key) }
    // BKDR的hash值相同的点归到同一个桶
    val BKDR_group = BKDR_hash.groupByKey
    println("桶生成完毕！总共有桶：" + BKDR_group.count)
    val BKDRtime = System.currentTimeMillis()
    //System.out.println("现在时间：" + new Date(BKDRtime));//方法一：默认方式输出现在时间
    
    // 求两个桶的笛卡尔积（全连接）  然后过滤掉桶hash值相同的记录
    val bucket_combine = BKDR_group.cartesian(BKDR_group).filter{case (key,value) => (key._1 > value._1)}
    println("桶的笛卡尔积（实际已经完成了桶合并）！总共有桶：" + bucket_combine.count)
    val buckettime = System.currentTimeMillis()
    //System.out.println("现在时间：" + new Date(buckettime))
    //bucket_combine.cache
    
    // 计算局部top-k 取top-80 
    val Topk_caculate = bucket_combine.map{case(key,value) =>
      ((key._1,value._1),shen.top_k.CallTop_k.pointPair((key._2 ++ value._2).toArray,80))}
    val distance = Topk_caculate.map{case (key,value) => (shen.top_k.Top_KD.DistanceOfTwoPoint(value(0),value(1)),value)}
    //val topk_globle = distance.sortByKey(true, 1)
    
    //val GlobalTopK_caculate = distance.sortByKey(true, 1)
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
    
    // 增加排序，取全局的top-k
    
    println("Top-k计算完毕！总共行数：" + distance.count)
    val topktime = System.currentTimeMillis()
    //System.out.println("结束时间：" + new Date(topktime) + " " + topktime);//方法一：默认方式输出现在时间
    
    println("启动时间——LSH完成时间：" + (LSHtime- starttime) / 1000 )
    println("LSH完成时间——BKDR桶完成时间：" + (BKDRtime- LSHtime) / 1000 )
    println("BKDR桶完成时间——buck合并完成时间：" + (buckettime- BKDRtime) / 1000 + " 行数：" + LSH_calculate.count + " 桶数：" + BKDR_group.count + " 桶组合数：" + bucket_combine.count)
    println("buck合并完成时间——topk完成时间：" + (topktime- buckettime) / 1000)
    println("总共完成时间：" + (topktime - starttime) / 1000)
    
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

