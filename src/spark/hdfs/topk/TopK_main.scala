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
import org.apache.spark.storage.StorageLevel
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class MyRegistrator extends KryoRegistrator {
        override def registerClasses(kryo: Kryo) {
            kryo.register(classOf[org.apache.spark.rdd.RDD[(String, String)]]) // line
            kryo.register(classOf[org.apache.spark.rdd.RDD[(Array[Int], Array[Int])]]) // LSH_calculate
            kryo.register(classOf[org.apache.spark.rdd.RDD[(Int, Array[Int])]]) // BKDR_hash
            kryo.register(classOf[org.apache.spark.rdd.RDD[(Int, Seq[Array[Int]])]])  // BKDR_group
            kryo.register(classOf[org.apache.spark.rdd.RDD[((Int, Seq[Array[Int]]), (Int, Seq[Array[Int]]))]]) // bucket_combine
            kryo.register(classOf[org.apache.spark.rdd.RDD[((Int, Int), Array[Array[Int]])]]) // Topk_caculate
            kryo.register(classOf[org.apache.spark.rdd.RDD[(Double, Array[Array[Int]])]]) // distance
            
        }
    }

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

    // 控制Spark中通信消息的最大容量 （如 task 的输出结果），默认为10M。
    //当处理大数据时，task 的输出可能会大于这个值，需要根据实际数据设置一个更高的值。
    //System.setProperty("spark.akka.frameSize", "100") 
    //控制用于 Spark 缓存的 Java 堆空间，默认值是0.67，即 2/3 的 Java 堆空间用于 Spark 的缓存。
    //如果任务的计算过程中需要用到较多的内存，而 RDD 所需内存较少，可以调低这个值，以减少计算过程中因为内存不足而产生的 GC 过程。
    System.setProperty("spark.storage.memoryFraction", "0.01") 
    
    //System.setProperty("spark.shuffle.memoryFraction","0.5") // 如果shuffle频繁，可以考虑提升比值
    //System.setProperty("spark.shuffle.file.buffer.kb","1000") // shuffle的buffer的大小 kb 默认10m
    //System.setProperty("spark.shuffle.consolidateFiles","true")
    
    System.setProperty("spark.executor.memory","1280m")
    System.setProperty("spark.cores.max","2")
    
    System.setProperty("spark.local.dir","/home/dhu/sparkTmp")
    
    // 序列化 kryo  不知为何  使用kryo序列化，会出现内存溢出的情况
    //System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //System.setProperty("spark.kryo.registrator", "spark.hdfs.topk.MyRegistrator")
    //System.setProperty("spark.kryoserializer.buffer.mb","10")
    val sc = new SparkContext(args(0), "hash1",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_TEST_JAR")))
    //val file = sc.textFile(args(1))
    val file = sc.textFile(args(1))
    
    // 从每一行记录提取记录
    val line = file.flatMap(_.split("\n")).map(l => (l.split("#").head, l.split("#").tail.head)) //.foreach(println)  //.foreach(println)
    
    // 进行LSH
    val LSH_calculate = line.map { case (key, value: String) => 
      (value.split(" ").map(_.toInt), LSH.LSHTranst(value)) }// value.split(" ").map(_.toInt)
    //println("LSH计算完毕！总共有记录行数：" + LSH_calculate.count)
    val LSHtime = System.currentTimeMillis()
    //System.out.println("现在时间：" + new Date(LSHtime));
    
    // 进行BKDRHash  这里控制桶个数，以模多少方式进行
    val BKDR_hash = LSH_calculate.map { case (key, value) => (BKDR.BKDRHash(value, 5000), key) }
    //BKDR_hash.persist(StorageLevel.MEMORY_AND_DISK)
    // BKDR的hash值相同的点归到同一个桶
    val BKDR_group = BKDR_hash.groupByKey
    //println("桶生成完毕！总共有桶：" + BKDR_group.count)
    val BKDRtime = System.currentTimeMillis()
    //System.out.println("现在时间：" + new Date(BKDRtime));//方法一：默认方式输出现在时间
    // 设置存储级别
    BKDR_group.persist(StorageLevel.MEMORY_AND_DISK)
    
    // 求两个桶的笛卡尔积（全连接）  然后过滤掉桶hash值相同的记录
    val bucket_combine = BKDR_group.cartesian(BKDR_group).filter{case (key,value) => (key._1 > value._1)}
    //println("桶的笛卡尔积（实际已经完成了桶合并）！总共有桶：" + bucket_combine.count)
    // 设置存储级别
    bucket_combine.persist(StorageLevel.MEMORY_AND_DISK)
    val buckettime = System.currentTimeMillis()
    //System.out.println("现在时间：" + new Date(buckettime))
    //bucket_combine.saveAsTextFile("3k-10.txt")
    
    
    // 计算局部top-k 取top-80 
    val Topk_caculate = bucket_combine.map{case(key,value) =>
      ((key._1,value._1),shen.top_k.CallTop_k.pointPair((key._2 ++ value._2).toArray,20))}
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
    
    // 增加排序，取全局的top-k
    //distance.sortByKey(true, 1).saveAsTextFile("hdfs://dhu3:9000/top-k/output")
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

