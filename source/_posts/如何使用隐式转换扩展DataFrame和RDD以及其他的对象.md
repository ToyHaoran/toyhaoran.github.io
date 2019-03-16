---
title: 如何使用隐式转换扩展DataFrame和RDD以及其他的对象
date: 2018-11-30 21:07:57
tags: spark
categories: spark
---

## 目的

DataFrame可以点出来很多方法，都是DF内置的。

比如说：df.withColumn()，df.printSchema()。

但是如果你想打印df中的分区位置信息，以及每个key有多少记录。怎么才能点出来这个方法呢？

## 实现工具类

``` scala
package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

//导入对应的规则类，以免出现警告
import scala.language.implicitConversions

object BaseUtil {
    /**
      * DF的装饰类（隐式转换）
      */
    class RichDataFrame(dataFrame: DataFrame){
        /**
          * 用来统计相同key的记录数，常用于调整数据倾斜
          */
        def printKeyNums(column: Column): Unit ={
            val map = dataFrame.select(column).rdd.countByValue()
            println(s"一共${map.size}个key")
            for ((key, num) <- map) {
                println(key + "共有" + num + "条记录")
            }
        }
        def printKeyNums(column: String): Unit ={
            printKeyNums(dataFrame.col(column))
        }
        /**
          * 打印分区位置信息
          */
        def printLocation(): Unit ={
            println("分区位置信息如下==============")
            dataFrame.rdd.mapPartitionsWithIndex(printLocationFunc).collect().foreach(println(_))
        }
    }

     /**
      * 扩展df的方法，隐式转换
      */
    implicit def df2RichDF(src: DataFrame): RichDataFrame = new RichDataFrame(src)

    /**
      * RDD的装饰类（隐式转换）,不加泛型读取不到
      */
    class RichRDD(rdd:RDD[_ <: Any]){
        def printLocation(): Unit ={
            println("分区位置信息如下==============")
            rdd.mapPartitionsWithIndex(printLocationFunc).collect().foreach(println(_))
        }
    }

    /**
      * 扩展RDD的方法，隐式转换
      */
    implicit def rdd2RichRDD(src: RDD[_ <: Any]): RichRDD = new RichRDD(src)

    /**
      * 打印rdd的分区信息，需要用mapPartitionsWithIndex方法。
      * 使用方法：df.rdd.mapPartitionsWithIndex(printLocationFunc).collect().foreach(println(_))
      */
    def printLocationFunc(index: Int, iter: Iterator[Any]): Iterator[String] = {
        iter.map(x => "分区" + index + "：" + x + "")
    }
}
```



## 测试工具类

``` scala
import utils.BaseUtil._

object DataFrameDemo extends App {
    val sc = ConnectUtil.sc
    val spark = ConnectUtil.spark
    import spark.implicits._
    val df = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("bbb", 1, 5), ("bbb", 2, 1), ("ccc", 4, 5), ("bbb", 4, 6))).toDF("key1", "key2", "key3")
    
    //测试
    df.printKeyNums("key1")
    df.printKeyNums($"key1")
    df.printLocation()
    df.rdd.printLocation()
}
```





