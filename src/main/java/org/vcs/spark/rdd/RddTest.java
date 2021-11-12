package org.vcs.spark.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RddTest {
    public static void main(String[] args) throws InterruptedException {
        //
        // The "modern" way to initialize Spark is to create a SparkSession
        // although they really come from the world of Spark SQL, and Dataset
        // and DataFrame.
        //
        SparkSession spark = SparkSession
                .builder()
                .appName("RDD-Basic")
                .master("local")
                .getOrCreate();

        //
        // Operating on a raw RDD actually requires access to the more low
        // level SparkContext -- get the special Java version for convenience
        //
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> textRdd=sc.textFile("/Users/vivekkumar/Documents/sparksample.txt",1);
        JavaPairRDD<String,Integer> pairRDD=textRdd.map(s->s.split(" ")).mapToPair(s->new Tuple2(s,1)).reduceByKey((a,b)->(int)a+(int)b);
        final List<Tuple2<String, Integer>> output = pairRDD.collect();

       Broadcast<Integer> b1=sc.broadcast(3);

        int s= b1.value();
        System.out.println(s);
        b1.destroy();
        spark.stop();
    }
}