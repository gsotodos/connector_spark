package org.expand.tests;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.expand.hadoop.Expand;
import org.expand.spark.ExpandOutputFormat;
import org.expand.spark.ExpandInputFormat;

public final class testSparkExpand {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    JavaSparkContext sc = new JavaSparkContext(new SparkConf()
			.set("spark.hadoop.fs.defaultFS", "xpn:///")
			.set("spark.hadoop.fs.xpn.impl", "org.expand.hadoop.Expand"));

    SparkSession spark = SparkSession
      .builder()
      .appName("JavaWordCount")
      .getOrCreate();

    JavaPairRDD <LongWritable, Text> linespair = sc.newAPIHadoopFile(args[0],ExpandInputFormat.class,LongWritable.class,Text.class,sc.hadoopConfiguration());

    JavaRDD<String> lines = linespair.map(pair -> pair._2().toString());
    
    // lines.take(10);

    // Configuration xpnconf = sc.hadoopConfiguration();
    
    // long startSt1 = System.nanoTime();

    // JavaRDD<String> lines = sc.textFile(args[0],4096);

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<Text, IntWritable> counts = ones.reduceByKey((i1, i2) -> i1 + i2).mapToPair(pair -> new Tuple2<>(new Text(pair._1()), new IntWritable(pair._2())));

    // long endSt1 = System.nanoTime();

    // long startSt2 = System.nanoTime();

    // ExpandSparkFunctions.writeExpand(counts, args[1], sc.hadoopConfiguration());
    
    counts.saveAsTextFile(args[1]);

    // xpnconf.set("xpn.output.path", args[1]);
/*    
    counts.saveAsHadoopFile (
    args[1],
    Text.class,
    IntWritable.class,
    ExpandOutputFormat.class
    );
*/
    // long endSt2 = System.nanoTime();

    // System.out.println(counts.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(10));

    // System.out.println("TIEMPO DE EJECUCION DEL STAGE 1: " + (endSt1 - startSt1));
    // System.out.println("TIEMPO DE EJECUCION DEL STAGE 2: " + (endSt2 - startSt2));
    // System.out.println("TIEMPO DE EJECUCION TOTAL: " + (endSt2 - startSt1));

    spark.stop();
  }
}

