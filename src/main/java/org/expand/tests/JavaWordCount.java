/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.expand.tests;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    JavaSparkContext sc = new JavaSparkContext(new SparkConf());

    SparkSession spark = SparkSession
      .builder()
      .appName("JavaWordCount")
      .getOrCreate();

    long startSt1 = System.nanoTime();

    JavaRDD<String> lines = sc.textFile(args[0],8192);

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

    // List<Tuple2<String, Integer>> output = counts.collect();

    long endSt1 = System.nanoTime();

    long startSt2 = System.nanoTime();

    counts.saveAsTextFile(args[1]);

    long endSt2 = System.nanoTime();

    // System.out.println(counts.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(10));

    System.out.println("TIEMPO DE EJECUCION DEL STAGE 1: " + (endSt1 - startSt1));
    System.out.println("TIEMPO DE EJECUCION DEL STAGE 2: " + (endSt2 - startSt2));
    System.out.println("TIEMPO DE EJECUCION TOTAL: " + (endSt2 - startSt1));

    spark.stop();
  }
}
