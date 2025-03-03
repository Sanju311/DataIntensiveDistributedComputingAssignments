package ca.uwaterloo.cs451.a2
/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */


import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

class FirstWordPartitioner(override val numPartitions: Int ) extends Partitioner {
  
  def getPartition(key: Any): Int = key match{
    case (w1: String, _) => (w1.hashCode() & Integer.MAX_VALUE) % numPartitions
    case _ => 0
  }
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("BigramRelativeFrequencyPairs")
    val sc = new SparkContext(conf)

    val reducers = args.reducers()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    //computer bigram and word counts simulataneously
    val bigram_counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) 
          tokens.sliding(2).flatMap({ case Seq(w1, w2) => List(((w1, w2), 1), ((w1, "*"), 1)) })
        else
          List()
      })

    val partitioned_bigram_counts = bigram_counts.partitionBy(new FirstWordPartitioner(reducers))

    val reduced_bigram_counts = partitioned_bigram_counts
      .reduceByKey(new FirstWordPartitioner(reducers), _ + _) 
      .mapPartitions(iter => iter.toList.sortBy(_._1).iterator)


    val relative_freq = reduced_bigram_counts
      .mapPartitions(iter => {
        var totalCount = 1 // Default value in case "*"-entry is missing
        iter.flatMap({
          case ((w1, "*"), count) => 
            totalCount = count // Store total count for `w1`
            Some(((w1, "*"), count.toDouble))
          case ((w1, w2), count) =>
            Some(((w1, w2), count.toDouble / totalCount)) 
        })
      })
    relative_freq.saveAsTextFile(args.output())
  }
}
