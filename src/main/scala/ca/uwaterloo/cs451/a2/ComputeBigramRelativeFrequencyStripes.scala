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
import ca.uwaterloo.cs451.a2.Conf

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("BigramRelativeFrequencyStripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    //computer bigram and word counts simulataneously
    val bigram_counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) 
          tokens.sliding(2).flatMap({ case Seq(w1, w2) => List((w1, Map(w2 -> 1)), (w1,Map("*" -> 1)))})
        else
          List()
      })

    val reduced_bigram_counts = bigram_counts
      .reduceByKey({
        (map1, map2) => {
          map2.foldLeft(map1) {
            case (map1, (k,v)) => map1.updated(k, v + map1.getOrElse(k, 0))
          }
        }
      }, args.reducers()) //shuffles data to make sure that all identical bigrams are in the same partition
      
    val relative_freq = reduced_bigram_counts
      .map({
        case (w1, counts) => 
        val total = counts("*")
        (w1, counts.filterKeys(_ != "*")
        .map({ case (k, v) => k -> v.toDouble / total}))
      })

    relative_freq.saveAsTextFile(args.output())
  }
}
