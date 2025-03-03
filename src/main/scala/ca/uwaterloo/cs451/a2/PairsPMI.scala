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
import scala.math.log10
import java.io._

class ConfPMI(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())
    

    val output = args.output()

    val conf = new SparkConf().setAppName("PairsPMI")
    val sc = new SparkContext(conf)

    val threshold = sc.broadcast(args.threshold())
    val reducers = args.reducers()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    //first pass to get the word count
    val word_counts = textFile
      .flatMap(line => { 
        val words = tokenize(line).take(40).distinct
        val word_counts = words.map(word => (word, 1)).toList
        word_counts :+ ("*", 1)
      })
      .reduceByKey(_ + _, reducers)
      .collectAsMap()
   
    // Save the word counts to a text file within the output directory
    val writer = new PrintWriter(new File(output + "word_counts.txt"))
    word_counts.foreach { case (word, count) =>
      writer.println(s"$word\t$count")
    }
    writer.close()
    val word_counts_broadcast = sc.broadcast(word_counts)

    //second pass to get the pairsPMI
    val pairs = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(40).distinct
        if (tokens.length > 1)
          tokens.combinations(2).flatMap({ case Seq(w1, w2) => List(((w1, w2),1),((w2, w1),1))})
        else
          List()
      })
      .reduceByKey(_ + _, reducers)
      .filter{case ((w1,w2), count) => count >= threshold.value}
      .map{
        case ((w1,w2), count) => {

          val word_counts = word_counts_broadcast.value
          val line_count = word_counts("*").toDouble

          val w12prob = count.toDouble / line_count
          val w1prob = word_counts(w1) / line_count
          val w2prob = word_counts(w2) / line_count

          ((w1,w2), log10(w12prob / (w1prob * w2prob)))
        }
      }
    pairs.saveAsTextFile(output)
  }    
}

