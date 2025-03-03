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

package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.P;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;
import tl.lin.data.pair.PairOfStringInt;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import java.util.Arrays;


import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.print.DocFlavor.BYTE_ARRAY;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
    //private static final Text WORD = new Text();
    private static final PairOfStringInt WORD_DOCNO = new PairOfStringInt();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms and their frequencies.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }
      // Emit postings in the format ((term, doc#) , (freq))
      for (PairOfObjectInt<String> e : COUNTS) {

        WORD_DOCNO.set(e.getLeftElement(), (int) docno.get());
        context.write(WORD_DOCNO, new IntWritable(e.getRightElement()));
      }

    }
  }

  private static final class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStringInt, IntWritable,Text, BytesWritable> {
    private static final IntWritable DOC_COUNT = new IntWritable(0);
    private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    private final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    private final Text TERM = new Text();
    private final Text TERM_PREV = new Text();
    private final IntWritable DOC_ID = new IntWritable();
    private final IntWritable PREV_DOC_ID = new IntWritable(0);
    private final BytesWritable BYTE_ARRAY = new BytesWritable();
    
    

    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();

      // Compress the postings list
      TERM.set(key.getLeftElement()); 
      DOC_ID.set(key.getRightElement());

      //New term case
      if (TERM_PREV != null && !TERM_PREV.toString().equals(TERM.toString())){

        ByteArrayOutputStream finalByteOutputStream = new ByteArrayOutputStream();
        DataOutputStream finalDataOutputStream = new DataOutputStream(finalByteOutputStream);
        WritableUtils.writeVInt(finalDataOutputStream, DOC_COUNT.get());

        dataOutputStream.flush();
        finalDataOutputStream.write(byteArrayOutputStream.toByteArray());

        finalDataOutputStream.flush();

        BYTE_ARRAY.set(finalByteOutputStream.toByteArray(), 0, finalByteOutputStream.size());

        context.write(TERM_PREV, BYTE_ARRAY);
        
        //Reset the output stream
        BYTE_ARRAY.set(new byte[0], 0, 0);
        byteArrayOutputStream.reset();

        PREV_DOC_ID.set(0);
        DOC_COUNT.set(0);
      }

      
      int tf = 0;
      while (iter.hasNext()){
        tf += iter.next().get();
      }

      int gap = DOC_ID.get() - PREV_DOC_ID.get();
      
      WritableUtils.writeVInt(dataOutputStream, gap);
      WritableUtils.writeVInt(dataOutputStream, tf);
      

      DOC_COUNT.set(DOC_COUNT.get() + 1);

      //Update state variables
      PREV_DOC_ID.set(DOC_ID.get());
      TERM_PREV.set(TERM.toString());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      if (TERM_PREV != null){


        ByteArrayOutputStream finalByteOutputStream = new ByteArrayOutputStream();
        DataOutputStream finalDataOutputStream = new DataOutputStream(finalByteOutputStream);
        WritableUtils.writeVInt(finalDataOutputStream, DOC_COUNT.get());

        dataOutputStream.flush();
        finalDataOutputStream.write(byteArrayOutputStream.toByteArray());
        
        finalDataOutputStream.flush();

        BYTE_ARRAY.set(finalByteOutputStream.toByteArray(), 0, finalByteOutputStream.size());
        
        System.out.println("\n\nFinal Term Written: " + TERM_PREV.toString() + " with DOC_COUNT: " + DOC_COUNT.get());
    
        context.write(TERM_PREV, BYTE_ARRAY);
        byteArrayOutputStream.reset();
      }
    }

  }


  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);
    
    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
