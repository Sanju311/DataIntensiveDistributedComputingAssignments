package ca.uwaterloo.cs451.a1;

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


import io.bespin.java.util.Tokenizer;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloats;

import java.net.URI;
import org.apache.hadoop.util.LineReader;

/**
* Simple word count demo with no optimization. See WordCount.java for combiner optimization.
*/
public class PairsPMI extends Configured implements Tool {
  
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);
  private static int MAX_WORD = 40;

  // Mapper: emits (token, 1) for a word occurrence on a line.
  public static final class wordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);  
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      HashSet<String> wordMap = new HashSet<>();
      List<String> line =  Tokenizer.tokenize(value.toString());


      context.write(new Text("__TOTAL_LINES__"), ONE);
      if (line.size() > 0){

        for (int i=0; i< Math.min(MAX_WORD, line.size()); i++){
          String word = line.get(i);
          if(!wordMap.contains(word)){
            wordMap.add(word);
            WORD.set(word);
            context.write(WORD, ONE);
          }
        }
      }
      
    }
  }

  public static final class wordCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        SUM.set(sum);
        context.write(key, SUM); // Emit partial sum
    }

  }

  // Reducer: sums up all the word counts.
  public static final class wordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects.
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      
      // Sum up values.
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

    // Mapper: emits for pairs of word appearances in the same line.
  public static final class CoOccurenceMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static int MAX_WORD = 40;
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
      
      List<String> Line = Tokenizer.tokenize(value.toString());
      HashSet<PairOfStrings> pairMap = new HashSet<>();

      if(Line.size() > 1){
        for (int i=0; i< Math.min(MAX_WORD, Line.size()); i++){
          for (int j = 0; j< Math.min(MAX_WORD, Line.size()); j++){
            
            if(i == j || Line.get(i).equals(Line.get(j))){
              continue;
            }

            PairOfStrings pair = new PairOfStrings(Line.get(i), Line.get(j));
            if(!pairMap.contains(pair)){
              pairMap.add(pair);
              context.write(pair,ONE);
            }
          }
        }
      }
    }
  }

  public static final class CoOccurenceCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    protected void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        SUM.set(sum);
        context.write(key, SUM); // Emit partial sum
    }

  }


  // Reducer: sums up all the counts of pairs of words.
  public static final class CoOccurenceReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloats > {
    // Reuse objects.
    private static final IntWritable SUM = new IntWritable();
    private static HashMap<String, Integer> wordCount = new HashMap<>();
    private int threshold;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

      //get the output of job 1 as a file and read from the file 
      URI[] cacheFiles = context.getCacheFiles();
      FileSystem fs = FileSystem.get(context.getConfiguration());

      if (cacheFiles != null && cacheFiles.length > 0) {  
        
        Path path = new Path(cacheFiles[0].toString());
        LineReader reader = new LineReader(fs.open(path));
        Text txt = new Text(); 

        while (reader.readLine(txt) > 0){
          String[] wordCountPair = txt.toString().split("\\s+");
          wordCount.put(wordCountPair[0], Integer.parseInt(wordCountPair[1]));
        }
        reader.close();
    }

      threshold = context.getConfiguration().getInt("threshold", 10);
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      //gets wordCount from cache
      PairOfFloats PMI = new PairOfFloats();

      
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      if (sum >= threshold){
        
        double word1Freq = (double) wordCount.get(key.getLeftElement())/wordCount.get("__TOTAL_LINES__"); // Frequency of word1
        double word2Freq = (double) wordCount.get(key.getRightElement())/wordCount.get("__TOTAL_LINES__"); // Frequency of word2
        double word12Freq = (double) sum / wordCount.get("__TOTAL_LINES__");  // Frequency of word1 and word2

        float pmi = (float)Math.log10(word12Freq / (word1Freq * word2Freq));

        PMI.set(pmi,sum) ;
        context.write(key, PMI);
      }
      
    }
  }

  /**
  * Creates an instance of this tool.
  */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "minimal co-occurrence threshold")
    int threshold = 10;
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

    Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO);
    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = getConf();
    conf.setInt("threshold", args.threshold);

    Job job = Job.getInstance(conf);
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);
    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output + "_unique_word_count"));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapperClass(wordMapper.class);
    job.setCombinerClass(wordCombiner.class);
    job.setReducerClass(wordReducer.class);
    job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    //Delete the output directory if it exists already.
    Path outputDir1 =  FileOutputFormat.getOutputPath(job);
    FileSystem.get(conf).delete(outputDir1, true);

    long startTime = System.currentTimeMillis();
    boolean job1success = job.waitForCompletion(true);
    LOG.info("Job 1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    if (!job1success) {
      System.out.println("Job 1 failed, exiting");
      return -1;
    }

    //job 2 
    Job job2 = Job.getInstance(conf);
    job2.setJobName(PairsPMI.class.getSimpleName());
    job2.setJarByClass(PairsPMI.class);
    job2.setNumReduceTasks(args.numReducers);
    job2.addCacheFile(new Path(FileOutputFormat.getOutputPath(job) + "/part-r-00000").toUri());

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(PairOfFloats.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    job2.setMapperClass(CoOccurenceMapper.class);
    job2.setReducerClass(CoOccurenceReducer.class);
    job2.setCombinerClass(CoOccurenceCombiner.class);
    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    Path outputDir2 = new Path(args.output);
    FileSystem.get(conf).delete(outputDir2, true);

    long startTime2 = System.currentTimeMillis();
    boolean job2success = job2.waitForCompletion(true);
    LOG.info("Job 2 Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
    if (!job2success) {
      System.out.println("Job 2 failed, exiting");
      return -1;
    }

    return 0;
  }

  /**
  * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
  *
  * @param args command-line arguments
  * @throws Exception if tool encounters an exception
  */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}
 