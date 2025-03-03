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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import com.esotericsoftware.minlog.Log;

import tl.lin.data.pair.PairOfWritables;
import java.util.Arrays;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import org.apache.hadoop.fs.FileStatus;
import java.io.ByteArrayInputStream;

public class BooleanRetrievalCompressed extends Configured implements Tool {
  private MapFile.Reader[] index;
  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;

  private BooleanRetrievalCompressed() {}

  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
    try {
        //load index files across all partitions
      FileStatus[] status = fs.listStatus(new Path(indexPath), path -> path.getName().startsWith("part-r-")); 
      String[] folder_names = new String[status.length];
      index = new MapFile.Reader[status.length];
      
      //load index file paths across all partitions
      for (int i = 0; i < status.length; i++) {
        Path path = new Path(status[i].getPath().toString());
        folder_names[i] = path.toString();
        
      }
      Arrays.sort(folder_names);
      int i = 0;
      for (String folder_name : folder_names){
        System.out.println("Folder name: " + folder_name);
        index[i] = new MapFile.Reader(new Path(folder_name), fs.getConf());
        i++;
      }


      collection = fs.open(new Path(collectionPath));
      stack = new Stack<>();
    } catch (IOException e) {
     System.out.println("Error initializing files: " + e.getMessage());
    }
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    //pushes the term's doc ids to the stack
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }


  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Text key = new Text();
    BytesWritable value = new BytesWritable();
    Set<Integer> doc_set = new TreeSet<>();

    key.set(term);

    int partition = (term.hashCode() & Integer.MAX_VALUE) % index.length;
    MapFile.Reader reader = index[partition];
    System.out.println("Using reader for partition: " + partition);
    
      try{
        if (reader.get(key, value) != null){
          //fetches the postings list for a term
          byte[] postings = value.getBytes();
          

          DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(postings));
          
          int posting_length = WritableUtils.readVInt(dataInput);
          System.out.println("Posting Length: " + posting_length);
          int prev_doc_id = 0;
          

          for (int i =0; i< posting_length; i++){
            try {

              int gap = WritableUtils.readVInt(dataInput);
              int doc_id = gap + prev_doc_id;
              WritableUtils.readVInt(dataInput);
              // System.out.println("Doc ID: " + doc_id);
              // System.out.println("Term Frequency: " + WritableUtils.readVInt(dataInput));
              // System.out.println("Term: " + term);
              prev_doc_id = doc_id;
              doc_set.add(doc_id);
            } catch (IOException e) {
              System.out.println("Error fetching document set: " + e.getMessage());
              e.printStackTrace();
              break;
            }
            
          }

        }
    } catch (IOException e) {
      System.out.println("Error fetching document set: " + e.getMessage());
      e.printStackTrace();
    }
    return doc_set;
  }

  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  private static final class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    String query;
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

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(args.index, args.collection, fs);

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
