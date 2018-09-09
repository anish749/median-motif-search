import static utils.Utils.allMotifCombinations;
import static utils.Utils.minDistance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.MotifMatchWritable;


public class MedianStringMotifSearchMapReduce {

  private final static int DEFAULT_MOTIF_LENGTH = 8;

  /**
   * Mapper to explore all motifs and find min hamming distance with the given sequence
   */
  public static class MedianMotifSearchMapper extends
      Mapper<LongWritable, Text, Text, MotifMatchWritable> {

    private long seqId;
    private int motifLength;
    private String[] allMotifs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      seqId = 0;
      motifLength = Integer.parseInt(context.getConfiguration().get("motif.length"));
      allMotifs = allMotifCombinations(motifLength);
    }

    public void map(LongWritable key, Text record, Context con)
        throws IOException, InterruptedException {
      String dnaSeq = record.toString();

      for (String searchMotif : allMotifs) {
        con.write(new Text(searchMotif), minDistance(seqId, dnaSeq, searchMotif));
      }
      seqId++;
    }
  }

  /**
   * Find total hamming distance for each motif
   */
  public static class TotalHammingDistanceReducer extends
      Reducer<Text, MotifMatchWritable, Text, LongWritable> {

    public void reduce(Text motif, Iterable<MotifMatchWritable> valueList,
        Context con) throws IOException, InterruptedException {

      long totalHammingDistance = 0;
      for (MotifMatchWritable value : valueList) {
        totalHammingDistance += value.getMinHammingDistance().get();
      }

      con.write(motif, new LongWritable(totalHammingDistance));

    }
  }

  private static boolean totalHammingDistanceJob(String inputPath, String outputPath,
      int motifLength) {
    String jobName = "TotalHammingDistanceJob";

    try {
      Configuration conf = new Configuration(true);
      conf.set("motif.length", Integer.toString(motifLength));
      Job job = Job.getInstance(conf, jobName);
      job.setJarByClass(MedianStringMotifSearchMapReduce.class);

      job.setMapperClass(MedianMotifSearchMapper.class);
      job.setReducerClass(TotalHammingDistanceReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(MotifMatchWritable.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);

      FileInputFormat.addInputPath(job, new Path(inputPath));
      FileOutputFormat.setOutputPath(job, new Path(outputPath));

      return job.waitForCompletion(true);
    } catch (Exception e) {
      System.err.println("Exception while processing job " + jobName + " : " + e.getMessage());
    }
    return false;
  }


  // key by motif, then reduce again to minimize total hamming distance,
  public static class ConsensusStringSearchMapper extends
      Mapper<LongWritable, Text, Text, MotifMatchWritable> {

    public void map(LongWritable key, Text record, Context con)
        throws IOException, InterruptedException {
      String[] splited = record.toString().split("\t");
      // Split motif and total hamming distance across all sequences
      con.write(new Text(splited[0]),
          new MotifMatchWritable(splited[0], Long.parseLong(splited[1])));
    }
  }


  // Uses State and should be run as one reducer only
  // For 4^8 = 65536 one reducer would not be the bottle neck
  // The logic here should not be combined with TotalHammingDistanceReducer since Total Distance can be calculated with multiple reducers.
  public static class ConsensusStringSearchReducer extends
      Reducer<Text, MotifMatchWritable, Text, LongWritable> {

    private Text consensusString;
    private long minHammingDistance;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      minHammingDistance = Long.MAX_VALUE;
    }

    public void reduce(Text motif, Iterable<MotifMatchWritable> valueList,
        Context con) throws IOException, InterruptedException {

      // We expect only the total value to come here
      Iterator<MotifMatchWritable> it = valueList.iterator();
      if (it.hasNext()) {
        long totalHammingDistance = it.next().getTotalHammingDistance().get();
        if (totalHammingDistance < minHammingDistance) {
          minHammingDistance = totalHammingDistance;
          consensusString = new Text(motif);
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(consensusString, new LongWritable(minHammingDistance));
      super.cleanup(context);
    }
  }


  private static boolean consensusStringSearch(String inputPath, String outputPath) {
    String jobName = "ConsensusStringSearchJob";

    try {
      Configuration conf = new Configuration(true);
      Job job = Job.getInstance(conf, jobName);
      job.setJarByClass(MedianStringMotifSearchMapReduce.class);

      job.setMapperClass(ConsensusStringSearchMapper.class);
      job.setReducerClass(ConsensusStringSearchReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(MotifMatchWritable.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);

      job.setNumReduceTasks(1);

      FileInputFormat.addInputPath(job, new Path(inputPath));
      FileOutputFormat.setOutputPath(job, new Path(outputPath));

      return job.waitForCompletion(true);
    } catch (Exception e) {
      System.err.println("Exception while processing job " + jobName + " : " + e.getMessage());
    }
    return false;
  }

  /**
   * This is used to filter the results of the Motif Search Mapper with the selected Median String
   */
  public static class FilterConsensusStringReducer extends
      Reducer<Text, MotifMatchWritable, MotifMatchWritable, NullWritable> {

    public void reduce(Text motif, Iterable<MotifMatchWritable> valueList,
        Context con) throws IOException, InterruptedException {

      boolean isMedianMotif = false;
      long totalHammingDistance = 0;
      ArrayList<MotifMatchWritable> values = new ArrayList<MotifMatchWritable>();
      for (MotifMatchWritable value : valueList) {
        if (value.getIsMedianMotif().get()) { // If there is a match, we set this to true.
          isMedianMotif = true;
          totalHammingDistance = value.getTotalHammingDistance().get();
        } else {
          values.add(new MotifMatchWritable(value));
        }
      }

      if (isMedianMotif) {
        for (MotifMatchWritable val : values) {
          val.setTotalHammingDistance(totalHammingDistance);
          con.write(val, NullWritable.get());
        }
      }


    }
  }

  /**
   * Filter the original data to create all sequences with Median String
   */
  private static boolean medianMotifStringSearchJob(String inputPath, String consensusStringPath,
      String outputPath, int motifLength) {
    String jobName = "MedianMotifStringSearchJob";

    try {
      Configuration conf = new Configuration(true);
      conf.set("motif.length", Integer.toString(motifLength));
      Job job = Job.getInstance(conf, jobName);
      job.setJarByClass(MedianStringMotifSearchMapReduce.class);

      MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class,
          MedianMotifSearchMapper.class);
      MultipleInputs.addInputPath(job, new Path(consensusStringPath), TextInputFormat.class,
          ConsensusStringSearchMapper.class); // Use the same mapper to read the final consensus String

      job.setReducerClass(FilterConsensusStringReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(MotifMatchWritable.class);

      job.setOutputKeyClass(MotifMatchWritable.class);
      job.setOutputValueClass(NullWritable.class);

      FileOutputFormat.setOutputPath(job, new Path(outputPath));

      return job.waitForCompletion(true);
    } catch (Exception e) {
      System.err.println("Exception while processing job " + jobName + " : " + e.getMessage());
    }
    return false;

  }


  public static void main(String[] args) {

    String inputPath = args[0];
    String tmpOutputPath = args[1];
    String outputPath = args[2];
    int motifLength = DEFAULT_MOTIF_LENGTH;
    if (args.length >= 4) {
      motifLength = Integer.parseInt(args[3]);
    }

    String totalHammingDistancePath = tmpOutputPath + "/totalHammingDistancePath";
    String consensusStringPath = tmpOutputPath + "/consensusString";

    if (totalHammingDistanceJob(inputPath, totalHammingDistancePath, motifLength)
        && consensusStringSearch(totalHammingDistancePath, consensusStringPath)
        && medianMotifStringSearchJob(inputPath, consensusStringPath, outputPath, motifLength)) {
      System.exit(0);
    } else {
      System.exit(1);
    }
  }
}
