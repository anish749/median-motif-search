import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Internet {

  public static class MotifInSeq implements Writable {

    public int score, seq_id, start_index, total = 0;
    public String candidate;

    public MotifInSeq() {

    }

    public MotifInSeq(MotifInSeq t) {
      score = t.score;
      seq_id = t.seq_id;
      start_index = t.start_index;
      total = t.total;
      candidate = t.candidate;
    }

    public void readFields(DataInput in) throws IOException {
      score = in.readInt();
      seq_id = in.readInt();
      start_index = in.readInt();
      candidate = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
      out.writeInt(score);
      out.writeInt(seq_id);
      out.writeInt(start_index);
      out.writeUTF(candidate);
    }

    @Override
    public String toString() {
      return candidate + "\t" + seq_id + "\t" + score + "\t" + start_index + "\t " + total;
    }
  }

  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, MotifInSeq> {

    private MotifInSeq value = new MotifInSeq();
    private Text word = new Text();

    public void map(LongWritable key, Text input, Context context)
        throws IOException, InterruptedException {
      String str = input.toString();
      int str_len = str.length();

      char m_char[] = "acgt".toCharArray();
      int[] m = new int[8];

      for (m[0] = 0; m[0] < 4; m[0]++) {
        for (m[1] = 0; m[1] < 4; m[1]++) {
          for (m[2] = 0; m[2] < 4; m[2]++) {
            for (m[3] = 0; m[3] < 4; m[3]++) {
              for (m[4] = 0; m[4] < 4; m[4]++) {
                for (m[5] = 0; m[5] < 4; m[5]++) {
                  for (m[6] = 0; m[6] < 4; m[6]++) {
                    for (m[7] = 0; m[7] < 4; m[7]++) {
                      int min_dist = 10;
                      int min_index = 0;
                      String motif = "";

                      for (int i = 0; i < 8; i++) {
                        motif += m_char[m[i]];
                      }

                      for (int i = 0; i < str_len - 8; i++) {
                        int dist = 0;
                        for (int j = 0; j < 8; j++) {
                          if (str.charAt(i + j) != motif.charAt(j)) {
                            dist++;
                          }
                        }
                        if (dist < min_dist) {
                          min_dist = dist;
                          min_index = i;
                        }
                        if (min_dist == 0) {
                          break;
                        }
                      }

                      word.set(motif);

                      value.score = min_dist;
                      value.seq_id = (int) key.get();
                      value.seq_id /= 59;
                      value.start_index = min_index;
                      value.candidate = str.substring(min_index, min_index + 8);

                      context.write(word, value);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, MotifInSeq, Text, MotifInSeq> {

    protected void setup(Context context) throws IOException, InterruptedException {

    }

    public void reduce(Text key, Iterable<MotifInSeq> values, Context context)
        throws IOException, InterruptedException {
      ArrayList<MotifInSeq> cache = new ArrayList<MotifInSeq>();
      int sum = 0;

      for (MotifInSeq val : values) {
        cache.add(new MotifInSeq(val));
        sum += val.score;
        //val.total = sum;
        //context.write(key, val);
      }

      for (MotifInSeq val : cache) {
        val.total = sum;
        context.write(key, val);
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");

    job.setJarByClass(Internet.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MotifInSeq.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MotifInSeq.class);

    FileInputFormat.addInputPath(job, new Path("data/small.txt"));
    FileOutputFormat.setOutputPath(job, new Path("output/tmp"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}