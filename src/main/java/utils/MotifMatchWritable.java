package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// Encapsulates a motif along with its matching sequence
public class MotifMatchWritable implements Writable {

  private LongWritable sequenceId;
  private Text dnaSequence;
  private Text motif;
  private IntWritable minHammingDistance;
  private IntWritable alignmentIndex;

  private BooleanWritable isMedianMotif;
  private LongWritable totalHammingDistance; // this the total HD for all sequences for the given Motif

  public MotifMatchWritable() {
    this.sequenceId = new LongWritable();
    this.dnaSequence = new Text();
    this.motif = new Text();
    this.minHammingDistance = new IntWritable();
    this.alignmentIndex = new IntWritable();
    this.isMedianMotif = new BooleanWritable(false);
    this.totalHammingDistance = new LongWritable();
  }

  // Constructor for MedianMotifString
  public MotifMatchWritable(String motif, long totalHammingDistance) {
    this.sequenceId = new LongWritable();
    this.dnaSequence = new Text();
    this.motif = new Text(motif);
    this.minHammingDistance = new IntWritable();
    this.alignmentIndex = new IntWritable();
    this.isMedianMotif = new BooleanWritable(true);
    this.totalHammingDistance = new LongWritable(totalHammingDistance);
  }

  public MotifMatchWritable(LongWritable sequenceId, Text dnaSequence,
      Text motif, IntWritable minHammingDistance, IntWritable alignmentIndex) {
    this.sequenceId = sequenceId;
    this.dnaSequence = dnaSequence;
    this.motif = motif;
    this.minHammingDistance = minHammingDistance;
    this.alignmentIndex = alignmentIndex;
    this.isMedianMotif = new BooleanWritable(false);
    this.totalHammingDistance = new LongWritable(); // This should be set using setter
  }

  public MotifMatchWritable(long sequenceId, String dnaSequence, String motif,
      int minHammingDistance, int alignmentIndex) {
    this.sequenceId = new LongWritable(sequenceId);
    this.dnaSequence = new Text(dnaSequence);
    this.motif = new Text(motif);
    this.minHammingDistance = new IntWritable(minHammingDistance);
    this.alignmentIndex = new IntWritable(alignmentIndex);
    this.isMedianMotif = new BooleanWritable(false);
    this.totalHammingDistance = new LongWritable(); // This should be set using setter
  }

  // Copy constructor
  public MotifMatchWritable(MotifMatchWritable motifMatchWritable) {
    this.sequenceId = new LongWritable(motifMatchWritable.getSequenceId().get());
    this.dnaSequence = new Text(motifMatchWritable.getDnaSequence().toString());
    this.motif = new Text(motifMatchWritable.getMotif().toString());
    this.minHammingDistance = new IntWritable(motifMatchWritable.getMinHammingDistance().get());
    this.alignmentIndex = new IntWritable(motifMatchWritable.getAlignmentIndex().get());
    this.isMedianMotif = new BooleanWritable(motifMatchWritable.getIsMedianMotif().get());
    this.totalHammingDistance = new LongWritable(
        motifMatchWritable.getTotalHammingDistance().get());
  }

  public LongWritable getSequenceId() {
    return sequenceId;
  }

  public Text getDnaSequence() {
    return dnaSequence;
  }

  public Text getMotif() {
    return motif;
  }

  public IntWritable getMinHammingDistance() {
    return minHammingDistance;
  }

  public IntWritable getAlignmentIndex() {
    return alignmentIndex;
  }

  public BooleanWritable getIsMedianMotif() {
    return isMedianMotif;
  }

  public LongWritable getTotalHammingDistance() {
    return totalHammingDistance;
  }

  public void setTotalHammingDistance(LongWritable totalHammingDistance) {
    this.totalHammingDistance = totalHammingDistance;
  }

  public void setTotalHammingDistance(long totalHammingDistance) {
    this.totalHammingDistance = new LongWritable(totalHammingDistance);
  }

  public void write(DataOutput dataOutput) throws IOException {
    sequenceId.write(dataOutput);
    dnaSequence.write(dataOutput);
    motif.write(dataOutput);
    minHammingDistance.write(dataOutput);
    alignmentIndex.write(dataOutput);
    isMedianMotif.write(dataOutput);
    totalHammingDistance.write(dataOutput);
  }

  public void readFields(DataInput dataInput) throws IOException {
    sequenceId.readFields(dataInput);
    dnaSequence.readFields(dataInput);
    motif.readFields(dataInput);
    minHammingDistance.readFields(dataInput);
    alignmentIndex.readFields(dataInput);
    isMedianMotif.readFields(dataInput);
    totalHammingDistance.readFields(dataInput);
  }

  @Override
  public String toString() {
    return motif +
        "\t" + dnaSequence.toString()
        .substring(alignmentIndex.get(), alignmentIndex.get() + motif.getLength()) +
        "\t" + sequenceId +
        "\t" + minHammingDistance +
        "\t" + alignmentIndex +
        "\t" + totalHammingDistance;
  }
}
