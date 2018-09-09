package utils;

import java.util.Arrays;
import org.apache.commons.lang.StringUtils;

public class Utils {

  private final static String nucleotides = "atgc";

  public static String[] allMotifCombinations(int length) {
    char[] chars = nucleotides.toCharArray();
    int num_nucleotides = nucleotides.length();

    int totalCombinations = (int) Math.pow(num_nucleotides, length);
    String[] combinations = new String[totalCombinations];

    String t;
    for (int i = 0; i < totalCombinations; i++) {
      // Produce `i` in base 4 and pad with 0
      t = StringUtils.leftPad(Integer.toString(i, num_nucleotides), length, '0');

      // convert to nucleotides
      for (int j = 0; j < num_nucleotides; j++) {
        t = t.replaceAll(Integer.toString(j), Character.toString(chars[j]));
      }
      combinations[i] = t;
    }

    return combinations;
  }

  public static void main(String[] args) {
//    System.out.println(Arrays.asList(allMotifCombinations(5)));
    minDistance(1, "ccgagtagacccttagagagcatgtcagcctcgacaacttgcataaatgctttcttg", "aacgcttt");
  }

  public static int hammingDistance(String s1, String s2) {
    int distance = Math.abs(s1.length() - s2.length());
    for (int i = 0; i < Math.min(s1.length(), s2.length()); i++) {
      if (s1.charAt(i) != s2.charAt(i)) {
        distance++;
      }
    }
    return distance;
  }


  public static MotifMatchWritable minDistance(long sequenceId, String dnaSeq, String motif) {

    int motifLength = motif.length();
    int minHammingDistance = motifLength + 1;
    int alignmentIndex = -1;

    for (int i = 0; i < dnaSeq.length() - motifLength; i++) {
      int hd = hammingDistance(motif, dnaSeq.substring(i, i + motifLength));
      if (hd < minHammingDistance) {
        minHammingDistance = hd;
        alignmentIndex = i;
      }
    }
    return new MotifMatchWritable(sequenceId, dnaSeq, motif, minHammingDistance, alignmentIndex);
  }

//  public static int totalDistance(String dna, String motif){
//
//    int totalDist = 0;
//    int k = motif.length();
//
//    for (char c:dna.toCharArray()){
//      int minHammingDist = k +1;
//      for
//    }
//
//    totalDist = 0
//    bestAlignment = []
//    k = len(motif)
//    for seq in DNA:
//    minHammingDist = k+1
//    for s in xrange(len(seq)-k+1):
//    HammingDist = sum([1 for i in xrange(k) if motif[i] != seq[s+i]])
//    if (HammingDist < minHammingDist):
//    bestS = s
//    minHammingDist = HammingDist
//    bestAlignment.append(bestS)
//    totalDist += minHammingDist
//    return bestAlignment, totalDist
//  }

}
