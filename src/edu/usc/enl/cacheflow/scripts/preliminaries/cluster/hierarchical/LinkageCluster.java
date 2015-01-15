package edu.usc.enl.cacheflow.scripts.preliminaries.cluster.hierarchical;

import edu.usc.enl.cacheflow.algorithms.feasibility.memory.clustering.CutoffCluster;
import edu.usc.enl.cacheflow.model.Statistics;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/17/12
 * Time: 10:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class LinkageCluster {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        /*String linkageFile = "input/temp.txt";
        boolean[][] inputMatrix = new boolean[][]{
                {false, false, false}, {false, false, true},
                {false, true, false}, {false, true, true}, {true, false, false},
                {false, true, false}, {true, true, false}, {true, true, true}};
        int partitionsSize = 8;
        int rulesSize = 3;
        int[][] z;
        int cutoff = 2;*/


        int cutoff = 2700;
        String inputMatrixFile = "input\\nsdi\\partitionclassbench\\vmstart\\uniform\\-3\\m_20480_classbench_131072_2.txt";
        String linkageFile = "input\\nsdi\\partitionclassbench\\vmstart\\uniform\\-3\\l_20480_classbench_131072_2.txt";
        String clusterFile = "input\\nsdi\\clusterclassbench\\uniform\\-3\\" + cutoff + "_classbench_131072_2.txt";
        new File(clusterFile).getParentFile().mkdirs();

        long[][] inputMatrix;
        int[][] z;
        int partitionsSize;
        int rulesSize;
        Map<String, Object> parameters;
        {
            ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(inputMatrixFile)));
            parameters = (Map<String, Object>) ois.readObject();
            partitionsSize = ois.readInt();
            rulesSize = ois.readInt();
            inputMatrix = new long[partitionsSize][];
            GenerateLinkage.load(ois, inputMatrix);
        }
        {
            ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(linkageFile)));
            z = (int[][]) ois.readObject();
            ois.close();
        }

        {
            if (cutoff < 0) {
                int min = z[0][2];
                int max = z[z.length - 1][2];
                System.out.println("min= " + min + " max= " + max + " median= " + (z[z.length / 2][2]));
                int sum = 0;
                Set<Integer> heights = new HashSet<Integer>();
                for (int[] link : z) {
                    heights.add(link[2]);
                    sum += link[2];
                }
                System.out.println("dist mean=" + (sum / z.length));
                sum = 0;
                for (Integer height : heights) {
                    sum += height;
                }
                System.out.println("heights mean=" + (sum / heights.size()));
                System.exit(0);
            }
        }
        parameters.put("cluster.cutoff", cutoff);

        CutoffCluster cutoffCluster = new CutoffCluster(cutoff, inputMatrix, z, partitionsSize).invoke();
        Map<Integer, List<Integer>> clusterPartitions = cutoffCluster.getClusterPartitions();
        Map<Integer, long[]> clusterData = cutoffCluster.getClusterData();

        //write output
        PrintWriter writer = new PrintWriter(clusterFile);
        writer.println(Statistics.getParameterLine(parameters));

        for (int j = 0; j < inputMatrix.length; j++) {
            long[] booleans = inputMatrix[j];
            if (booleans == null) {
                continue;
            }
            writer.println(j + 1);//partition id
        }
        for (Map.Entry<Integer, List<Integer>> entry : clusterPartitions.entrySet()) {
            boolean first = true;
            for (Integer pid_1 : entry.getValue()) {
                writer.print((first ? "" : ",") + (pid_1 + 1));
                first = false;
            }
            writer.println();
        }

        writer.close();

        // write partitions in clusters
        for (int j = 0; j < inputMatrix.length; j++) {
            long[] booleans = inputMatrix[j];
            if (booleans == null) {
                continue;
            }
            int sum = 0;
            //System.out.print(j + ": [" + j + "], [");
            for (long boolSet : booleans) {
                //System.out.print((aBoolean ? 1 : 0) + ",");
                sum += Long.bitCount(boolSet);
            }
            //System.out.println("]");
            System.out.println(j + ": [" + j + "], " + sum);
        }
        for (Map.Entry<Integer, List<Integer>> entry : clusterPartitions.entrySet()) {
            int sum = 0;
            //System.out.print(entry.getKey() + ": " + entry.getValue() + ", [");
            for (long boolSet : clusterData.get(entry.getKey())) {
                //System.out.print((aBoolean ? 1 : 0) + ",");
                sum += Long.bitCount(boolSet);

            }
            //System.out.println("]");
            System.out.println(entry.getKey() + ": " + entry.getValue() + ", " + sum);
        }
    }

}
