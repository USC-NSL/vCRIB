package edu.usc.enl.cacheflow.algorithms.feasibility.memory.clustering;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CutoffCluster {
    private int cutoff;
    private long[][] inputMatrix;
    private int[][] z;
    private int partitionsSize;
    private Map<Integer, List<Integer>> clusterPartitions;
    private Map<Integer, long[]> clusterData;

    public CutoffCluster(int cutoff, long[][] inputMatrix, int[][] z, int partitionsSize) {
        this.cutoff = cutoff;
        this.inputMatrix = inputMatrix;
        this.z = z;
        this.partitionsSize = partitionsSize;
    }

    public Map<Integer, List<Integer>> getClusterPartitions() {
        return clusterPartitions;
    }

    public Map<Integer, long[]> getClusterData() {
        return clusterData;
    }

    public CutoffCluster invoke() {
        clusterPartitions = new HashMap<Integer, List<Integer>>(z.length);
        clusterData = new HashMap<Integer, long[]>(z.length);
        int i = 0;
        for (long[] booleans : inputMatrix) {
            clusterData.put(i++, booleans);
        }
        int c = partitionsSize;
        for (int[] link : z) {
            if (link[2] > cutoff) {
                break;
            }
            int a = link[0];
            int b = link[1];

            //update partitions
            List<Integer> aPartitions = clusterPartitions.get(a);
            List<Integer> bPartitions = clusterPartitions.get(b);
            List<Integer> cPartition;
            if (aPartitions == null && bPartitions == null) {
                cPartition = new LinkedList<Integer>();
                cPartition.add(a);
                cPartition.add(b);

            } else if (aPartitions == null) {
                cPartition = bPartitions;
                cPartition.add(a);
            } else if (bPartitions == null) {
                cPartition = aPartitions;
                cPartition.add(b);
            } else {
                cPartition = aPartitions;
                cPartition.addAll(bPartitions);
            }
            clusterPartitions.remove(a);
            clusterPartitions.remove(b);
            clusterPartitions.put(c, cPartition);

            //update bitvector data
            long[] aData = clusterData.get(a);
            long[] bData = clusterData.get(b);
            for (int j = 0; j < aData.length; j++) {
                aData[j] = aData[j] | bData[j];
            }
            clusterData.remove(a);
            clusterData.remove(b);
            if (a < partitionsSize) {
                inputMatrix[a] = null;
            }
            if (b < partitionsSize) {
                inputMatrix[b] = null;
            }
            clusterData.put(c, aData);
            c++;
        }
        return null;
    }
}