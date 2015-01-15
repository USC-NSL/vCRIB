package edu.usc.enl.cacheflow.scripts.preliminaries.cluster.hierarchical;

import edu.usc.enl.cacheflow.algorithms.feasibility.memory.clustering.BottomUpHierarchicalClustering;

import java.io.*;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/16/12
 * Time: 11:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateLinkage {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String inputMatrixFile = "input\\nsdi\\partitionclassbench\\vmstart\\uniform\\-1\\4076\\m_20480_classbench_131072_4.txt";
        //"input\\nsdismall\\partition2\\vmstart\\-1\\m_2559_classbench_8192_2.txt";
        String linkageFile = "input\\nsdi\\partitionclassbench\\vmstart\\uniform\\-1\\4076\\l_20480_classbench_131072_4.txt";
        ///"input\\nsdismall\\partition2\\vmstart\\-1\\l_2559_classbench_8192_2.txt";
        ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(inputMatrixFile)));
        Map<String, Object> parameters = (Map<String, Object>) ois.readObject();
        int partitionsSize = ois.readInt();
        int rulesSize = ois.readInt();
        long[][] inputMatrix = new long[partitionsSize][];
        load(ois, inputMatrix);
        System.out.println("input loaded");
        /*String linkageFile="input/temp.txt";
        boolean[][] inputMatrix = new boolean[][]{
                {false, false, false}, {false, false, true},
                {false, true, false}, {false, true, true}, {true, false, false},
                {false, true, false}, {true, true, false}, {true, true, true}};
        int partitionsSize = 8;
        int rulesSize = 3;*/

        BottomUpHierarchicalClustering clusterer = new BottomUpHierarchicalClustering(inputMatrix);

        //write partition sizes
        for (long[] inputLine : inputMatrix) {
            System.out.println(clusterer.distance(inputLine, inputLine));
        }
        //System.exit(0);

        //create distance matrix
        int distSize = (int) (0l + partitionsSize * (partitionsSize - 1) / 2);
        int[] dist = new int[distSize];
        clusterer.computeDistanceThread( dist);
        System.out.println("distance created");
        //compute linkage
        int[][] z = clusterer.linkage(dist);
        ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(linkageFile)));
        oos.writeObject(z);
        /*for (int i = 0; i < z.length; i++) {
            for (int j = 0; j < z[i].length; j++) {
                System.out.print(z[i][j] + ",");
            }
            System.out.println();
        }*/
        oos.close();

    }


    public static void load(ObjectInputStream ois, long[][] inputMatrix) throws IOException {
        int pid = 1;
        while (true) {
            try {
                Object o = ois.readObject();
                inputMatrix[pid - 1] = (long[]) o;
                pid++;
            } catch (IOException e) {
                break;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        ois.close();
    }


}
