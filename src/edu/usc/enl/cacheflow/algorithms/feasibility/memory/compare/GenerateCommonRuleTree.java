package edu.usc.enl.cacheflow.algorithms.feasibility.memory.compare;

import edu.usc.enl.cacheflow.scripts.preliminaries.cluster.hierarchical.GenerateLinkage;

import java.io.*;
import java.util.Map;

import static edu.usc.enl.cacheflow.util.Util.IntegerWrapper;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/27/12
 * Time: 4:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateCommonRuleTree {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String inputMatrixFile =
        //"input\\nsdi\\partitiontenant\\vmstart\\-1\\m_20480_0_64_8_16_0.5_-1_1.0.txt";
        //"input\\nsdismall\\partitionclassbench\\vmstart\\uniform\\-1\\m_2560_classbench_32768_4.txt";
        "input\\nsdi\\classbenchpartition\\vmstart\\imc\\m_20480_classbench_131072_2.txt";
        String linkageFile =
                //"input\\nsdi\\partitiontenant\\vmstart\\-1\\lc_20480_0_64_8_16_0.5_-1_1.0.txt";
        //"input\\nsdismall\\partitionclassbench\\vmstart\\uniform\\-1\\lc_2560_classbench_32768_4.txt";
        "input\\nsdi\\classbenchpartition\\vmstart\\imc\\l_20480_classbench_131072_2.txt";
        loadAndRun(inputMatrixFile, linkageFile);

    }

    public static void loadAndRun(String inputMatrixFile, String linkageFile) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(inputMatrixFile)));
        Map<String, Object> parameters = (Map<String, Object>) ois.readObject();
        int partitionsSize = ois.readInt();
        int rulesSize = ois.readInt();
        int longNumbers = (int) Math.ceil(1.0 * rulesSize / 64);
        long[][] inputMatrix = new long[partitionsSize][longNumbers];
        GenerateLinkage.load(ois, inputMatrix);
        /*long[][] inputMatrix = new long[][]{
                new long[]{255-2},
                new long[]{255-1},
                new long[]{2047 - 15},
                new long[]{2047 - 15 - 256},
                new long[]{2048 + 128},
        };
        for (long[] longs : inputMatrix) {
            for (long aLong : longs) {
                System.out.println(Long.toBinaryString(aLong));
            }
        }
        int partitionsSize = 5;
        int rulesSize = 11;
        String linkageFile = "tempLinkage.txt";*/

        System.out.println("input loaded");
        int[] sizes = getSizes(inputMatrix);
        File file = new File(inputMatrixFile);
        PrintWriter similarityHistogramWriter = new PrintWriter(file.getParent() + "/hist_" + file.getName());
        run(linkageFile, partitionsSize, rulesSize, inputMatrix, sizes, new PrintWriter(System.out), similarityHistogramWriter, null);
        similarityHistogramWriter.close();
    }

    /**
     * That's users oportunity to close the provided writers
     *
     * @param linkageFile
     * @param partitionsSize
     * @param rulesSize
     * @param inputMatrix
     * @param sizes
     * @param sizeWriter
     * @param similarityHistogramWriter
     * @param matlabLinkagePresentationWriter
     *
     * @throws IOException
     */
    private static void run(String linkageFile, int partitionsSize, int rulesSize, long[][] inputMatrix, int[] sizes, PrintWriter sizeWriter,
                            PrintWriter similarityHistogramWriter, PrintWriter matlabLinkagePresentationWriter) throws IOException {
        //write partition sizes
        if (sizeWriter != null) {
            for (int i = 0; i < sizes.length; i++) {
                sizeWriter.print((i == 0 ? "" : ",") + sizes[i]);
            }
            sizeWriter.println();
        }

        //create similarity matrix
        int distSize = (int) (0l + partitionsSize * (partitionsSize - 1) / 2);
        int[] sim = new int[distSize];
        computeSimilarityThread(partitionsSize, inputMatrix, sim, sizes);
        System.out.println("similarity created");

        if (similarityHistogramWriter != null) {
            //compute similrity histogram
            double[] simSum = computeSimilarityHistogram(sim, partitionsSize, sizes);
            for (int i1 = 0, simSumLength = simSum.length; i1 < simSumLength; i1++) {
                similarityHistogramWriter.print((i1 == 0 ? "" : ",") + String.format("%.4f", simSum[i1]));
            }
            similarityHistogramWriter.println("");
        }

        //compute linkage
        ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(linkageFile)));
        oos.writeObject(sim);
        int[][] z = linkage(inputMatrix, sim, sizes);
        oos.writeObject(z);
        oos.writeObject(sizes);

        oos.close();

        if (matlabLinkagePresentationWriter != null) {
            //commulative for matlab linkage presentation
            for (int i = z.length - 1; i >= 0; i--) {
                if (z[i][0] >= partitionsSize) {
                    z[z[i][0] - partitionsSize][2] += z[i][2]; //parent contributes to children
                }
                if (z[i][1] >= partitionsSize) {
                    z[z[i][1] - partitionsSize][2] += z[i][2]; //parent contributes to children
                }
            }

            for (int[] aZ : z) {
                matlabLinkagePresentationWriter.println(aZ[0] + "," + aZ[1] + "," + (rulesSize - aZ[2]));
            }
        }
    }

    private static double[] computeSimilarityHistogram(int[] sim, int m, int[] sizes) {
        int[] num = new int[2 * m - 1];
        double[] simSum = new double[2 * m - 1];
        for (int i = 0; i < m; i++) {
            for (int i1 = 0; i1 < i; i1++) {
                num[m + i1 - i - 1] += 1;
                simSum[m + i1 - i - 1] += 1.0 * sim[getIndex(i1, i, m)] / sizes[i];
            }
            num[m - 1] += 1;
            simSum[m - 1] += 1;//sizes[i]/sizes[i]
            for (int i1 = i + 1; i1 < m; i1++) {
                num[m + i1 - i - 1] += 1;
                simSum[m + i1 - i - 1] += 1.0 * sim[getIndex(i, i1, m)] / sizes[i];
            }
        }
        for (int i = 0; i < simSum.length; i++) {
            simSum[i] /= num[i];
        }
        return simSum;
    }

    private static int[][] linkage(long[][] inputMatrix, int[] dist, int[] sizes) {
        int n = dist.length;
        int m = (int) Math.ceil(Math.sqrt(2 * n));
        int[][] z = new int[m - 1][3];
        n = m;//m will change
        int[] R = new int[n];
        for (int i = 0; i < n; i++) {
            R[i] = i;
        }
        Converter converter = new Converter();

        for (int s = 0; s < n - 1; s++) {
            FindMax findMax = new FindMaxThread(6, dist).invoke();
            int maxIndex = findMax.getMaxIndex();
            int max = findMax.getMax();
            converter.compute(maxIndex, m);
            int i = converter.getI();
            int j = converter.getJ();
            z[s] = new int[]{R[i], R[j], max};
            if (R[i] - n >= 0) {
                z[R[i] - n][2] -= max;
            }
            if (R[j] - n >= 0) {
                z[R[j] - n][2] -= max;
            }


            //System.out.println(s + ": merge " + i + "(" + R[i] + "), " + j + "(" + R[j] + "), " + max);
            if (s % 1000 == 0) {
                System.out.println(s + " out of " + (n - 1));
            }
            for (int i1 = 0; i1 < inputMatrix[i].length; i1++) {
                inputMatrix[i][i1] = inputMatrix[i][i1] & inputMatrix[j][i1];
            }
            //update similarity function
            inputMatrix[j] = null;
            updateDistances(inputMatrix, dist, m, i, sizes);
            updateDistances(inputMatrix, dist, m, j, sizes);
            R[i] = n + s;
        }
        return z;
    }

    private static void updateDistances(long[][] inputMatrix, int[] sim, int m, int i, int[] sizes) {
        for (int i1 = 0; i1 < i; i1++) {
            sim[getIndex(i1, i, m)] = similarity(inputMatrix[i], inputMatrix[i1], sizes[i], sizes[i1]);
        }
        for (int i1 = i + 1; i1 < m; i1++) {
            sim[getIndex(i, i1, m)] = similarity(inputMatrix[i], inputMatrix[i1], sizes[i], sizes[i1]);
        }
    }

    private static int getIndex(int i, int j, int m) {
        i = i + 1;
        j = j + 1;
        return (int) (i * (m - (i + 1) / 2.0) - m + j) - 1;
    }

    private static class Converter {
        int i = 0;
        int j = 0;

        public void compute(int k, int m) {
            k = k + 1;
            i = (int) (m + 1 / 2.0 - Math.sqrt(m * m - m + 1 / 4.0 - 2 * (k - 1)));
            j = (int) (k - (i - 1) * (m - i / 2.0) + i);
            i = i - 1;
            j = j - 1;
        }

        public int getI() {
            return i;
        }

        public int getJ() {
            return j;
        }
    }

    protected static void computeSimilarityThread(int partitionsSize, final long[][] inputMatrix, final int[] sim, final int[] sizes) {
        final int n = partitionsSize; //for brevity!
        final int threadNum = 6;
        Thread[] threads = new Thread[threadNum];
        final IntegerWrapper index = new IntegerWrapper(0);
        for (int i = 0; i < threadNum; i++) {
            final int threadId = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    int i;
                    while (true) {
                        synchronized (index) {
                            if (index.getValue() > n - 2) {
                                return;
                            }
                            i = index.getValue();
                            index.setValue(i + 1);
                        }
                        if (i % 1000 == 0) {
                            System.out.println(i + " in " + threadId);
                        }
                        for (int j = i + 1; j < n; j++) {
                            sim[getIndex(i, j, n)] = similarity(inputMatrix[i], inputMatrix[j], sizes[i], sizes[j]);
                        }
                        //Y(k:(k + n - i - 1))=feval(distfun, X(i,:),X((i + 1):n,:),distargs {:})';
                    }
                }
            };
        }
        for (Thread thread : threads) {
            thread.start();
        }
        try {
            for (Thread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static abstract class ComputThread extends Thread {
        protected int threadId;
        protected IntegerWrapper index;

        protected ComputThread(int threadId, IntegerWrapper index) {
            this.threadId = threadId;
            this.index = index;
        }
    }


    private static void computeSimilarity(int partitionsSize, long[][] inputMatrix, int[] dist, int[] sizes) {
        int n = partitionsSize; //for brevity!
        for (int i = 0; i < n - 1; i++) {
            if (i % 1000 == 0) {
                System.out.println(i);
            }
            for (int j = i + 1; j < n; j++) {
                dist[getIndex(i, j, n)] = similarity(inputMatrix[i], inputMatrix[j], sizes[i], sizes[j]);
            }
            //Y(k:(k + n - i - 1))=feval(distfun, X(i,:),X((i + 1):n,:),distargs {:})';
        }
    }

    private static int[] getSizes(long[][] inputMatrix) {
        int[] output = new int[inputMatrix.length];
        for (int i = 0; i < inputMatrix.length; i++) {
            long[] longs = inputMatrix[i];
            int sum = 0;
            for (long aLong : longs) {
                sum += Long.bitCount(aLong);
            }
            output[i] = sum;
        }
        return output;
    }

    private static int similarity(long[] x, long[] y, int sizeX, int sizeY) {
        if (x == null || y == null) {
            return 0;
        }
        int sum = 0;
        for (int i = 0; i < x.length; i++) {
            sum += Long.bitCount(x[i] & y[i]);
        }

        return sum;
    }

    private static class FindMax {
        protected int[] dist;
        protected int max;
        protected int maxIndex;

        public FindMax(int[] dist) {
            this.dist = dist;

        }

        public int getMax() {
            return max;
        }

        public int getMaxIndex() {
            return maxIndex;
        }

        public FindMax invoke() {
            max = 0;
            maxIndex = -1;
            for (int i = 0; i < dist.length; i++) {
                if (max < dist[i] || maxIndex < 0) {
                    maxIndex = i;
                    max = dist[i];
                }
            }
            return this;
        }
    }

    private static class FindMaxThread extends FindMax {
        private final int threadNum;

        public FindMaxThread(int threadNum, int[] dist) {
            super(dist);
            this.threadNum = threadNum;
        }

        public FindMaxThread invoke() {
            int perThread = (int) Math.ceil(1.0 * dist.length / threadNum);
            MaxFinderThread[] threads = new MaxFinderThread[threadNum];

            for (int i = 0; i < threadNum; i++) {
                threads[i] = new MaxFinderThread(i, dist, perThread);
                threads[i].start();
            }
            try {
                for (MaxFinderThread thread : threads) {
                    thread.join();
                    if (max < thread.getMaxValue() || maxIndex < 0) {
                        maxIndex = thread.getMaxIndex();
                        max = thread.getMaxValue();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return this;
        }

    }

    private static class MaxFinderThread extends Thread {
        private final int index;
        private final int dist[];
        private final int perThreadNum;
        private int maxValue;
        private int maxIndex;

        private MaxFinderThread(int index, int[] dist, int perThreadNum) {
            this.index = index;
            this.dist = dist;
            this.perThreadNum = perThreadNum;
        }

        public int getMaxValue() {
            return maxValue;
        }

        public int getMaxIndex() {
            return maxIndex;
        }

        @Override
        public void run() {
            maxValue = 0;
            maxIndex = -1;
            int max = Math.min(dist.length, (index + 1) * perThreadNum);
            for (int i = index * perThreadNum; i < max; i++) {
                if (maxValue < dist[i] || maxIndex < 0) {
                    maxIndex = i;
                    maxValue = dist[i];
                }
            }
        }
    }
}
