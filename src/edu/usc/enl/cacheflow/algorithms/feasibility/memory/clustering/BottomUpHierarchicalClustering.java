package edu.usc.enl.cacheflow.algorithms.feasibility.memory.clustering;

import edu.usc.enl.cacheflow.util.Util;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/19/12
 * Time: 3:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class BottomUpHierarchicalClustering {
    private long[][] inputMatrix;

    public BottomUpHierarchicalClustering(long[][] inputMatrix) {
        this.inputMatrix = inputMatrix;
    }

    public int[][] linkage( int[] dist) {
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
            FindMin findMin = new FindMinThread(6, dist).invoke();
            int minIndex = findMin.getMinIndex();
            int min = findMin.getMin();
            converter.compute(minIndex, m);
            int i = converter.getI();
            int j = converter.getJ();
            z[s] = new int[]{R[i], R[j], min};
            System.out.println(s + ": merge " + i + "(" + R[i] + "), " + j + "(" + R[j] + "), " + min);
            for (int i1 = 0; i1 < inputMatrix[i].length; i1++) {
                inputMatrix[i][i1] = inputMatrix[i][i1] | inputMatrix[j][i1];
            }
            //update distance function
            inputMatrix[j] = null;
            updateDistances( dist, m, i);
            updateDistances( dist, m, j);
            R[i] = n + s;
            /*for (int j1 = 0; j1 < j - 1; j1++) {
                dist[getIndex(j1, j, m)] = Integer.MAX_VALUE;
            }
            for (int j1 = j + 1; j1 < m; j1++) {
                dist[getIndex(j, j1, m)] = Integer.MAX_VALUE;
            }*/
        }
        return z;
    }

    private void updateDistances( int[] dist, int m, int i) {
        for (int i1 = 0; i1 < i; i1++) {
            dist[getIndex(i1, i, m)] = distance(inputMatrix[i], inputMatrix[i1]);
        }
        for (int i1 = i + 1; i1 < m; i1++) {
            dist[getIndex(i, i1, m)] = distance(inputMatrix[i], inputMatrix[i1]);
        }
    }

    private int getIndex(int i, int j, int m) {
        i = i + 1;
        j = j + 1;
        return (int) (i * (m - (i + 1) / 2.0) - m + j) - 1;
    }

    public void computeDistanceThread( final int[] dist) {
        final int n = inputMatrix.length; //for brevity!
        final int threadNum = 6;
        Thread[] threads = new Thread[threadNum];
        final Util.IntegerWrapper index = new Util.IntegerWrapper(0);
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
                        if (i % 100 == 0) {
                            System.out.println(i + " in " + threadId);
                        }
                        for (int j = i + 1; j < n; j++) {
                            dist[getIndex(i, j, n)] = distance(inputMatrix[i], inputMatrix[j]);
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

    private void computeDistance( int[] dist) {
        int n = inputMatrix.length; //for brevity!
        for (int i = 0; i < n - 1; i++) {
            if (i % 1000 == 0) {
                System.out.println(i);
            }
            for (int j = i + 1; j < n; j++) {
                dist[getIndex(i, j, n)] = distance(inputMatrix[i], inputMatrix[j]);
            }
            //Y(k:(k + n - i - 1))=feval(distfun, X(i,:),X((i + 1):n,:),distargs {:})';
        }
    }

    public int distance(long[] x, long[] y) {
        if (x == null || y == null) {
            return Integer.MAX_VALUE;
        }
        int sum = 0;
        for (int i = 0; i < x.length; i++) {
            sum += Long.bitCount(x[i] | y[i]);
        }

        return sum;
    }

    private class FindMin {
        protected int[] dist;
        protected int min;
        protected int minIndex;

        public FindMin(int[] dist) {
            this.dist = dist;

        }

        public int getMin() {
            return min;
        }

        public int getMinIndex() {
            return minIndex;
        }

        public FindMin invoke() {
            min = 0;
            minIndex = -1;
            for (int i = 0; i < dist.length; i++) {
                if (min > dist[i] || minIndex < 0) {
                    minIndex = i;
                    min = dist[i];
                }
            }
            return this;
        }
    }

    //runs multiple threads on an array and gets the minimum of the value of them
    //this is a method extract object
    private class FindMinThread extends FindMin {

        private int threadNum;

        public FindMinThread(int threadNum, int[] dist) {
            super(dist);
            this.threadNum = threadNum;
        }

        public FindMinThread invoke() {
            int perThread = (int) Math.ceil(1.0 * dist.length / threadNum);
            MinFinderThread[] threads = new MinFinderThread[threadNum];

            for (int i = 0; i < threadNum; i++) {
                threads[i] = new MinFinderThread(i, dist, perThread);
                threads[i].start();
            }
            try {
                for (MinFinderThread thread : threads) {
                    thread.join();
                    if (min > thread.getMin() || minIndex < 0) {
                        minIndex = thread.getMinIndex();
                        min = thread.getMin();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return this;
        }
    }

    private class MinFinderThread extends Thread {
        private final int index;
        private final int dist[];
        private final int perThreadNum;
        private int min;
        private int minIndex;

        private MinFinderThread(int index, int[] dist, int perThreadNum) {
            this.index = index;
            this.dist = dist;
            this.perThreadNum = perThreadNum;
        }

        public int getMin() {
            return min;
        }

        public int getMinIndex() {
            return minIndex;
        }

        @Override
        public void run() {
            min = 0;
            minIndex = -1;
            int max = Math.min(dist.length, (index + 1) * perThreadNum);
            //System.out.println("start: " + index * perThreadNum + ", end: " + max + " size: " + dist.length);
            for (int i = index * perThreadNum; i < max; i++) {
                if (min > dist[i] || minIndex < 0) {
                    minIndex = i;
                    min = dist[i];
                }
            }
        }
    }

    private class Converter {
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

    public abstract class ComputThread extends Thread {
        protected int threadId;
        protected Util.IntegerWrapper index;

        protected ComputThread(int threadId, Util.IntegerWrapper index) {
            this.threadId = threadId;
            this.index = index;
        }
    }
}
