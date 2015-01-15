package edu.usc.enl.cacheflow.algorithms.feasibility.memory.compare;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.scripts.preliminaries.cluster.hierarchical.GenerateLinkage;
import edu.usc.enl.cacheflow.util.Util;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/28/12
 * Time: 7:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class CompareFeasibleSolution {
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        String inputMatrixFile = "input\\nsdi\\classbenchpartition\\vmstart\\imc\\matrix\\m_20480_classbench_131072_2.txt";
        String linkageFile = "input\\nsdi\\classbenchpartition\\vmstart\\imc\\matrix\\lc_20480_classbench_131072_2.txt";
        String placementFile = "input\\nsdi\\classbenchfeasible\\-3\\20480_classbench_131072_2";

        loadAndRun(inputMatrixFile, linkageFile, placementFile);
    }

    public static void loadAndRun(String inputMatrixFile, String linkageFile, String placementFolder
    ) throws IOException, ClassNotFoundException {
        new File(placementFolder).mkdirs();

        int[][] z;
        int[] sizes;
        int rulesSize;
        long[][] inputMatrix;
        Map<String, Object> parameters;
        {
            ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(inputMatrixFile)));
            parameters = (Map<String, Object>) ois.readObject();
            int partitionsSize = ois.readInt();
            rulesSize = ois.readInt();
            int longNumbers = (int) Math.ceil(1.0 * rulesSize / 64);
            inputMatrix = new long[partitionsSize][longNumbers];
            GenerateLinkage.load(ois, inputMatrix);

        }
//        int machineSize = 7;
//        inputMatrix = new long[][]{
//                new long[]{255 - 2},
//                new long[]{255 - 1},
//                new long[]{2047 - 15},
//                new long[]{2047 - 15 - 256},
//                new long[]{2048 + 128},
//        };
//        partitionsSize = 5;
//        rulesSize = 12;
//        String linkageFile = "tempLinkage.txt";
        {
            ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(linkageFile)));
            ois.readObject();//skip dist
            z = (int[][]) ois.readObject();
            sizes = (int[]) ois.readObject();
            ois.close();
        }

//        for (int[] ints : z) {
//            System.out.println(ints[0] + "," + ints[1] + "," + ints[2]);
//        }

        int max = sizes[0];
        for (int i = 1; i < sizes.length; i++) {
            max = Math.max(sizes[i], max);
        }
        System.out.println("max sizes= " + max + ", rule size= " + rulesSize);

        int stepsNum = 11;
        runForMachineSizes(z, sizes, rulesSize, max, stepsNum, placementFolder, parameters, inputMatrix);
    }

    private static void runForMachineSizes(int[][] z, int[] sizes, int rulesSize, int max, int stepsNum, String placementFolder, Map<String, Object> parameters,
                                           long[][] inputMatrix) {
        System.out.println("MachineSize,AddedSize,Coverage,LB,Greedy,Unique,Opt2,Oblivious,ffdSharing,ffdFullSharing,ffdNeighborhoodSharing,ffdFullNeighborSharing,ffNeighborsharing," +
                "LBMem,GreedyMem,UniqueMem,LowerBound,ObliviousMem,ffdSharingMem,ffdFullSharingMem,ffdNeighborhoodSharingMem,ffdFullNeighborSharingMem,ffNeighborsharingMem," +
                "ffdfullsharingduration,ffdNeighborhoodSharingduration,ffdFullNeighborSharingduration,ffneighborSharingduration");
        TreeSet<Integer> machineSizes = new TreeSet<Integer>();
        fillMachineSizesLog(rulesSize, max, stepsNum, machineSizes);

        int[] vmUnionSizeForRules = vmUnionSizeForRules(inputMatrix, rulesSize);


        int obliviousMemory = 0;
        for (int size : sizes) {
            obliviousMemory += size;
        }

        int[] sortedSizesIndex = sortToIndex(sizes);


        int[] nonSharingVms = null;
        int[] sortedNonSharingVMIndex = null;
        for (Integer machineSize : machineSizes) {

            InternalNode root = fillTree(machineSize, z, sizes);
            System.out.print(machineSize + "," + (machineSize - max));
            //compute coverage
            long coverage = getCoverage(root);
            System.out.print("," + 1.0 * rulesSize / coverage);

            //fill nonSharingVms
            if (nonSharingVms == null) {
                nonSharingVms = fillNonSharingVms(root, sizes.length);
                Arrays.sort(nonSharingVms);
                sortedNonSharingVMIndex = new int[nonSharingVms.length];
                for (int i = 0; i < nonSharingVms.length; i++) {
                    sortedNonSharingVMIndex[i] = i;
                }
            }

            //fill count and size
            computeCountSize2(root);

            System.out.print("," + root.getCount());
            int lbMem = root.getSize();

            //run greedy and save it to file
            List<AbstractNode> greedy = greedy(root);
            int greedyMem = 0;
            try {
                PrintWriter writer = new PrintWriter(placementFolder + "/" + machineSize + ".txt");
                writer.println(Statistics.getParameterLine(parameters));
                for (AbstractNode abstractNode : greedy) {
                    writer.print(machineSize);
                    greedyMem += machineSize - (abstractNode.getCap() - abstractNode.getSize());
                    PostOrderIterator itr = new PostOrderIterator(abstractNode);
                    while (itr.hasNext()) {
                        AbstractNode next = itr.next();
                        if (next instanceof VMNode) {
                            writer.print("," + (next.getId() + 1));
                        }
                    }
                    writer.println();
                }
                writer.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }


            //save other stats
            System.out.print("," + greedy.size());
            System.out.print("," + Math.ceil(1.0 * rulesSize / machineSize));
            System.out.print("," + ffd(sortedNonSharingVMIndex, nonSharingVms, machineSize));
            System.out.print("," + ffd(sortedSizesIndex, sizes, machineSize));
            int[] machinesFfdSharing = ffdSharing(sortedSizesIndex, inputMatrix, sizes, machineSize, false);
            long start = System.currentTimeMillis();
            int[] machinesFfdFullSharing = ffdSharing(sortedSizesIndex, inputMatrix, sizes, machineSize, true);
            long duration = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            int[] machinesFfdNeighborSharing = ffdNeighborhoodSharing(sizes, inputMatrix, machineSize, false, true);
            long durationNeighbor = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            int[] machinesFfdFullNeighborSharing = ffdNeighborhoodSharing(sizes, inputMatrix, machineSize, true, true);
            long durationFullNeighbor = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            int[] machinesFfNeighborSharing = ffdNeighborhoodSharing(sizes, inputMatrix, machineSize, false, false);
            long durationNoFNeighbor = System.currentTimeMillis() - start;
            System.out.print("," + machinesFfdSharing.length);
            System.out.print("," + machinesFfdFullSharing.length);
            System.out.print("," + machinesFfdNeighborSharing.length);
            System.out.print("," + machinesFfdFullNeighborSharing.length);
            System.out.print("," + machinesFfNeighborSharing.length);
            //memory
            System.out.print("," + lbMem);
            System.out.print("," + greedyMem);
            System.out.print("," + rulesSize);


            //find lowerbound memory usage
            {
                int lowerBoundMem = 0;
                for (int vmUnionSizeForRule : vmUnionSizeForRules) {
                    lowerBoundMem += Math.ceil(1.0 * vmUnionSizeForRule / machineSize);
                }
                System.out.print("," + lowerBoundMem);
            }
            System.out.print("," + obliviousMemory);
            System.out.print("," + calculateMemorySum(machinesFfdSharing));
            System.out.print("," + calculateMemorySum(machinesFfdFullSharing));
            System.out.print("," + calculateMemorySum(machinesFfdNeighborSharing));
            System.out.print("," + calculateMemorySum(machinesFfdFullNeighborSharing));
            System.out.print("," + calculateMemorySum(machinesFfNeighborSharing));
            System.out.print("," + duration);
            System.out.print("," + durationNeighbor);
            System.out.print("," + durationFullNeighbor);
            System.out.print("," + durationNoFNeighbor);
            System.out.println();

        }
    }

    private static int calculateMemorySum(int[] machinesSizes) {
        int sum = 0;
        for (int machine : machinesSizes) {
            sum += machine;
        }
        return sum;
    }

    private static int[] sortToIndex(int[] sizes) {
        List<IntegerIndex> integerIndexList = new ArrayList<IntegerIndex>(sizes.length);
        for (int i = 0, sizesLength = sizes.length; i < sizesLength; i++) {
            integerIndexList.add(new IntegerIndex(sizes[i], i));
        }
        Collections.sort(integerIndexList);
        int[] indexes = new int[sizes.length];
        for (int i = 0, integerIndexListSize = integerIndexList.size(); i < integerIndexListSize; i++) {
            indexes[i] = integerIndexList.get(i).index;
        }

        return indexes;
    }

    private static class IntegerIndex implements Comparable<IntegerIndex> {
        private int value;
        private int index;

        private IntegerIndex(int value, int index) {
            this.value = value;
            this.index = index;
        }

        @Override
        public int compareTo(IntegerIndex o2) {
            return value - o2.value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IntegerIndex that = (IntegerIndex) o;

            if (index != that.index) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return index;
        }
    }

    private static void fillMachineSizesLinear(int rulesSize, int max, int stepsNum, TreeSet<Integer> machineSizes) {
        int length = rulesSize - max;
        if (stepsNum > 1) {
            double step = length / (stepsNum - 1);
            for (int i = 0; i < stepsNum - 1; i++) {
                machineSizes.add((int) (max + i * step));
            }
        }
        machineSizes.add(rulesSize);
    }

    private static void fillMachineSizesLog(int rulesSize, int max, int stepsNum, TreeSet<Integer> machineSizes) {
        int length = rulesSize - max;
        double length2 = Integer.highestOneBit(length) << 1;
        length2 = Math.log(length2) / Math.log(2);
        double step = length2 / stepsNum;
        for (int i = 0; i < stepsNum - 2 && length2 > 0; i++) {
            length2 -= Math.max(1, step);
            machineSizes.add((int) (max + Math.pow(2, length2)));
        }
        machineSizes.add(rulesSize);
        machineSizes.add(max + 1);
    }

    private static int[] vmUnionSizeForRules(long[][] inputMatrix, int ruleSize) {
        int[] output = new int[ruleSize];
        long[] union = new long[inputMatrix[0].length];
        for (int i = 0; i < ruleSize; i++) {
            Arrays.fill(union, 0);
            for (long[] longs : inputMatrix) {
                if (MatrixRuleSet.hasOneAt(longs, i)) {
                    MatrixRuleSet.union(union, longs);
                }
            }
            int unionSize = MatrixRuleSet.getSize(union);
            output[i] = unionSize;
        }
        return output;
    }

    /**
     * @param nonSharingVms sorted ascending
     * @param machineSize
     * @return
     */
    private static int ffd(int[] indexes, int[] nonSharingVms, Integer machineSize) {
        List<Util.IntegerWrapper> machines = new LinkedList<Util.IntegerWrapper>();
        for (int i = indexes.length - 1; i >= 0; i--) {
            int index = indexes[i];
            int vm = nonSharingVms[index];
            boolean assigned = false;
            for (Util.IntegerWrapper machine : machines) {
                if (machine.getValue() + vm <= machineSize) {
                    machine.setValue(machine.getValue() + vm);
                    assigned = true;
                    break;
                }
            }
            if (!assigned) {
                machines.add(new Util.IntegerWrapper(vm));
            }
        }
        return machines.size();
    }

    private static int[] ffdNeighborhoodSharing(int[] sizes, long[][] inputMatrix, Integer machineSize, boolean full, boolean maxFirst) {
        LinkedList<Integer> sizesLink = new LinkedList<Integer>();
        LinkedList<long[]> inputMatrixLink = new LinkedList<long[]>();
        for (int i = 0; i < sizes.length; i++) {
            sizesLink.add(sizes[i]);
            inputMatrixLink.add(inputMatrix[i]);
        }
        List<Util.IntegerWrapper> machines = new LinkedList<Util.IntegerWrapper>();
        int longsNum = inputMatrix[0].length;
        long[] rightTemp = new long[longsNum];
        long[] leftTemp = new long[longsNum];
        List<long[]> machinesMemory = full ? new LinkedList<long[]>() : null;
        while (sizesLink.size() > 0) {
            int max;
            int maxIndex;
            if (maxFirst) {
                //find maximum
                max = -1;
                maxIndex = -1;
                int i = 0;
                for (Integer size : sizesLink) {
                    if (max < size || maxIndex < 0) {
                        max = size;
                        maxIndex = i;
                    }
                    i++;
                }
            } else {
                max = sizesLink.getFirst();
                maxIndex = 0;
            }
            ListIterator<long[]> inputItr = inputMatrixLink.listIterator(maxIndex);
            long[] candidateVM = inputItr.next();
            Util.IntegerWrapper machine = null;
            long[] machineMemory = null;
            if (full) {
                //search current machines that have empty capacity
                Iterator<long[]> machineMemItr = machinesMemory.iterator();
                int maxSimilarity = -1;
                int maxSum = -1;
                for (Util.IntegerWrapper machine1 : machines) {
                    long[] machineMemory1 = machineMemItr.next();
                    if (machine1.getValue() < machineSize) {
                        int sum = fillUnion(candidateVM, leftTemp, machineMemory1);///just need a temp
                        int similarity = max - sum + machine1.getValue();
                        if (sum <= machineSize && (maxSimilarity < similarity || maxSimilarity < 0)) {
                            machine = machine1;
                            maxSum = sum;
                            machineMemory = machineMemory1;
                            maxSimilarity = similarity;
                            //break;
                        }
                    }
                }
                if (machine != null) {
                    machine.setValue(maxSum);
                    MatrixRuleSet.union(machineMemory, candidateVM);
                }
            }
            if (machine == null) {
                machine = new Util.IntegerWrapper(max);
                machines.add(machine);
                machineMemory = new long[longsNum];
                System.arraycopy(candidateVM, 0, machineMemory, 0, machineMemory.length);
                if (full) {
                    machinesMemory.add(machineMemory);
                }
            }
            ListIterator<Integer> sizesItr = sizesLink.listIterator(maxIndex);
            sizesItr.next();
            sizesItr.remove();
            inputItr.remove();

            //now do it for its neighbors
            while (machine.getValue() <= machineSize) {
                //find best neighbor
                //right neighbor
                int rightSize = -1;
                if (inputItr.hasNext()) {
                    int sum = fillUnion(inputItr.next(), rightTemp, machineMemory);
                    if (sum <= machineSize) {
                        rightSize = sum;
                    }
                    inputItr.previous(); //go back
                }
                int leftSize = -1;
                if (inputItr.hasPrevious()) {
                    int sum = fillUnion(inputItr.previous(), leftTemp, machineMemory);
                    if (sum <= machineSize) {
                        leftSize = sum;
                    }
                    inputItr.next(); //go back
                }
                if (rightSize < 0 && leftSize < 0) {
                    //no need to bring the inputir back to its position
                    break;
                } else if (rightSize > 0 && leftSize < 0) {
                    System.arraycopy(rightTemp, 0, machineMemory, 0, rightTemp.length);
                    machine.setValue(rightSize);
                    inputItr.next();
                    inputItr.remove();
                    sizesItr.next();
                    sizesItr.remove();
                } else if (leftSize > 0 && rightSize < 0) {
                    System.arraycopy(leftTemp, 0, machineMemory, 0, leftTemp.length);
                    machine.setValue(leftSize);
                    inputItr.previous();
                    inputItr.remove();
                    sizesItr.previous();
                    sizesItr.remove();
                } else {//find the one that has maximum similarity
                    int rightSimilarity = sizesItr.next() - rightSize + machine.getValue();
                    sizesItr.previous();
                    int leftSimilarity = sizesItr.previous() - leftSize + machine.getValue();
                    sizesItr.next();
                    if (rightSimilarity >= leftSimilarity) {
                        System.arraycopy(rightTemp, 0, machineMemory, 0, rightTemp.length);
                        machine.setValue(rightSize);
                        inputItr.next();
                        inputItr.remove();
                        sizesItr.next();
                        sizesItr.remove();
                    } else {
                        System.arraycopy(leftTemp, 0, machineMemory, 0, leftTemp.length);
                        machine.setValue(leftSize);
                        inputItr.previous();
                        inputItr.remove();
                        sizesItr.previous();
                        sizesItr.remove();
                    }
                }
            }
        }
        int[] output = new int[machines.size()];
        int i = 0;
        for (Util.IntegerWrapper machine : machines) {
            output[i++] = machine.getValue();
        }
        return output;
    }


    private static int[] ffdSharing(int[] sortedSizesIndex, long[][] inputMatrix, int[] sizes, Integer machineSize, boolean full) {
        List<Util.IntegerWrapper> machines = new LinkedList<Util.IntegerWrapper>();
        List<long[]> machinesMemory = new LinkedList<long[]>();
        long[] temp = new long[inputMatrix[0].length];
        for (int i = sortedSizesIndex.length - 1; i >= 0; i--) {
            int index = sortedSizesIndex[i];
            int vm = sizes[index];
            boolean assigned = false;
            int machineIndex = 0;
            for (Util.IntegerWrapper machine : machines) {
                if (machine.getValue() + vm <= machineSize) {
                    long[] machineMemory = machinesMemory.get(machineIndex);
                    MatrixRuleSet.union(machineMemory, inputMatrix[index]);
                    int sum = MatrixRuleSet.getSize(machineMemory);
                    machine.setValue(sum);
                    assigned = true;
                    break;
                } else {
                    //it can even now accommodate it
                    if (full) {
                        long[] machineMemory = machinesMemory.get(machineIndex);
                        int sum = fillUnion(inputMatrix[index], temp, machineMemory);
                        if (sum <= machineSize) {
                            machine.setValue(sum);
                            System.arraycopy(temp, 0, machineMemory, 0, temp.length);
                            assigned = true;
                            break;
                        }
                    }
                }
                machineIndex++;
            }
            if (!assigned) {
                machines.add(new Util.IntegerWrapper(vm));
                long[] memory = new long[inputMatrix[index].length];
                System.arraycopy(inputMatrix[index], 0, memory, 0, memory.length);
                machinesMemory.add(memory);
            }
        }
        int[] output = new int[machines.size()];
        int index = 0;
        for (Util.IntegerWrapper machine : machines) {
            output[index++] = machine.getValue();
        }
        return output;
    }

    /**
     * output is in temp
     * @param longs
     * @param temp
     * @param machineMemory
     * @return
     */
    public static int fillUnion(long[] longs, long[] temp, long[] machineMemory) {
        System.arraycopy(machineMemory, 0, temp, 0, temp.length);
        MatrixRuleSet.union(temp, longs);
        return MatrixRuleSet.getSize(temp);
    }

    private static int[] fillNonSharingVms(InternalNode root, int vmsNum) {
        int[] nonSharingVms = new int[vmsNum];
        //PostOrderIterator itr = new PostOrderIterator(root, true);
        //need preorder traversal
        LinkedList<AbstractNode> stack = new LinkedList<AbstractNode>();
        stack.add(root);
        int currentSum = 0;
        int vmIndex = 0;
        while (stack.size() > 0) {
            AbstractNode next = stack.pop();
            currentSum += next.getW();
            if (next instanceof VMNode) {
                nonSharingVms[vmIndex++] = currentSum;
                currentSum = 0;
            } else {
                stack.add(((InternalNode) next).getLeft());
                stack.add(((InternalNode) next).getRight());
            }
        }

        return nonSharingVms;
    }

    private static long getCoverage(InternalNode root) {
        long coverage = 0;
        PostOrderIterator itr = new PostOrderIterator(root);
        while (itr.hasNext()) {
            coverage += itr.next().getW();
        }
        return coverage;
    }

    private static InternalNode fillTree(int machineSize, int[][] z, int[] sizes) {
        InternalNode root;
        int vmsNum = sizes.length;
        AbstractNode[] nodes = new AbstractNode[z.length + vmsNum];
        //create vm nodes
        for (int i = 0; i < vmsNum; i++) {
            nodes[i] = new VMNode(i, sizes[i]);//set w later
        }
        VMNode.machineSize = machineSize;

        //create internal nodes
        for (int i = 0; i < z.length; i++) {
            nodes[i + vmsNum] = new InternalNode(i + vmsNum, z[i][2], nodes[z[i][0]], nodes[z[i][1]]);
        }
        z = null;

        root = (InternalNode) nodes[nodes.length - 1];

        //set caps
        root.setCap(machineSize);
        for (int i = nodes.length - 2; i >= vmsNum; i--) {
            InternalNode node = (InternalNode) nodes[i];
            node.setCap(node.getParent().getCap() - node.getParent().getW());
        }
        return root;
    }

    public static List<AbstractNode> greedy(InternalNode root) {
        List<AbstractNode> output = new LinkedList<AbstractNode>();
        if (root.getCount() == 1) {
            output.add(root);
            return output;
        }

        LinkedList<InternalNode> twoOnOnesNodes = new LinkedList<InternalNode>();
        PostOrderIterator itr = new PostOrderIterator(root);
        while (itr.hasNext()) {
            AbstractNode next = itr.next();
            if (next instanceof InternalNode && next.getCount() == 2 &&
                    ((InternalNode) next).getLeft().getCount() == 1 && ((InternalNode) next).getRight().getCount() == 1) {
                twoOnOnesNodes.add((InternalNode) next);
            }
        }

        while (twoOnOnesNodes.size() > 0) {
            //remove first
            InternalNode toPlace = twoOnOnesNodes.pollFirst();
            //we know that no one can place these two in one machine, so creat two machines
            output.add(toPlace.getLeft());
            output.add(toPlace.getRight());
            if (toPlace.getParent() != null) {
                try {
                    InternalNode candidate = toPlace.remove();
                    if (candidate != null) {
                        twoOnOnesNodes.add(candidate);
                    }
                } catch (InternalNode.LastSingleNode e) {
                    output.add(e.getRootNode());
                    break;
                }
            } else {
                break;
            }
        }
        return output;
    }

    /*public static long[] unionLeavesPattern(AbstractNode node) {
        if (node instanceof VMNode) {
            return ((VMNode) node).getPattern();
        }
        PostOrderIterator itr = new PostOrderIterator((InternalNode) node);
        long[] output = null;
        while (itr.hasNext()) {
            AbstractNode next = itr.next();
            if (next instanceof VMNode) {
                VMNode nextVM = (VMNode) next;
                long[] pattern = nextVM.getPattern();
                if (output == null) {
                    output = new long[pattern.length];
                    for (int i = 0, patternLength = pattern.length; i < patternLength; i++) {
                        output[i] = pattern[i];
                    }
                } else {
                    for (int i = 0, patternLength = pattern.length; i < patternLength; i++) {
                        output[i] |= pattern[i];
                    }
                }
            }
        }
        return output;
    }*/

    private static class PostOrderIterator implements Iterator<AbstractNode> {
        private AbstractNode cur;
        private AbstractNode prev;
        private AbstractNode root;

        private PostOrderIterator(AbstractNode root) {
            this.root = root;
            prev = null;
            cur = root;
        }

        @Override
        public boolean hasNext() {
            return cur != null;
        }

        @Override
        public AbstractNode next() {
            while (true) {
                if (cur instanceof VMNode) {
                    prev = cur;
                    if (prev == root) {
                        cur = null;//don't go further than the root
                    } else {
                        cur = cur.getParent();
                    }
                    return prev;
                } else {
                    InternalNode curI = (InternalNode) cur;
                    if (prev == curI.getRight()) {
                        prev = cur;
                        if (prev == root) {
                            cur = null;//don't go further than the root
                        } else {
                            cur = cur.getParent();
                        }
                        return prev;
                    } else if (prev == curI.getLeft()) {
                        prev = cur;
                        cur = curI.getRight();
                    } else {
                        prev = cur;
                        cur = curI.getLeft();
                    }
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static void computeCountSize2(InternalNode root) {
        PostOrderIterator itr = new PostOrderIterator(root);
        while (itr.hasNext()) {
            AbstractNode next = itr.next();
            if (next instanceof InternalNode) {
                InternalNode curI = (InternalNode) next;
                curI.computeCountSize();
            }
        }
    }

}