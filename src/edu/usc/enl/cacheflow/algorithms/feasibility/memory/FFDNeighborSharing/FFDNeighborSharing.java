package edu.usc.enl.cacheflow.algorithms.feasibility.memory.FFDNeighborSharing;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

public class FFDNeighborSharing {
    private final LinkedList<PartitionObject> mainInputMatrix;

    public FFDNeighborSharing(List<Partition> partitions) {
        this.mainInputMatrix = getPartitionObjects(partitions);
    }

    public static Map<Integer, Integer> extractMachineCapacities(Topology topology, int vmPerMachine) {
        Map<Integer, Integer> output = new HashMap<Integer, Integer>();
        Set<Switch> edges = new HashSet<Switch>(topology.findEdges());
        for (Switch aSwitch : topology.getSwitches()) {
            MemorySwitch memorySwitch = (MemorySwitch) aSwitch;
            int capacity = memorySwitch.getMemoryCapacity();
            if (edges.contains(aSwitch)) {
                capacity -= vmPerMachine;//for forwarding rules
            }
            if (capacity > 0) {
                Integer num = output.get(capacity);
                if (num == null) {
                    output.put(capacity, 1);
                } else {
                    output.put(capacity, num + 1);
                }
            }
        }
        return output;
    }

    public List<Machine> ffdNeighborhoodSharing(boolean full, boolean maxFirst, Map<Integer, Integer> machineCapacityNum) throws NoAssignmentFoundException {
        //List<GenerateLinkage.IntegerWrapper> machines = new LinkedList<GenerateLinkage.IntegerWrapper>();
        LinkedList<PartitionObject> inputMatrix = (LinkedList<PartitionObject>) mainInputMatrix.clone();
        TreeMap<Integer, Integer> machineCapacityNumDecreasing = new TreeMap<Integer, Integer>(Collections.reverseOrder());
        machineCapacityNumDecreasing.putAll(machineCapacityNum);

        MatrixRuleSet rightTemp = new MatrixRuleSet();
        MatrixRuleSet leftTemp = new MatrixRuleSet();
        List<Machine> machines = new LinkedList<Machine>();
        while (inputMatrix.size() > 0) {
            int max;
            int maxIndex;
            if (maxFirst) {
                //find maximum
                max = -1;
                maxIndex = -1;
                int i = 0;
                for (PartitionObject partitionObject : inputMatrix) {
                    if (max < partitionObject.size() || maxIndex < 0) {
                        max = partitionObject.size();
                        maxIndex = i;
                    }
                    i++;
                }
            } else {
                max = inputMatrix.getFirst().size();
                maxIndex = 0;
            }
            ListIterator<PartitionObject> inputItr = inputMatrix.listIterator(maxIndex);
            PartitionObject candidateVM = inputItr.next();
            Machine machine = null;
            if (full) {
                //search current machines that have empty capacity
                int maxSimilarity = -1;
                for (Machine testMachine : machines) {
                    if (testMachine.size() < testMachine.capacity) {
                        int similarity = testMachine.memory.getSimilarity(candidateVM.matrix);
                        int sum = candidateVM.size() + testMachine.size() - similarity;
                        if (sum <= testMachine.capacity && (maxSimilarity < similarity || maxSimilarity < 0)) {
                            machine = testMachine;
                            maxSimilarity = similarity;
                            //break;
                        }
                    }
                }
                if (machine != null) {
                    machine.memory.addAll(candidateVM.matrix);
                    machine.partitions.add(candidateVM);
                }
            }
            if (machine == null) {
                if (machineCapacityNumDecreasing.size() == 0) {
                    throw new NoAssignmentFoundException();
                }
                Map.Entry<Integer, Integer> entry = machineCapacityNumDecreasing.firstEntry();
                if (max > entry.getKey()) {
                    throw new NoAssignmentFoundException();
                }
                if (entry.getValue() > 1) {
                    machineCapacityNumDecreasing.put(entry.getKey(), entry.getValue() - 1);
                } else {
                    machineCapacityNumDecreasing.pollFirstEntry();
                }
                machine = new Machine(entry.getKey(), new MatrixRuleSet());
                machines.add(machine);
                machine.add(candidateVM);
            }
            inputItr.remove();

            //now do it for its neighbors
            while (machine.size() <= machine.capacity) {
                //find best neighbor
                //right neighbor
                int rightSize = -1;
                PartitionObject right = null;
                PartitionObject left = null;
                if (inputItr.hasNext()) {
                    right = inputItr.next();
                    rightTemp.copy(machine.memory);
                    rightTemp.addAll(right.matrix);
                    if (rightTemp.size() <= machine.capacity) {
                        rightSize = rightTemp.size();
                    }
                    inputItr.previous(); //go back
                }
                int leftSize = -1;
                if (inputItr.hasPrevious()) {
                    left = inputItr.previous();
                    leftTemp.copy(machine.memory);
                    leftTemp.addAll(left.matrix);
                    if (leftTemp.size() <= machine.capacity) {
                        leftSize = leftTemp.size();
                    }
                    inputItr.next(); //go back
                }
                if (rightSize < 0 && leftSize < 0) {
                    //no need to bring the inputir back to its position
                    break;
                } else if (rightSize > 0 && leftSize < 0) {
                    machine.copy(rightTemp);
                    machine.partitions.add(right);
                    inputItr.next();
                    inputItr.remove();
                } else if (leftSize > 0 && rightSize < 0) {
                    machine.copy(leftTemp);
                    machine.partitions.add(left);
                    inputItr.previous();
                    inputItr.remove();
                } else {//find the one that has maximum similarity
                    int rightSimilarity = right.size() - rightSize + machine.size();
                    int leftSimilarity = left.size() - leftSize + machine.size();
                    if (rightSimilarity >= leftSimilarity) {
                        machine.copy(rightTemp);
                        machine.partitions.add(right);
                        inputItr.next();
                        inputItr.remove();
                    } else {
                        machine.copy(leftTemp);
                        machine.partitions.add(left);
                        inputItr.previous();
                        inputItr.remove();
                    }
                }
            }
        }
        return machines;
    }

    private LinkedList<PartitionObject>  getPartitionObjects(List<Partition> partitions) {
        LinkedList<PartitionObject> inputMatrix = new LinkedList<PartitionObject>();
        for (Partition partition : partitions) {
            final MatrixRuleSet rules = new MatrixRuleSet();
            rules.addAll(partition.getRules());
            inputMatrix.add(new PartitionObject(partition.getId(), rules));
        }
        return inputMatrix;
    }

    public static class Machine {
        private final int capacity;
        private MatrixRuleSet memory;
        private List<PartitionObject> partitions = new LinkedList<PartitionObject>();

        public Machine(int capacity, MatrixRuleSet memory) {
            this.capacity = capacity;
            this.memory = memory;
        }

        public void add(PartitionObject p) {
            memory.addAll(p.matrix);
            partitions.add(p);
        }

        public void copy(MatrixRuleSet m) {
            memory.copy(m);
        }

        public int size() {
            return memory.size();
        }

        public List<PartitionObject> getPartitions() {
            return partitions;
        }
    }

    public static class PartitionObject {
        private final int id;
        private final MatrixRuleSet matrix;

        public PartitionObject(int id, MatrixRuleSet matrix) {
            this.id = id;
            this.matrix = matrix;
        }

        public int size() {
            return matrix.size();
        }

        public int getId() {
            return id;
        }
    }
}