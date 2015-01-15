package edu.usc.enl.cacheflow.algorithms.feasibility.general;

import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/19/12
 * Time: 5:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class FeasiblePlacer2 extends Placer {
    private final int threadNum;
    private Map<Switch, Integer> switchIndexMap;
    private final Map<Switch, Collection<Partition>> sourcePartitions;
    private Map<Switch, Set<Partition>> rplacement;

    public FeasiblePlacer2(boolean checkLinks, Map<Partition, Map<Switch, Rule>> forwardingRules, int threadNum,
                           Map<Switch, Collection<Partition>> sourcePartitions) {
        super(checkLinks, forwardingRules, sourcePartitions, Util.threadNum);
        this.threadNum = threadNum;
        this.sourcePartitions = sourcePartitions;

    }

    @Override
    public Collection<Switch> getLastAvailableSwitches() {
        return topology.getSwitches();
    }

    @Override
    public Assignment place2(Topology topology, Collection<Partition> partitions) throws NoAssignmentFoundException {
        List<Switch> considerableSwitches = getConsiderableSwitches(topology);

        final List<Switch> edges = fillRPlacement(topology, considerableSwitches);

        fillSwitchIndexMap(considerableSwitches);

        long start = System.currentTimeMillis();

        initSwitchStates(edges);

        //System.out.println("init switches " + (System.currentTimeMillis() - start));


        final List<PartitionObject> partitionObjects = getPartitionObjects(considerableSwitches);

        //System.out.println("partition objects " + (System.currentTimeMillis() - start));



        //select the minimum partition and place it

        SeedPartitionObjectFinder2 seedPartitionObjectFinder = new SeedPartitionObjectFinder2(considerableSwitches, partitionObjects,
                switchIndexMap, threadNum, this);

        List<PartitionObject> sortedPartitionObjects = new ArrayList<>(partitionObjects.size());
        final Map<PartitionObject, Double> emptyScore = new HashMap<>(partitionObjects.size());
        int filledSwitch = 0;
        int placedByNow = 0;
        while (partitionObjects.size() > 0) {
            //find the one with minimum maximum similarity
            filledSwitch++;
            seedPartitionObjectFinder.invoke();
            PartitionObject partitionObject = seedPartitionObjectFinder.getPartitionObject();
            final int partitionObjectMaxSwitchIndex = seedPartitionObjectFinder.getPartitionObjectMaxSwitchIndex();
            if (partitionObject == null) {
                throw new RuntimeException("partition object is null");
            }
            if (partitionObjectMaxSwitchIndex < 0) {
                System.out.println("no assignment found ");
                throw new NoAssignmentFoundException("No feasible placement found for " + partitionObject);
            }
            final Switch candidateSwitch = considerableSwitches.get(partitionObjectMaxSwitchIndex);
            final Set<Partition> currentSwitchPartitions = rplacement.get(candidateSwitch);
            Switch.FeasibleState candidateSwitchOldState = candidateSwitch.getState();
            Switch.FeasibleState candidateSwitchNewState;
            try {
                candidateSwitchNewState = place(partitionObject, candidateSwitch, false);
            } catch (Switch.InfeasibleStateException e) {
                throw new RuntimeException("Not feasible min switch " + candidateSwitch + " for " + partitionObject.getPartition(), e);
            }
            currentSwitchPartitions.add(partitionObject.getPartition());
            partitionObjects.remove(partitionObject);
            placedByNow++;
//            System.out.println(placedByNow + " partitions placed on : " + filledSwitch + " switches " +
//                    partitionObject.getPartition().getId() + " --> " + candidateSwitch);

            //sort partitions based on similarity with the current partition
            emptyScore.clear();
            {
                fillEmptyScores(partitionObjects, candidateSwitch, emptyScore);

                candidateSwitch.setState(candidateSwitchNewState);
                fillJointScores(partitionObjects, candidateSwitch);
            }

            sortedPartitionObjects.clear();
            sortedPartitionObjects.addAll(partitionObjects);
            Collections.sort(sortedPartitionObjects, new Comparator<PartitionObject>() {
                @Override
                public int compare(PartitionObject o1, PartitionObject o2) {
                    return -(Double.compare(emptyScore.get(o1) - o1.getSwitchScore(partitionObjectMaxSwitchIndex), emptyScore.get(o2) - o2.getSwitchScore(partitionObjectMaxSwitchIndex)));
                }
            });

            int sortedPartitionIndex = 0;
            while (sortedPartitionIndex < sortedPartitionObjects.size()) {
                PartitionObject mostSimilarToPlace = sortedPartitionObjects.get(sortedPartitionIndex++);
                if (!mostSimilarToPlace.getSwitchFeasible(partitionObjectMaxSwitchIndex)) {
                    break;
                }
                try {
                    place(mostSimilarToPlace, candidateSwitch, true);
                    currentSwitchPartitions.add(mostSimilarToPlace.getPartition());
                    partitionObjects.remove(mostSimilarToPlace);
                    placedByNow++;

                } catch (Switch.InfeasibleStateException e) {
                    break;
                }
            }
        }
        final Assignment assignment = Assignment.getAssignment(rplacement);
        assignment.updateForwardingRules(forwardingRules);
        return assignment;
    }

    private void fillEmptyScores(List<PartitionObject> partitionObjects, final Switch candidateSwitch, final Map<PartitionObject, Double> emptyScore) {
        //get cost when partitions are placed only
        final Iterator<PartitionObject> itr = partitionObjects.iterator();
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    while (true) {
                        PartitionObject p;
                        synchronized (itr) {
                            if (itr.hasNext()) {
                                p = itr.next();
                            } else {
                                break;
                            }
                        }
                        try {
                            emptyScore.put(p, getScore(p, candidateSwitch, false));
                        } catch (Switch.InfeasibleStateException e) {
                            e.printStackTrace();
                            System.out.println("I set checkresources false so there should be no exception");
                        }
                    }
                }
            };
        }
        Util.runThreads(threads);
    }

    private void fillJointScores(List<PartitionObject> partitionObjects, final Switch candidateSwitch) {
        //go through remaining and get joint score
        final Iterator<PartitionObject> itr = partitionObjects.iterator();
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    while (true) {
                        PartitionObject p;
                        synchronized (itr) {
                            if (itr.hasNext()) {
                                p = itr.next();
                            } else {
                                break;
                            }
                        }
                        double score = Float.MAX_VALUE;
                        final Integer index = getSwitchIndexMap().get(candidateSwitch);
                        try {
                            score = getScore(p, candidateSwitch, true);
                            p.setSwitchFeasible(index, true);
                        } catch (Switch.InfeasibleStateException e) {
                            p.setSwitchFeasible(index, false);
                        }
                        p.setScore(index, (float) score);
                    }
                }
            };
        }
        Util.runThreads(threads);
    }

    public static int flowsNum(Switch s, Collection<Partition> hostedOnS, Topology topology) {
        System.out.println(s);
        int sum = 0;
        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry0 : topology.ruleFlowMap.entrySet()) {
            for (Map.Entry<Rule, Collection<Flow>> entry : entry0.getValue().entrySet()) {
                for (Flow flow : entry.getValue()) {
                    boolean isSrc = flow.getSource().equals(s);
                    boolean isHosted = hostedOnS != null && hostedOnS.contains(entry0.getKey());
                    boolean isDst = flow.getDestination().equals(s) && entry.getKey().getAction().doAction(flow) != null;
                    if (isSrc) {
                        sum++;
                        System.out.println("src " + flow);
                    }
                    if (isHosted && !isSrc) {
                        sum++;
                        System.out.println("host " + flow);
                    }
                    if (!isHosted && isDst) {
                        sum++;
                        System.out.println("dst " + flow);
                    }
                }
            }
        }
        return sum;
    }

    private List<Switch> fillRPlacement(Topology topology, List<Switch> considerableSwitches) {
        rplacement = new HashMap<>(topology.getSwitches().size(), 1);
        for (Switch considerableSwitch : considerableSwitches) {
            rplacement.put(considerableSwitch, new HashSet<Partition>());
        }
        final List<Switch> edges = topology.findEdges();
        for (Switch edge : edges) {
            rplacement.put(edge, new HashSet<Partition>());
        }
        return edges;
    }

    private Map<Class<? extends Switch>, Switch> createBaseSwitches(List<Switch> considerableSwitches) {
        Map<Class<? extends Switch>, Switch> baseSwitches = new HashMap<>();
        for (Switch considerableSwitch : considerableSwitches) {
            if (!baseSwitches.containsKey(considerableSwitch.getClass())) {
                baseSwitches.put(considerableSwitch.getClass(), considerableSwitch.cloneEmpty(considerableSwitch.getClass().getSimpleName()));
            }
        }
        for (Switch baseSwitch : baseSwitches.values()) {
            rplacement.put(baseSwitch, new HashSet<Partition>());
        }
        return baseSwitches;
    }


    @Override
    public String toString() {
        return "FeasiblePlacer2";
    }

    private List<PartitionObject> getPartitionObjects(List<Switch> considerableSwitches) {
        final List<PartitionObject> partitionObjects = new ArrayList<>(forwardingRules.size());
        for (Map.Entry<Partition, Map<Switch, Rule>> entry : forwardingRules.entrySet()) {
            partitionObjects.add(new PartitionObject(considerableSwitches, entry.getKey(), entry.getValue().keySet().iterator().next()));
        }
        return partitionObjects;
    }

    private void fillSwitchIndexMap(List<Switch> considerableSwitches) {
        switchIndexMap = new HashMap<>(considerableSwitches.size(), 1);
        {
            int i = 0;
            for (Switch considerableSwitch : considerableSwitches) {
                switchIndexMap.put(considerableSwitch, i);
                i++;
            }
        }
    }

    public double getScore(PartitionObject partitionObject, Switch s, boolean checkResources) throws Switch.InfeasibleStateException {

        Partition p = partitionObject.getPartition();

        final double newScore = topology.getHelper(s).resourceUsage(s, p, forwardingRules.get(p).keySet(), checkResources);
        return newScore - s.getUsedResources(s.getState());
    }

    //THIS CANNOT BE STATIC, OK!?
    public Switch.FeasibleState place(PartitionObject partitionObject, Switch s, boolean commit) throws Switch.InfeasibleStateException {
        Partition p = partitionObject.getPartition();
        return topology.getHelper(s).isAddFeasible(s, p, forwardingRules.get(p).keySet(), true, commit);
    }

    protected Map<Switch, Integer> getSwitchIndexMap() {
        return switchIndexMap;
    }

    private List<Switch> getConsiderableSwitches(Topology topology) {
        final List<Switch> topologySwitches = topology.getSwitches();
        List<Switch> considerableSwitches = new ArrayList<Switch>(topologySwitches.size());
        int minPartitionSize = Integer.MAX_VALUE;
        for (Partition partition : forwardingRules.keySet()) {
            if (partition.getSize() < minPartitionSize) {
                minPartitionSize = partition.getSize();
            }
        }
        for (Switch aSwitch : topologySwitches) {
            if (aSwitch instanceof MemorySwitch && ((MemorySwitch) aSwitch).getMemoryCapacity() >= minPartitionSize) {
                considerableSwitches.add(aSwitch);
            } else {
                considerableSwitches.add(aSwitch);
            }
        }
        return considerableSwitches;
    }

    private void initSwitchStates(Collection<Switch> edges) throws NoAssignmentFoundException {
        try {
            for (Switch edge : edges) {
                topology.getHelper(edge).initToNotOnSrc(edge, sourcePartitions.get(edge), true);
            }
        } catch (Switch.InfeasibleStateException e) {
            System.out.println("A machine is not OK with forwarding rules and not src flows");
            throw new NoAssignmentFoundException(e);
        }
    }
}
