package edu.usc.enl.cacheflow.algorithms.feasibility.general;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/19/12
 * Time: 5:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class FeasiblePlacer extends Placer {
    private final int threadNum;
    private Map<Switch, Integer> switchIndexMap;
    private final Map<Switch, Collection<Partition>> sourcePartitions;
    private Map<Switch, Set<Partition>> rplacement;
    private Map<Class<? extends Switch>, Switch> baseSwitches;

    public FeasiblePlacer(boolean checkLinks, Map<Partition, Map<Switch, Rule>> forwardingRules, int threadNum,
                          Map<Switch, Collection<Partition>> sourcePartitions) {
        super(checkLinks, forwardingRules, sourcePartitions, Util.threadNum);
        this.threadNum = threadNum;
        this.sourcePartitions = sourcePartitions;

    }

    @Override
    public Collection<Switch> getLastAvailableSwitches() {
        return topology.getSwitches();
    }

    public Map<Class<? extends Switch>, Switch> getBaseSwitches() {
        return baseSwitches;
    }

    @Override
    public Assignment place2(Topology topology, Collection<Partition> partitions) throws NoAssignmentFoundException {
        List<Switch> considerableSwitches = getConsiderableSwitches(topology);

        final List<Switch> edges = fillRPlacement(topology, considerableSwitches);

        fillSwitchIndexMap(considerableSwitches);

        long start = System.currentTimeMillis();

        initSwitchStates(edges);

        System.out.println("init switches " + (System.currentTimeMillis() - start));

        baseSwitches = createBaseSwitches(considerableSwitches);

        final Collection<PartitionObject> partitionObjects = getPartitionObjects(considerableSwitches);

        System.out.println("partition objects " + (System.currentTimeMillis() - start));

        final UpdateScoreThread[] updateScoreThreads = fillInitialMatrix(considerableSwitches, baseSwitches, partitionObjects);

        System.out.println("initial matrix " + (System.currentTimeMillis() - start));

        final Map<Switch, Set<PartitionObject>> oneChoicePartitions = Collections.synchronizedMap(
                new HashMap<Switch, Set<PartitionObject>>(partitionObjects.size(), 1));

        //select the minimum partition and place it

        SeedPartitionObjectFinder seedPartitionObjectFinder = new SeedPartitionObjectFinder(considerableSwitches, partitionObjects,
                oneChoicePartitions, switchIndexMap, threadNum);
        int filledSwitch = 0;
        int placedByNow = 0;
        while (partitionObjects.size() > 0) {
            //find the one with minimum maximum similarity
            filledSwitch++;
            seedPartitionObjectFinder.invoke();
            PartitionObject partitionObject = seedPartitionObjectFinder.getPartitionObject();
            int partitionObjectMaxSwitchIndex = seedPartitionObjectFinder.getPartitionObjectMaxSwitchIndex();
            if (partitionObject == null) {
                throw new RuntimeException("partition object is null");
            }
            final Switch candidateSwitch = considerableSwitches.get(partitionObjectMaxSwitchIndex);
            final Set<Partition> currentSwitchPartitions = rplacement.get(candidateSwitch);
            final Set<PartitionObject> oneChoicePartitionsOnCandidateSwitch = oneChoicePartitions.get(candidateSwitch);
            while (true) {
                partitionObjects.remove(partitionObject);
                placedByNow++;
                System.out.println(placedByNow + " partitions placed on : " + filledSwitch + " switches " +
                        partitionObject.getPartition().getId() + " --> " + candidateSwitch);
                placeAndUpdate(considerableSwitches, updateScoreThreads,
                        currentSwitchPartitions, candidateSwitch, partitionObject, partitionObjects);

//                if (candidateSwitch instanceof OVSSwitch) {
//                    final int sum = flowsNum(candidateSwitch, rplacement.get(candidateSwitch));
//                    final int newFlows = ((OVSSwitch.OVSState) candidateSwitch.getState()).getNewFlows();
//                    if (newFlows != sum) {
//                        System.out.println();
//                    }
//                }

                if (partitionObjects.size() == 0) {
                    break;
                }

                PartitionObject maxSimilarPartitionObject = getNextPartitionObject(partitionObjects, oneChoicePartitions,
                        partitionObjectMaxSwitchIndex, candidateSwitch, oneChoicePartitionsOnCandidateSwitch);

                if (maxSimilarPartitionObject == null) {
                    break;
                }
                partitionObject = maxSimilarPartitionObject;
            }
        }
        for (Switch aSwitch : baseSwitches.values()) {
            rplacement.remove(aSwitch);
        }
        final Assignment assignment = Assignment.getAssignment(rplacement);
        assignment.updateForwardingRules(forwardingRules);
        return assignment;
    }

    public static int flowsNum(Switch s, Collection<Partition> hostedOnS,Topology topology) {
        System.out.println(s);
        int sum = 0;
        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry0 : topology.ruleFlowMap.entrySet()) {
            for (Map.Entry<Rule, Collection<Flow>> entry : entry0.getValue().entrySet()) {
                for (Flow flow : entry.getValue()) {
                    if (!flow.isNew()){
                        continue;
                    }
                    boolean isSrc = flow.getSource().equals(s);
                    boolean isHosted = hostedOnS != null && hostedOnS.contains(entry0.getKey());
                    boolean isDst = flow.getDestination().equals(s) && entry.getKey().getAction().doAction(flow) != null;
                    if (isSrc) {
                        sum++;
                        System.out.println("src "+flow);
                    }
                    if (isHosted && !isSrc) {
                        sum++;
                        System.out.println("host "+flow);
                    }
                    if (!isHosted && isDst) {
                        sum++;
                        System.out.println("dst "+flow);
                    }
                }
            }
        }
        return sum;
    }

    private PartitionObject getNextPartitionObject(Collection<PartitionObject> partitionObjects,
                                                   Map<Switch, Set<PartitionObject>> oneChoicePartitions, int partitionObjectMaxSwitchIndex,
                                                   Switch candidateSwitch, Set<PartitionObject> oneChoicePartitionsOnCandidateSwitch) {
        PartitionObject maxSimilarPartitionObject = null;
        if (oneChoicePartitionsOnCandidateSwitch != null && oneChoicePartitionsOnCandidateSwitch.size() > 0) {
            //get onechoice
            final Iterator<PartitionObject> ocpocsItr = oneChoicePartitionsOnCandidateSwitch.iterator();
            maxSimilarPartitionObject = ocpocsItr.next();
            ocpocsItr.remove();
            if (!ocpocsItr.hasNext()) {
                oneChoicePartitions.remove(candidateSwitch);
            }
        } else {
            //now find those that are mostly similar with the current switch

            maxSimilarPartitionObject = getMaxSimilar(partitionObjects, partitionObjectMaxSwitchIndex, candidateSwitch, maxSimilarPartitionObject);
        }
        return maxSimilarPartitionObject;
    }

    private PartitionObject getMaxSimilar(Collection<PartitionObject> partitionObjects, int partitionObjectMaxSwitchIndex,
                                          Switch candidateSwitch, PartitionObject maxSimilarPartitionObject) {
        double maxSimilarValue = -Double.MAX_VALUE;
        for (PartitionObject partitionObject1 : partitionObjects) {
            final double value = partitionObject1.getSwitchSimilarity(partitionObjectMaxSwitchIndex, candidateSwitch);
            if (partitionObject1.getSwitchFeasible(partitionObjectMaxSwitchIndex) && (maxSimilarPartitionObject == null || maxSimilarValue < value)) {
                maxSimilarPartitionObject = partitionObject1;
                maxSimilarValue = value;
            }
        }
        return maxSimilarPartitionObject;
    }

    private UpdateScoreThread[] fillInitialMatrix(List<Switch> considerableSwitches, Map<Class<? extends Switch>, Switch> baseSwitches,
                                                  Collection<PartitionObject> partitionObjects) {
        //rank each feasible place per partition based on % of resource usage of the whole network
        final UpdateScoreThread[] updateScoreThreads = new UpdateScoreThread[threadNum];
        {
            //go through all
            final Iterator<PartitionObject> iterator = partitionObjects.iterator();
            for (int i = 0; i < threadNum; i++) {
                updateScoreThreads[i] = new UpdateScoreThread(iterator, considerableSwitches, null, this);
            }

            Util.runThreads(updateScoreThreads);
        }
        return updateScoreThreads;
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

    Map<Switch, Set<Partition>> getRplacement() {
        return rplacement;
    }

    @Override
    public String toString() {
        return "FeasiblePlacer";
    }

    private void placeAndUpdate(List<Switch> considerableSwitches,
                                UpdateScoreThread[] threads,
                                Set<Partition> currentSwitchPartition,
                                Switch candidateSwitch, PartitionObject partitionObject,
                                Collection<PartitionObject> partitionObjects) {
        //place it
        final Partition partition = partitionObject.getPartition();
        try {
            place(partitionObject, candidateSwitch, true);
        } catch (Switch.InfeasibleStateException e) {
            e.printStackTrace();
            throw new RuntimeException("Not feasible min switch " + candidateSwitch + " for " + partition);
        }
        currentSwitchPartition.add(partition);
        //update all partitions that have the changed switches in their list

        {
            //go through remaining
            final Iterator<PartitionObject> iterator = partitionObjects.iterator();
            for (int i = 0; i < threadNum; i++) {
                threads[i] = new UpdateScoreThread(iterator, considerableSwitches, candidateSwitch, this);
            }
            Util.runThreads(threads);
        }
    }

    private Collection<PartitionObject> getPartitionObjects(List<Switch> considerableSwitches) {
        final Collection<PartitionObject> partitionObjects = new HashSet<>(forwardingRules.size(), 1);
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

    public double getScore(PartitionObject partitionObject, Switch s) throws Switch.InfeasibleStateException {

        Partition p = partitionObject.getPartition();

        final double newScore = topology.getHelper(s).resourceUsage(s, p, forwardingRules.get(p).keySet(), true);
        return newScore - s.getUsedResources(s.getState());
    }

    //THIS CANNOT BE STATIC, OK!?
    public Switch.FeasibleState place(PartitionObject partitionObject, Switch s, boolean commitAndCheck) throws Switch.InfeasibleStateException {
        Partition p = partitionObject.getPartition();
        return topology.getHelper(s).isAddFeasible(s, p, forwardingRules.get(p).keySet(), commitAndCheck, commitAndCheck);
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
