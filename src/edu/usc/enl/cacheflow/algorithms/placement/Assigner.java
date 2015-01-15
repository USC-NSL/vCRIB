package edu.usc.enl.cacheflow.algorithms.placement;

import edu.usc.enl.cacheflow.algorithms.placement.partitionselection.PartitionSorter;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.SwitchSelection;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchhelper.SwitchHelper;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 9/22/11
 * Time: 2:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class Assigner extends Placer {

    private final SwitchSelection switchSelection;
    private final PartitionSorter partitionSorter;
    protected final int maxBacktrack;
    private List<Switch> lastAvailableSwitches;


    public Assigner(SwitchSelection switchSelection, PartitionSorter partitionSorter,
                    int maxBacktrack, boolean checkLinks, Map<Partition, Map<Switch, Rule>> forwardingRules, Map<Switch, Collection<Partition>> sourcePartitions) {
        super(checkLinks, forwardingRules, sourcePartitions, Util.threadNum);
        this.maxBacktrack = maxBacktrack;
        this.partitionSorter = partitionSorter;
        this.switchSelection = switchSelection;

    }

    @Override
    public String toString() {
        return "Assigner";
    }

    public boolean isFeasible(Partition p, Switch s,
                              Map<Link, Long> oldTraffic,
                              Map<Switch, Switch.FeasibleState> toKeepSwitchStates) {
        final SwitchHelper<Switch> helper = topology.getHelper(s);
        final Switch.FeasibleState feasible;
        try {
            feasible = helper.isAddFeasible(s, p, forwardingRules.get(p).keySet(), true, false);
        } catch (Switch.InfeasibleStateException e) {
            return false;
        }

        toKeepSwitchStates.put(s, s.getState());

        s.setState(feasible);

        return true;
    }


    @Override
    public Collection<Switch> getLastAvailableSwitches() {
        return lastAvailableSwitches;
    }

    public Assignment place2(Topology topology, Collection<Partition> partitionSpace) throws NoAssignmentFoundException {
        long start = System.currentTimeMillis();
        switchSelection.init(topology);
        partitionSorter.init(topology);

        final List<Switch> availableSwitches0 = new LinkedList<Switch>(topology.getSwitches());


        try {
            setInitialState(partitionSpace);
        } catch (Switch.InfeasibleStateException e) {
            throw new NoAssignmentFoundException();
        }


        LinkedList<StackObjects<Partition>> stack = new LinkedList<StackObjects<Partition>>();

        stack.add(new StackObjects<Partition>(availableSwitches0));
        Map<Partition, Switch> assignment = new HashMap<Partition, Switch>();
        final LinkedList<Partition> sortedPartitionSpace = partitionSorter.getSortedPartitionSpace(partitionSpace);
        boolean backtrack = false;
        int currentBacktrack2 = 0;

        while (stack.size() > 0) {
            /*int sum = 0;
            for (Switch aSwitch : topology.findEdges()) {
                sum += aSwitch.getState().getFinalRules().size();
            }
            System.out.println(1.0 * sum / 1000);*/

            boolean goNextLevel = false;
            //load back from stack
            StackObjects<Partition> stackObjects = stack.getFirst();
            List<Switch> availableSwitches = stackObjects.getAvailableSwitches();
            Map<Link, Long> oldTraffic = stackObjects.getOldTraffic();
            Map<Switch, Switch.FeasibleState> oldSwitchStates = stackObjects.getOldSwitchStates();

            LinkedList<Switch> availableToChoose;
            Partition candidatePartition;
            if (backtrack) {
                backtrack = false;
                revert(oldTraffic, oldSwitchStates);
                availableToChoose = stackObjects.getAvailableToChoose();
                candidatePartition = stackObjects.getCandidateObject();
//                Runtime.getRuntime().gc();
            } else {
                //prune once
                pruneSwitches(availableSwitches);
                //initialize
                //
                // select the partition
                candidatePartition = sortedPartitionSpace.poll();
                stackObjects.setCandidateObject(candidatePartition);
                availableToChoose = switchSelection.sortSwitches(new LinkedList<Switch>(availableSwitches), assignment, candidatePartition);
                stackObjects.setAvailableToChoose(availableToChoose);

                if (stack.size() > (maxBacktrack - currentBacktrack2 + 1)) {
                    ListIterator<StackObjects<Partition>> itr = stack.listIterator(maxBacktrack - currentBacktrack2 + 1);
                    while (itr.hasNext()) {
                        StackObjects next = itr.next();
                        next.empty();
                    }
                }
            }
            //no need to keep these in stack
            int level = stack.size();
            String indent = getIndent(level);
            while (availableToChoose.size() > 0) {
                Switch candidateSwitch = availableToChoose.pop();
                //System.out.println(level + ":" + candidatePartition + " ---> " + candidateSwitch);

                //update solution
                System.out.println(level + " " + (System.currentTimeMillis() - start) / 1000.0 + " " +
                        candidatePartition.hashCode() + " " + candidateSwitch);
                //create revert structures
                oldTraffic.clear();
                oldSwitchStates.clear();
                if (isFeasible(candidatePartition, candidateSwitch, oldTraffic, oldSwitchStates)) {
                    assignment.put(candidatePartition, candidateSwitch);
                    //if there are more partitions to assign
                    if (sortedPartitionSpace.size() > 0) {
                        //don't need more than maxBacktrack choices!
                        while (availableToChoose.size() > maxBacktrack) {
                            availableToChoose.removeLast();
                        }

                        //go to next stage
                        List<Switch> newAvailableSwitches = new LinkedList<Switch>(availableSwitches);
                        stack.push(new StackObjects<Partition>(newAvailableSwitches));
                        goNextLevel = true;
                        break;
                    } else {
                        stack.clear();
                        final Assignment assignment1 = new Assignment(assignment);
                        assignment1.updateForwardingRules(forwardingRules);
                        lastAvailableSwitches = availableSwitches;
                        return assignment1;
                    }
                } else {
                    //System.out.println("failed");
                }
            }

            if (!goNextLevel) {//so go up
                Util.logger.info(indent + "backtrack");
                if (maxBacktrack > 0 && currentBacktrack2 >= maxBacktrack) {
                    throw new NoAssignmentFoundException();
                } else {
                    currentBacktrack2++;
                    sortedPartitionSpace.addFirst(candidatePartition);
                    stack.removeFirst();
                    assignment.remove(candidatePartition);
                    backtrack = true;
                }
            }
        }
        throw new NoAssignmentFoundException();
    }

    /* private Map<Switch, Integer> findSwitchFlows(Topology topology, Set<Partition> partitionSpace) {
            Map<Switch, Integer> switchNewFlows = new HashMap<Switch, Integer>();
            for (Switch edgeSwitch : topology.findEdges()) {
                switchNewFlows.put(edgeSwitch, 0);
            }
            for (Partition partition : partitionSpace) {
                for (Map.Entry<Rule, Collection<Flow>> entry : ruleFlowMap.get(partition).entrySet()) {
                    for (Flow flow : entry.getValue()) {
                        final Switch source = flow.getSource();
                        final Switch destination = flow.getDestination();
                        switchNewFlows.put(source, switchNewFlows.get(source) + 1);
                        if (!source.equals(destination) && entry.getKey().getAction().doAction(flow) != null) {
                            switchNewFlows.put(destination, switchNewFlows.get(destination) + 1);
                        }
                    }
                }
            }
            return switchNewFlows;
        }
    */

}
