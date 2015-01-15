package edu.usc.enl.cacheflow.algorithms.placement;

import edu.usc.enl.cacheflow.algorithms.placement.clusterselection.ClusterSelection;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectioncluster.SwitchSelectionCluster;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.factory.ClusterFactory;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.rule.Cluster;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.processor.network.InformOnRestart;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 9/22/11
 * Time: 2:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class AssignerCluster extends Placer implements InformOnRestart {
    private final SwitchSelectionCluster switchSelection;
    private final ClusterSelection clusterSelection;
    private List<Switch> lastAvailableSwitches;
    private Collection<Cluster> clusters;
    protected final int maxBacktrack;


    public AssignerCluster(SwitchSelectionCluster switchSelection, ClusterSelection clusterSelection, int maxBacktrack,
                           boolean checkLinks, Map<Partition, Map<Switch, Rule>> forwardingRules, Collection<Cluster> clusters,
                           Map<Switch, Collection<Partition>> sourcePartitions) {
        super(checkLinks, forwardingRules, sourcePartitions, Util.threadNum);
        this.maxBacktrack = maxBacktrack;
        this.clusterSelection = clusterSelection;
        this.switchSelection = switchSelection;
        this.clusters = clusters;

        //switchNeedsPartitionNum = new HashMap<Switch, Integer>();
    }

    public static List<Cluster> loadClusters(Map<String, Object> parameters, File clusterFolder, Collection<Partition> partitions) throws IOException {
        for (File file : clusterFolder.listFiles()) {
            Map<String, Object> parameters2 = new HashMap<String, Object>();
            Util.loadParameters(new ClusterFactory(new FileFactory.EndOfFileCondition(), partitions), file.getPath(), parameters2);
            if (Util.haveEqualParameters(parameters, parameters2, "^(topology|partition|rule)\\..*")) {
                return Util.loadFileFilterParam(new ClusterFactory(new FileFactory.EndOfFileCondition(), partitions),
                        file.getPath(), parameters, new LinkedList<Cluster>(), "cluster\\..*");
            }
        }
        return null;
    }

    public void setClusters(Collection<Cluster> clusters) {
        this.clusters = clusters;
    }

    @Override
    public String toString() {
        return "AssignerCluster";
    }

    public boolean isFeasible(Cluster c, Switch s,
                              Map<Link, Long> oldTraffic,
                              Map<Switch, Switch.FeasibleState> toKeepSwitchStates) {


        // check the host of partition

        try {
            final Switch.FeasibleState newState = topology.getHelper(s).isAddMultipleFeasible(s, c.getPartitions(), forwardingRules, false);
            toKeepSwitchStates.put(s, s.getState());
            s.setState(newState);
        } catch (Switch.InfeasibleStateException e) {
            return false;
        }

        return true;
    }

    public Assignment place2(Topology topology, Collection<Partition> partitionSpace)
            throws NoAssignmentFoundException {
        if (clusters == null) {
            throw new NoAssignmentFoundException("Cluster is null");
        }

        long start = System.currentTimeMillis();
        switchSelection.init(topology);
        clusterSelection.init(topology);
        final List<Switch> availableSwitches0 = new LinkedList<Switch>(topology.getSwitches());


        try {
            setInitialState(partitionSpace);
        } catch (Switch.InfeasibleStateException e) {
            throw new NoAssignmentFoundException();
        }


        LinkedList<StackObjects<Cluster>> stack = new LinkedList<StackObjects<Cluster>>();

        stack.add(new StackObjects<Cluster>(availableSwitches0));
        Map<Cluster, Switch> assignment = new HashMap<Cluster, Switch>();
        final LinkedList<Cluster> sortedPartitionSpace = clusterSelection.getSortedPartitionSpace(clusters);
        boolean backtrack = false;
        int currentBacktrack2 = 0;

        while (stack.size() > 0) {

            boolean goNextLevel = false;
            //load back from stack
            StackObjects<Cluster> stackObjects = stack.getFirst();
            List<Switch> availableSwitches = stackObjects.getAvailableSwitches();
            Map<Link, Long> oldTraffic = stackObjects.getOldTraffic();
            Map<Switch, Switch.FeasibleState> oldSwitchStates = stackObjects.getOldSwitchStates();

            LinkedList<Switch> availableToChoose;
            Cluster candidateCluster;
            if (backtrack) {
                backtrack = false;
                if (oldTraffic == null) {
                    System.out.println("currentbacktrack " + currentBacktrack2 + " stacksize " + stack.size());
                }
                revert(oldTraffic, oldSwitchStates);
                availableToChoose = stackObjects.getAvailableToChoose();
                candidateCluster = stackObjects.getCandidateObject();
//                Runtime.getRuntime().gc();
            } else {
                //prune once
                pruneSwitches(availableSwitches);
                //initialize
                //
                // select the partition
                candidateCluster = sortedPartitionSpace.poll();
                stackObjects.setCandidateObject(candidateCluster);
                availableToChoose = switchSelection.sortSwitches(new LinkedList<Switch>(availableSwitches), assignment, candidateCluster);
                stackObjects.setAvailableToChoose(availableToChoose);

                if (stack.size() > (maxBacktrack - currentBacktrack2 + 1)) {
                    ListIterator<StackObjects<Cluster>> itr = stack.listIterator(maxBacktrack - currentBacktrack2 + 1);
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
                //System.out.println(level + ":" + candidateCluster + " ---> " + candidateSwitch);

                //update solution
                System.out.println(level + " " + (System.currentTimeMillis() - start) / 1000.0 + " " +
                        candidateCluster.hashCode() + " " + candidateSwitch);
                //create revert structures
                oldTraffic.clear();
                oldSwitchStates.clear();
                if (isFeasible(candidateCluster, candidateSwitch, oldTraffic, oldSwitchStates)) {
                    //candidateSwitch.addPartition(candidateCluster);
                    //if there are more partitions to assign
                    assignment.put(candidateCluster, candidateSwitch);
                    if (sortedPartitionSpace.size() > 0) {
                        //don't need more than maxBacktrack choices!
                        while (availableToChoose.size() > maxBacktrack) {
                            availableToChoose.removeLast();
                        }

                        //go to next stage
                        List<Switch> newAvailableSwitches = new LinkedList<Switch>(availableSwitches);
                        stack.push(new StackObjects<Cluster>(newAvailableSwitches));
                        goNextLevel = true;
                        break;
                    } else {

                        Map<Partition, Switch> detailAssignment = new HashMap<Partition, Switch>();
                        for (Map.Entry<Cluster, Switch> entry : assignment.entrySet()) {
                            for (Partition partition : entry.getKey().getPartitions()) {
                                detailAssignment.put(partition, entry.getValue());
                            }
                        }

                        this.lastAvailableSwitches = availableSwitches;
                        stack.clear();
                        final Assignment assignment1 = new Assignment(detailAssignment);
                        assignment1.updateForwardingRules(forwardingRules);
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
                    sortedPartitionSpace.addFirst(candidateCluster);
                    stack.removeFirst();
                    assignment.remove(candidateCluster);
                    backtrack = true;
                }
            }
        }
        throw new NoAssignmentFoundException();
    }

    public List<Switch> getLastAvailableSwitches() {
        return lastAvailableSwitches;
    }


    @Override
    public void restart() {
        //reload clusters
    }
}
