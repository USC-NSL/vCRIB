package edu.usc.enl.cacheflow.algorithms.placement;

import edu.usc.enl.cacheflow.algorithms.replication.Replicator;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.algorithms.PostPlacer;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/14/12
 * Time: 7:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class VMStartAssigner implements PostPlacer {
    protected final Map<Partition, Map<Switch, Rule>> forwardingRules;
    protected final Map<Switch, Collection<Partition>> sourcePartitions;
    private final Topology topology;
    private final boolean sourceOrToR;

    public VMStartAssigner(Map<Partition, Map<Switch, Rule>> forwardingRules, Map<Switch, Collection<Partition>> sourcePartitions, Topology topology, boolean sourceOrToR) {
        this.forwardingRules = forwardingRules;
        this.sourcePartitions = sourcePartitions;
        this.topology = topology;
        this.sourceOrToR = sourceOrToR;
    }

    @Override
    public Map<Partition, Map<Switch, Switch>> postPlace(Set<Switch> availableSwitches, Assignment assignment, PrintWriter trendWriter) {
        Map<Partition, Map<Switch, Switch>> output = new HashMap<Partition, Map<Switch, Switch>>(forwardingRules.size());
        try {
            final List<Switch> edges = topology.findEdges();
            for (Switch edge : edges) {
                topology.getHelper(edge).initToNotOnSrc(edge,sourcePartitions.get(edge), true);
            }
        } catch (Switch.InfeasibleStateException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        try {
            if (sourceOrToR) {
                List<Switch> oneEntryList = new ArrayList<>(1);
                oneEntryList.add(topology.getControllerSwitch());//just to make its size=1
                for (Map.Entry<Partition, Map<Switch, Rule>> entry : forwardingRules.entrySet()) {
                    final Partition partition = entry.getKey();
                    Map<Switch, Switch> output2 = new HashMap<Switch, Switch>(entry.getValue().size());
                    for (Map.Entry<Switch, Rule> entry2 : entry.getValue().entrySet()) {
                        final Switch src = entry2.getKey();
                        output2.put(src, src);
                        oneEntryList.set(0, src);
                        topology.getHelper(src).isAddFeasible(src, partition, oneEntryList, true, true);
                    }
                    output.put(partition, output2);
                }
            } else {
                Map<Switch, Collection<Switch>> torEdgeMap = new HashMap<>();
                for (Switch edge : topology.findEdges()) {
                    final Switch tor = edge.getLinks().get(0).getEnd();
                    Collection<Switch> rackSwitches = torEdgeMap.get(tor);
                    if (rackSwitches == null) {
                        rackSwitches = new LinkedList<>();
                        torEdgeMap.put(tor, rackSwitches);
                    }
                    rackSwitches.add(edge);
                }
                Set<Switch> checkedTors = new HashSet<>();
                for (Map.Entry<Partition, Map<Switch, Rule>> entry : forwardingRules.entrySet()) {
                    final Partition partition = entry.getKey();
                    Map<Switch, Switch> output2 = new HashMap<>(entry.getValue().size(), 1);
                    checkedTors.clear();
                    for (Map.Entry<Switch, Rule> entry2 : entry.getValue().entrySet()) {
                        final Switch src = entry2.getKey();
                        final Switch tor = src.getLinks().get(0).getEnd();
                        if (checkedTors.contains(tor)) {
                            continue;
                        }
                        checkedTors.add(tor);
                        final Collection<Switch> sourcesInRack = torEdgeMap.get(tor);
                        for (Switch aSource : sourcesInRack) {
                            output2.put(aSource, tor);
                        }
                        topology.getHelper(tor).isAddFeasible(tor, partition, sourcesInRack, true, true);
                    }
                    //add forwarding rules
                    output.put(partition, output2);
                }
            }
        } catch (Switch.InfeasibleStateException e) {
            throw new RuntimeException(e);
        }

        Replicator.updateForwardingRules(output, forwardingRules);

        return output;
    }

    @Override
    public Statistics getStats(Map<String, Object> parameters) {
        return new Statistics();
    }
}
