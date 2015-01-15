package edu.usc.enl.cacheflow.model;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.ForwardAction;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/15/12
 * Time: 8:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class Assignment implements WriterSerializable {
    private Map<Partition, Switch> placement;
    private Map<Switch, Set<Partition>> rplacement;

    public static Assignment getAssignment(Map<Switch, ? extends Collection<Partition>> rplacement) {
        Map<Partition, Switch> placement = new HashMap<>();
        for (Map.Entry<Switch, ? extends Collection<Partition>> entry : rplacement.entrySet()) {
            for (Partition partition : entry.getValue()) {
                placement.put(partition, entry.getKey());
            }
        }
        return new Assignment(placement);
    }

    public Assignment(Map<Partition, Switch> placement) {
        this.placement = placement;
    }

    public void updateForwardingRules(Map<Partition, Map<Switch, Rule>> forwardingRules) {
        updateForwardingRules(placement,forwardingRules);
    }

    public static void updateForwardingRules(Map<Partition,Switch> placement,Map<Partition, Map<Switch, Rule>> forwardingRules) {
        for (Map.Entry<Partition, Switch> entry : placement.entrySet()) {
            for (Rule forwardingRule : forwardingRules.get(entry.getKey()).values()) {
                forwardingRule.setAction(new ForwardAction(entry.getValue()));
            }
        }
    }


    public Map<Partition, Switch> getPlacement() {
        if (placement == null) {
            placement = generatePlacement();
        }
        return placement;
    }

    public void loadOnTopology(Topology topology, Map<Partition, Map<Switch, Rule>> forwardingRules,
                               Map<Partition, Map<Rule, Collection<Flow>>> classified) {
        updateForwardingRules(forwardingRules);

    }


    private Map<Partition, Switch> generatePlacement() {
        Map<Partition, Switch> output = new HashMap<Partition, Switch>();
        for (Map.Entry<Switch, Set<Partition>> switchPartitionsEntry : rplacement.entrySet()) {
            final Switch aSwitch = switchPartitionsEntry.getKey();
            for (Partition partition : switchPartitionsEntry.getValue()) {
                output.put(partition, aSwitch);
            }
        }
        return output;
    }

    public Map<Switch, Set<Partition>> getRplacement() {
        if (rplacement == null) {
            rplacement = generateRPlacement(placement);
        }
        return rplacement;
    }

    public static Map<Switch, Set<Partition>> generateRPlacement(Map<Partition,Switch> placement) {
        Map<Switch, Set<Partition>> switchPartitionMap = new HashMap<Switch, Set<Partition>>();

        final HashSet<Switch> switches = new HashSet<Switch>(placement.values());
        for (Switch aSwitch : switches) {
            switchPartitionMap.put(aSwitch, new HashSet<Partition>());
        }
        for (Map.Entry<Partition, Switch> assignmentEntry : placement.entrySet()) {
            switchPartitionMap.get(assignmentEntry.getValue()).add(assignmentEntry.getKey());
        }
        return switchPartitionMap;
    }

    public void toString(PrintWriter writer) {
        for (Map.Entry<Switch, Set<Partition>> entry : getRplacement().entrySet()) {
            //write switch name
            writer.print(entry.getKey().getId());
            for (Partition partition : entry.getValue()) {
                writer.print("," + partition.getId());
            }
            writer.println();
        }
    }

    public void headerToString(PrintWriter p) {
    }

    /*public static void MemoryFillerRunner(Map<Switch, Set<Rule>> switchesMemoryMap, int threadNum,
                                          Map<Partition, Map<Switch, Rule>> forwardingRule,
                                          Map<Switch, Collection<Partition>> sourcePartitions) {

        MemoryFiller[] threads = new MemoryFiller[threadNum];
        Iterator<Switch> iterator = switchesMemoryMap.keySet().iterator();
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new MemoryFiller(forwardingRule, switchesMemoryMap, sourcePartitions, iterator);
        }
        Util.runThreads(threads);
    }

    public static void MemoryFillerRunner(Map<Switch, Set<Rule>> switchesMemoryMap, int threadNum,
                                          Map<Partition, Map<Switch, Rule>> forwardingRules,
                                          Map<Switch, Collection<Partition>> sourcePartitions,
                                          Assignment assignment, Map<Switch, Integer> switchNewFlows,
                                          Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) {

        MemoryFiller[] threads = new MemoryFiller[threadNum];
        Iterator<Switch> iterator = switchesMemoryMap.keySet().iterator();
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new MemoryFiller(forwardingRules, switchesMemoryMap, sourcePartitions, iterator,
                    assignment, switchNewFlows, ruleFlowMap);
        }
        if (assignment != null) {
            assignment.updateForwardingRules(forwardingRules);
        }
        Util.runThreads(threads);
    }*/

    /*public static void MemoryFillerRunner(Collection<Switch> switches, int threadNum,
                                          Map<Partition, Map<Switch, Rule>> forwardingRules,
                                          Map<Switch, Collection<Partition>> sourcePartitions,
                                          Assignment assignment, Map<Switch, Integer> switchNewFlows,
                                          Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) {

        MemoryFiller[] threads = new MemoryFiller[threadNum];
        Iterator<Switch> iterator = switches.iterator();
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new MemoryFiller(forwardingRules, null, sourcePartitions, iterator,
                    assignment, switchNewFlows, ruleFlowMap);
        }
        if (assignment != null) {
            assignment.updateForwardingRules(forwardingRules);
        }
        Util.runThreads(threads);
    }

    public static void MemoryFillerRunner(Collection<Switch> switches, int threadNum,
                                          Map<Partition, Map<Switch, Rule>> forwardingRules,
                                          Map<Switch, Collection<Partition>> sourcePartitions) {

        MemoryFiller[] threads = new MemoryFiller[threadNum];
        Iterator<Switch> iterator = switches.iterator();
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new MemoryFiller(forwardingRules, null, sourcePartitions, iterator,
                    null, null, null);
        }
        Util.runThreads(threads);
    }

    private static class MemoryFiller extends Thread {
        private final Map<Partition, Map<Switch, Rule>> forwardingRules;
        private final Assignment assignment;
        private final Map<Switch, Set<Rule>> switchRulesMap;
        private final Map<Switch, Collection<Partition>> sourcePartitions;
        private final Iterator<Switch> iterator;
        private final Map<Switch, Integer> switchNewFlows;
        private final Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap;

        private MemoryFiller(Map<Partition, Map<Switch, Rule>> forwardingRules, Map<Switch, Set<Rule>> switchRulesMap,
                             Map<Switch, Collection<Partition>> sourcePartitions, Iterator<Switch> iterator, Assignment assignment,
                             Map<Switch, Integer> switchNewFlows,
                             Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) {
            this.forwardingRules = forwardingRules;
            this.assignment = assignment;
            this.switchRulesMap = switchRulesMap;
            this.sourcePartitions = sourcePartitions;
            this.iterator = iterator;
            this.switchNewFlows = switchNewFlows;
            this.ruleFlowMap = ruleFlowMap;
        }

        private MemoryFiller(Map<Partition, Map<Switch, Rule>> forwardingRules, Map<Switch, Set<Rule>> switchRulesMap,
                             Map<Switch, Collection<Partition>> sourcePartitions, Iterator<Switch> iterator) {
            this.forwardingRules = forwardingRules;
            this.switchRulesMap = switchRulesMap;
            this.sourcePartitions = sourcePartitions;
            this.iterator = iterator;
            this.assignment = null;
            this.switchNewFlows = null;
            ruleFlowMap = null;

        }

        @Override
        public void run() {
            while (true) {
                Switch aSwitch;
                synchronized (iterator) {
                    if (iterator.hasNext()) {
                        aSwitch = iterator.next();
                    } else {
                        break;
                    }
                }
                final Set<Rule> rules;
                if (switchRulesMap == null) {
                    rules = aSwitch.getState().getRules();
                } else {
                    rules = switchRulesMap.get(aSwitch);
                }
                //put forwarding rules
                final Collection<Partition> srcPartitions = sourcePartitions.get(aSwitch);
                if (srcPartitions != null) {
                    for (Partition partition : srcPartitions) {
                        rules.add(forwardingRules.get(partition).get(aSwitch));
                    }
                }
                //put assigned rules
                if (assignment != null) {

                    int sum = 0;
                    final Set<Partition> hostedPartitions = assignment.getRplacement().get(aSwitch);
                    if (hostedPartitions != null) {
                        for (Partition partition : hostedPartitions) {
                            rules.addAll(partition.getRules());
                            rules.remove(forwardingRules.get(partition).get(aSwitch));
                            sum += Placer.getHostAddedFlows(aSwitch, ruleFlowMap.get(partition));
                        }
                    }
                    final Integer oldNewFlows = switchNewFlows.get(aSwitch);

                    //get the number of local new flows for current switch that it does not host
                    if (srcPartitions != null) {
                        for (Partition partition : srcPartitions) {
                            if (hostedPartitions == null || !hostedPartitions.contains(partition)) {
                                //sum+=local accept flows
                                for (Map.Entry<Rule, Collection<Flow>> entry : ruleFlowMap.get(partition).entrySet()) {
                                    for (Flow flow : entry.getValue()) {
                                        if (flow.getDestination().equals(aSwitch) && entry.getKey().getAction().doAction(flow) != null) {
                                            sum++;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    switchNewFlows.put(aSwitch, (oldNewFlows == null ? 0 : oldNewFlows) + sum);
                }
            }
        }
    }*/

}
