package edu.usc.enl.cacheflow.model.topology;

import edu.usc.enl.cacheflow.model.topology.switchhelper.MemorySwitchHelper;
import edu.usc.enl.cacheflow.model.topology.switchhelper.OVSSwitchHelper;
import edu.usc.enl.cacheflow.model.topology.switchhelper.SwitchHelper;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.WriterSerializable;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.OVSSwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.TCAMSRAMSwitch;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 9/22/11
 * Time: 12:35 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Topology implements WriterSerializable {
    public static final String TRAFFIC_STAT = "Sum Traffic";
    public static final String MEAN_LINK_STRESS_STAT = "Mean " + Link.STRESS_STAT;
    public static final String MEAN_RULES_STAT = "Mean " + Switch.RULES_STAT;
    public static final String MEAN_EDGE_STATS = "Mean Edge";
    public static final String MAX_EDGE_STATS = "Max Edge";
    public static final String MEAN_INTERNAL_STATS = "Mean Internal";


    protected List<Switch> switches = new LinkedList<Switch>();

    protected List<Link> links = new LinkedList<Link>();

    public Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap;

    private Switch.ControllerSwitch controllerSwitch = null;
    private Map<Class<? extends Switch>, SwitchHelper<? extends Switch>> switchHelpers;

    public void setRuleFlowMap(Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) {
        this.ruleFlowMap = ruleFlowMap;
    }

    /**
     * It needs iterating over rules
     *
     * @param partitions
     * @param forwardingRules
     * @return
     * @throws edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException
     *
     */
    public void createHelpers(List<Partition> partitions, Map<Partition, Map<Switch, Rule>> forwardingRules,
                              Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) throws UnalignedRangeException {
        final Set<Class<? extends Switch>> switchTypes = getSwitchTypes();
        switchHelpers = new HashMap<>(switchTypes.size(), 1);
        for (Class<? extends Switch> switchType : switchTypes) {
            if (switchType.equals(OVSSwitch.class)) {
                final OVSSwitchHelper ovsSwitchHelper = new OVSSwitchHelper(partitions, Util.threadNum, ruleFlowMap, forwardingRules);
                switchHelpers.put(OVSSwitch.class, ovsSwitchHelper);
            } else if (switchType.equals(MemorySwitch.class)) {
                final MemorySwitchHelper memorySwitchHelper = new MemorySwitchHelper(forwardingRules);
                switchHelpers.put(MemorySwitch.class, memorySwitchHelper);
            }
        }
        switchHelpers.put(getControllerSwitch().getClass(), new Switch.ControllerSwitchHelper());

        for (SwitchHelper<? extends Switch> switchHelper : switchHelpers.values()) {
            switchHelper.init();
        }
    }

    public Switch createASwitch(Aggregator aggregator, List<String> levelProperties, String idS,
                                Set<Rule> memoryTemplate) {
        if (levelProperties.get(0).equalsIgnoreCase("ovs")) {
            return new OVSSwitch(idS, aggregator, Integer.parseInt(levelProperties.get(1)), memoryTemplate);
        } else if (levelProperties.get(0).equalsIgnoreCase("tcamsram")) {
            return new TCAMSRAMSwitch(idS, aggregator, Integer.parseInt(levelProperties.get(1)), Integer.parseInt(levelProperties.get(2)), memoryTemplate);
        } else {
            return new MemorySwitch(idS, aggregator, Integer.parseInt(levelProperties.get(1)), memoryTemplate);
        }
    }

    public List<Switch> findEdges() {
        List<Switch> switches = getSwitches();
        List<Switch> edgeSwitches = new LinkedList<Switch>();
        for (Switch aSwitch : switches) {
            if (aSwitch.getLinks().size() == 1) {
                edgeSwitches.add(aSwitch);
            }
        }
        return edgeSwitches;
    }

    public long getTrafficForHosting(Partition p, Switch host) {
        long sum = 0;
        for (Map.Entry<Rule, Collection<Flow>> entry : ruleFlowMap.get(p).entrySet()) {
            for (Flow flow : entry.getValue()) {
                sum += flow.getTraffic() * getPathLength(flow.getSource(), host);
                if (entry.getKey().getAction().doAction(flow) != null) {
                    sum += flow.getTraffic() * getPathLength(host, flow.getDestination());
                }
            }
        }
        return sum;
    }

    /*
     * the shortest path between two nodes in the network
     */
    public List<Link> getPath(Switch start, Switch end, Flow flow) {
        return getPath(new SwitchPair(start, end), flow);
    }

    public int getPathLength(Switch start, Switch end) {
        return getPath(start, end, null).size();
    }

    public List<Link> getPath(SwitchPair pair, Flow flow) {
        final List<List<Link>> allPath = getAllPath(pair);
        if (allPath.size() == 0) {
            return new ArrayList<Link>();
        }
        if (allPath.size() == 1 || flow == null) {
            return allPath.iterator().next();
        }
        return allPath.get(Math.abs(flow.hashCode()) % allPath.size());
    }

    public List<List<Link>> getAllPath(Switch start, Switch end) {
        return getAllPath(new SwitchPair(start, end));
    }

    public abstract List<List<Link>> getAllPath(SwitchPair pair);

    public abstract void computeShortestPaths();


    public Map<String, Switch> getSwitchMap() {
        Map<String, Switch> output = new HashMap<String, Switch>();
        for (Switch aSwitch : switches) {
            output.put(aSwitch.getId(), aSwitch);
        }
        return output;
    }

    public void reset() {
        for (Switch aSwitch : switches) {
            aSwitch.reset();
        }

        for (Link link : links) {
            link.reset();
        }
    }

    public void addSwitch(Switch s) {
        switches.add(s);
    }

    public List<Link> getLinks() {
        return links;
    }

    public List<Switch> getSwitches() {
        return switches;
    }

    public Set<Class<? extends Switch>> getSwitchTypes() {
        Set<Class<? extends Switch>> output = new HashSet<>();
        for (Switch aSwitch : switches) {
            output.add(aSwitch.getClass());
        }
        return output;
    }

    /**
     * adds two links between switches. The capacity of both side links are similar
     *
     * @param a
     * @param b
     * @param capacity
     */
    public void addLink(Switch a, Switch b, long capacity) {
        Link l = new Link(a, b, capacity);
        links.add(l);
        a.addLink(l);
        Link l2 = new Link(b, a, capacity);
        links.add(l2);
        b.addLink(l2);
        l2.setOtherSide(l);
        l.setOtherSide(l2);
    }

    /**
     * @return the controller switch
     */
    public Switch.ControllerSwitch getControllerSwitch() {
        return controllerSwitch;
    }

    public void setControllerSwitch(Switch.ControllerSwitch controllerSwitch) {
        this.controllerSwitch = controllerSwitch;
    }

    public Map<Switch, Collection<Switch>> getRacks() {
        List<Switch> edges = findEdges();
        Map<Switch, Collection<Switch>> output = new HashMap<Switch, Collection<Switch>>();
        for (Switch edge : edges) {
            Switch agg = edge.getLinks().get(0).getEnd();
            Collection<Switch> switches1 = output.get(agg);
            if (switches1 == null) {
                switches1 = new LinkedList<Switch>();
                output.put(agg, switches1);
            }
            switches1.add(edge);
        }
        return output;
    }

    public void toString(PrintWriter p) {
        {
            if (this instanceof FatTree) {
                p.println("FatTree," + ((AbstractFatTree) this).getCoresSize());
            } else if (this instanceof Large2LevelFatTree) {
                p.println("LargeFatTree," + ((AbstractFatTree) this).getCoresSize());
            } else if (this instanceof Large4LevelFatTree) {
                p.println("large4fattree," + ((AbstractFatTree) this).getCoresSize());
            }
        }
        {
            //serialize switches
            p.println("#nodes");
            //add controller

            controllerSwitch.toString(p);
            for (Switch aSwitch : getSwitches()) {
                aSwitch.toString(p);
            }
        }

        {
            //serialize links
            p.println("#linkes");
            for (Link link : getLinks()) {
                if (link.getStart().hashCode() >= link.getEnd().hashCode()) {
                    link.toString(p);
                }
            }
        }
    }

    public void headerToString(PrintWriter p) {
    }

    public abstract JPanel draw(Dimension size);

    /**
     * computes the delay of flow on all links in the network. It basically sums the delaytraffic product on all links
     *
     * @return
     */
    public double computeDelay() {
        double sum = 0;
        for (Link link : links) {
            sum += link.getDelayTrafficProduct();
        }
        return sum;
    }

    /**
     * @return traffic sum on all links in the network
     */
    public double getTraffic() {
        double sum = 0;
        for (Link link : links) {
            sum += link.getUsedCapacity();
        }
        return sum;
    }

    public boolean hasHelper() {
        return switchHelpers != null;
    }

    public <T extends Switch> SwitchHelper<T> getHelper(T s) {
        return (SwitchHelper<T>) switchHelpers.get(s.getClass());
    }

    public void initHelpers(CollectionPool<Set<Rule>> ruleSetPool, CollectionPool<Set<Long>> wildcardPool) {
        if (switchHelpers != null) {
            for (SwitchHelper<? extends Switch> switchHelper : switchHelpers.values()) {
                if (switchHelper instanceof MemorySwitchHelper) {
                    ((MemorySwitchHelper) switchHelper).setRuleSetPool(ruleSetPool);
                } else if (switchHelper instanceof OVSSwitchHelper) {
                    ((OVSSwitchHelper) switchHelper).setRuleSetPool(ruleSetPool);
                    ((OVSSwitchHelper) switchHelper).setWildcardPool(wildcardPool);
                } else if (switchHelper instanceof Switch.ControllerSwitchHelper) {
                    ((Switch.ControllerSwitchHelper) switchHelper).setRuleSetPool(ruleSetPool);
                }
            }
        }
    }

    public void saveSwitchMemories(PrintWriter writer, Map<String, Object> parameters2) {
        throw new RuntimeException("Not implemented");
    }

    public void loadSwitchesMemory(BufferedReader bufferedReader) {
        throw new RuntimeException("Not implemented");
    }


    public Map<Class<? extends Switch>, SwitchHelper<? extends Switch>> getSwitchHelpers() {
        return switchHelpers;
    }

    /**
     * A wrapper class to be used to save shortest paths between a pair of switches
     */
    public static class SwitchPair {
        Switch first;
        Switch second;

        public SwitchPair(Switch first, Switch second) {
            this.first = first;
            this.second = second;
        }

        public void setFirst(Switch first) {
            this.first = first;
        }

        public void setSecond(Switch second) {
            this.second = second;
        }

        @Override
        public String toString() {
            return first + " -> " + second;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SwitchPair that = (SwitchPair) o;

            if (first != null ? !first.equals(that.first) : that.first != null) return false;
            if (second != null ? !second.equals(that.second) : that.second != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = first != null ? first.hashCode() : 0;
            result = 31 * result + (second != null ? second.hashCode() : 0);
            return result;
        }
    }

    public Statistics getStat(Map<String, Object> parameters) throws Exception {

        //get stats
        Statistics outputStatistics = new Statistics();
        {
            //switch memory
            List<Statistics> switchStats = new ArrayList<Statistics>(switches.size());
            for (Switch aSwitch : switches) {
                Statistics stats = aSwitch.getStats();
                switchStats.add(stats);
                outputStatistics.addStat("Switch " + aSwitch.getId(), stats);
            }
            final Double mean = Statistics.getMean(switchStats, Switch.RULES_STAT);
            outputStatistics.addStat(MEAN_RULES_STAT, mean);
        }

        getSwitchCategorizedStats(outputStatistics);

        getLinkStatistics(outputStatistics);
        outputStatistics.setParameters(parameters);
        return outputStatistics;
    }

    protected void getSwitchCategorizedStats(Statistics outputStatistics) {
        final List<Switch> edges = findEdges();
        //edge and internal memory usage
        {
            List<Statistics> switchStats = new ArrayList<Statistics>(edges.size());
            final Collection<String> statNames = edges.get(0).getStats().getStatNames();
            for (Switch aSwitch : edges) {
                Statistics stats = aSwitch.getStats();
                switchStats.add(stats);
                outputStatistics.addStat("Switch " + aSwitch.getId(), stats);
            }
            for (String statName : statNames) {
                final Double mean = Statistics.getMean(switchStats, statName);
                outputStatistics.addStat(MEAN_EDGE_STATS + " " + statName, mean);
                outputStatistics.addStat(MAX_EDGE_STATS + " " + statName, Statistics.getMax(switchStats, statName));
            }
        }
        {
            List<Statistics> switchStats = new ArrayList<Statistics>(edges.size());
            final List<Switch> internalSwitches = new ArrayList<Switch>(getSwitches());
            internalSwitches.removeAll(edges);
            final Collection<String> statNames = internalSwitches.get(0).getStats().getStatNames();
            for (Switch aSwitch : internalSwitches) {
                Statistics stats = aSwitch.getStats();
                switchStats.add(stats);
                outputStatistics.addStat("Switch " + aSwitch.getId(), stats);
            }
            for (String statName : statNames) {
                final Double mean = Statistics.getMean(switchStats, statName);
                outputStatistics.addStat(MEAN_INTERNAL_STATS + " " + statName, mean);
            }
        }
    }

    protected void getLinkStatistics(Statistics outputStatistics) {
        //link statistics
        List<Link> links = getLinks();
        List<Statistics> linkStatistics = new ArrayList<Statistics>(links.size());
        for (Link link : links) {
            Statistics stats = link.getStats();
            linkStatistics.add(stats);
            //outputStatistics.addStat("Link " + link.getStart() + " " + link.getEnd(), stats);
        }
        outputStatistics.addStat(MEAN_LINK_STRESS_STAT, Statistics.getMean(linkStatistics, Link.STRESS_STAT));
        outputStatistics.addStat(TRAFFIC_STAT, Statistics.getSum(linkStatistics, Link.USED_CAPACITY_STAT));
    }


}
