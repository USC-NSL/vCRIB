package edu.usc.enl.cacheflow.model.topology.switchmodel;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/23/12
 * Time: 12:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class OVSSwitch extends Switch {
    public static final String USED_CPU_STAT = "Used CPU Capacity";
    public static final String NEW_FLOWS_STAT = "New Flows";
    public static final String WILDCARD_PATTERNS_STAT = "Wildcard Patterns";

    private int cpuPercentCapacity;
    private OVSState state;

    public OVSSwitch(String id, Aggregator aggregator, int cpuPercentCapacity, Set<Rule> memoryTemplate) {
        super(id, aggregator);
        this.cpuPercentCapacity = cpuPercentCapacity;
        state = new OVSState(Util.getNewCollectionInstance(memoryTemplate), 0, 0, new HashSet<Long>());
    }

    @Override
    public void reset() {
        super.reset();
        state.getRules().clear();
        state = new OVSState(state.getRules(), 0, 0, new HashSet<Long>());
    }

    @Override
    public void fillParam(Switch aSwitch) {
        this.cpuPercentCapacity = ((OVSSwitch) aSwitch).cpuPercentCapacity;
    }

    public static float computeCPUUsage(int newFlows, int ruleSetSize, int wBitPatternsSize) {
        double a = 0.00000812775;
        double b = 86.878635;
        if (wBitPatternsSize == 0) {
            return 0;
        }
        return (float) (newFlows * wBitPatternsSize * a * Math.log(b * ruleSetSize / wBitPatternsSize));
    }

    @Override
    public boolean canSaveMore() {
        return true;
    }

    public int getCpuPercentCapacity() {
        return cpuPercentCapacity;
    }

    @Override
    public double getUsedResources(FeasibleState state) {
        if (cpuPercentCapacity == 0) {
            if (((OVSState) state).getRules().size() == 0) {
                return 0;
            } else {
                return Double.POSITIVE_INFINITY;
            }
        }
        return (1.0 * ((OVSState) state).getCpuUsage() / cpuPercentCapacity);
    }

    @Override
    public double getUsedAbsoluteResources(FeasibleState state) {
        return ((OVSState) state).getCpuUsage();
    }

    @Override
    public Switch cloneEmpty(String id) {
        return new OVSSwitch(id, aggregator, cpuPercentCapacity, state.getRules());
    }

    public Statistics getStats() {
        Statistics stat = super.getStats();
        stat.addStat(USED_CPU_STAT, state.getCpuUsage());
        stat.addStat(NEW_FLOWS_STAT, state.getNewFlows());
        Set<Long> wBitPatterns = new HashSet<Long>();
        try {
            for (Rule rule : state.getRules()) {
                wBitPatterns.add(rule.getWildCardBitPattern());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        stat.addStat(WILDCARD_PATTERNS_STAT, wBitPatterns.size());
        return stat;
    }

    @Override
    public void setState(FeasibleState feasibleState) {
        this.state = (OVSState) feasibleState;
    }

    @Override
    public FeasibleState getState() {
        return state;
    }

    public void toString(PrintWriter p) {
        p.println(id + ",OVS," + cpuPercentCapacity);
    }

    public static class OVSState extends MemorySwitch.MemoryState {
        private float cpuUsage;
        private int newFlows;
        private Set<Long> wildcards;

        public OVSState(Set<Rule> rules, float cpuUsage, int newFlows, Set<Long> wildcards) {
            super(rules);
            this.cpuUsage = cpuUsage;
            this.newFlows = newFlows;
            this.wildcards = wildcards;
        }

        public Set<Long> getWildcards() {
            return wildcards;
        }

        public float getCpuUsage() {
            return cpuUsage;
        }

        public void setCpuUsage(float cpuUsage) {
            this.cpuUsage = cpuUsage;
        }

        public int getNewFlows() {
            return newFlows;
        }

        public void setNewFlows(int newFlows) {
            this.newFlows = newFlows;
        }

        public void fillWildcards(Set<Long> wildcards) {
            this.wildcards.clear();
            this.wildcards.addAll(wildcards);
        }

        @Override
        public String toString() {
            return "CPU=" + cpuUsage + " for " + getRules().size() + " rules with " + wildcards.size() + " wildcards and new flows " + newFlows;
        }

        @Override
        public FeasibleState clone() {
            final Set<Rule> rules1 = Util.getNewCollectionInstance(rules);
            rules1.addAll(rules);
            final Set<Long> wildcards1 = Util.getNewCollectionInstance(wildcards);
            wildcards1.addAll(wildcards);
            return new OVSState(rules1, cpuUsage, newFlows, wildcards1);
        }
    }
}
