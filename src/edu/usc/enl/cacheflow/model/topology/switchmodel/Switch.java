package edu.usc.enl.cacheflow.model.topology.switchmodel;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.HasStatistics;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.WriterSerializable;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.ForwardAction;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.switchhelper.SwitchHelper;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 9/22/11
 * Time: 12:21 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Switch implements Comparable<Switch>, HasStatistics, WriterSerializable,Cloneable {
    protected final String id;
    //    private List<Patch> patches;
    private List<Link> links;

    protected Aggregator aggregator;
    private List<Flow> inputFlows;

    private int numberOfForwardRules = 0;

    public static final String CONTROLLER_ID = "Controller";
    public static final String RULES_STAT = "Number of Rules";
    public static final String FORWARDING_RULES_STAT = "Number of FW Rules";
    private int hashCodeCache = 0;


    public Switch(String id, Aggregator aggregator) {
        this.id = id;
        links = new LinkedList<Link>();
        this.aggregator = aggregator;
        inputFlows = new LinkedList<Flow>();
    }

    public abstract double getUsedResources(FeasibleState state);

    public abstract double getUsedAbsoluteResources(FeasibleState state);

    public List<Flow> getInputFlows() {
        return inputFlows;
    }

    public void addFlow(Flow flow) {
        inputFlows.add(flow);
    }

    public abstract Switch cloneEmpty(String id);

    public int getNumberOfForwardRules() {
        return numberOfForwardRules;
    }

    public void setNumberOfForwardRules(int numberOfForwardRules) {
        this.numberOfForwardRules = numberOfForwardRules;
    }

    public void headerToString(PrintWriter p) {
    }

    public Statistics getStats() {
        Statistics stat = new Statistics();
        int numOfFWRules = 0;
        int rulesNum = 0;
        try {
            final HashSet<Rule> toFill = new HashSet<>();
            getState().getRules(toFill);
            for (Rule rule : toFill) {
                if (rule.getAction() instanceof ForwardAction) {
                    numOfFWRules++;
                }
                rulesNum++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        stat.addStat(RULES_STAT, rulesNum);
        stat.addStat(FORWARDING_RULES_STAT, numOfFWRules);
        return stat;
    }

    /*public void reagregate() throws Exception {
        setAggregatedRules(aggregate(fineRules));
    }

    public List<Rule> aggregate(List<Rule> rules) throws Exception {
        aggregator.setTailInput(rules);
        return aggregator.run();
    }*/


    /*public List<Rule> getAggregatedWithCurrent(List<Rule> newRules) throws Exception {
        List<Rule> allRules = new ArrayList<Rule>(fineRules.size() + newRules.size());
        allRules.addAll(fineRules);
        allRules.addAll(newRules);
        return aggregate(allRules);
    }*/

    /*public void setAggregatedRules(List<Rule> rules) {
        aggregatedRules = rules;
    }*/

    /**
//     * if selfCommit, it will copy the ruleSet, otherwise creates a new state using the ruleSet
//     *
//     *
//     * @param ruleSet
//     * @param checkResources
//     * @param selfCommit
//     * @return
//     * @throws InfeasibleStateException
     */
    //public abstract FeasibleState isFeasible(Set<Rule> ruleSet, boolean checkResources, boolean selfCommit) throws InfeasibleStateException;


    public abstract void setState(FeasibleState feasibleState);

    public abstract FeasibleState getState() ;

    public void addLink(Link l) {
        links.add(l);
    }

    public List<Switch> getNeighbors() {
        List<Switch> output = new ArrayList<Switch>(links.size());
        for (Link connection : links) {
            output.add(connection.getEnd());
        }
        return output;
    }

    @Override
    public String toString() {
        return id;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Switch aSwitch = (Switch) o;

        if (id != null ? !id.equals(aSwitch.id) : aSwitch.id != null) return false;

        return true;
    }


    @Override
    public int hashCode() {
        if (hashCodeCache == 0) {
            hashCodeCache = id != null ? id.hashCode() : 0;
        }
        return hashCodeCache;
    }

    public List<Link> getLinks() {
        return links;
    }

    public int compareTo(Switch o) {
        return id.compareTo(o.getId());
    }

    public abstract boolean canSaveMore();

    public void reset() {
        inputFlows.clear();
    }

    public abstract void fillParam(Switch aSwitch);


    /* public Statistics getStats() {
        Statistics stat = new Statistics();
        //stat.addStat(FREE_MEMORY_STAT, getFreeCapacity());
        //stat.addStat(USED_MEMORY_STAT, getUsedCapacity());
        stat.addStat(USED_MEMORY_RATIO_STAT, 1.0 * getUsedCapacity() / getMemoryCapacity());
        //stat.addStat(USED_CPU_CAPACITY_STAT,  computationCapacityLink.getUsedCapacity());
        stat.addStat(USED_CPU_CAPACITY_RATIO_STAT, 1.0 * computationCapacityLink.getUsedCapacity()
                / computationCapacityLink.getCapacity());
        return stat;
    }*/

    public static abstract class FeasibleState implements Cloneable{
        public abstract void getRules(Set<Rule> toFill);
        public abstract FeasibleState clone();
    }

    public static class InfeasibleStateException extends Exception {
        //private FeasibleState infeasibleState;

        public InfeasibleStateException(String message) {
            super(message);
        }


        /*public FeasibleState getInfeasibleState() {
            return infeasibleState;
        }*/
    }

    public static class ControllerSwitch extends Switch {
        MemorySwitch.MemoryState state;

        public ControllerSwitch(Aggregator aggregator, int delay, Set<Rule> ruleTemplate) {
            super(CONTROLLER_ID, aggregator);
            state = new MemorySwitch.MemoryState(Util.getNewCollectionInstance(ruleTemplate));
        }

        @Override
        public double getUsedResources(FeasibleState state) {
            return 0;
        }

        @Override
        public double getUsedAbsoluteResources(FeasibleState state) {
            return 0;
        }

        @Override
        public Switch cloneEmpty(String id){
            return new ControllerSwitch(aggregator,0,  state.getRules());
        }

        @Override
        public void setState(FeasibleState feasibleState) {
            this.state= (MemorySwitch.MemoryState) feasibleState;
        }

        @Override
        public FeasibleState getState() {
            return state;
        }

        /*@Override
        public FeasibleState isFeasible(Set<Rule> ruleSet, boolean checkResources, boolean selfCommit) throws InfeasibleStateException {
            if (selfCommit) {
                getState().getRules().clear();
                getState().getRules().addAll(ruleSet);
                return getState();
            }
            return new MemoryState(ruleSet);
        }
*/
        @Override
        public boolean canSaveMore() {
            return true;
        }

        @Override
        public void fillParam(Switch aSwitch) {

        }

        public void toString(PrintWriter p) {
            p.println(id + " " + 0);
        }
    }

    public static class ControllerSwitchHelper extends SwitchHelper<ControllerSwitch>{

        private CollectionPool<Set<Rule>> ruleSetPool;

        public void setRuleSetPool(CollectionPool<Set<Rule>> ruleSetPool) {
            this.ruleSetPool = ruleSetPool;
        }

        @Override
        public double resourceUsage(Switch host, Partition newPartition, Collection<Switch> sources, boolean checkResources) throws InfeasibleStateException {
            return 0;
        }

        @Override
        public void init() {

        }

        @Override
        public FeasibleState isAddFeasible(ControllerSwitch host, Partition newPartition, Collection<Switch> sources, boolean checkResources, boolean selfCommit) throws InfeasibleStateException {
            return host.getState();
        }

        @Override
        public FeasibleState isRemoveFeasible(ControllerSwitch host, Map<Partition, Collection<Switch>> currentPartitions, Collection<Partition> sourcePartitions, boolean checkResources, boolean selfCommit, Partition removedPartition, Collection<Switch> sources) throws InfeasibleStateException {
            return host.getState();
        }

        @Override
        public void resetRuleCollections(ControllerSwitch aSwitch) throws InfeasibleStateException {
            final CollectionPool.TempCollection<Set<Rule>> tempCollection = ruleSetPool.getTempCollection();
            aSwitch.setState(new MemorySwitch.MemoryState( tempCollection.cloneData()));
            tempCollection.release();
        }

        @Override
        public FeasibleState initToNotOnSrc(ControllerSwitch aSwitch, Collection<Partition> sourcePartitions, boolean selfCommit) {
            return aSwitch.getState();
        }

        @Override
        public void migrate(Switch src, Switch dst, Partition partition) {

        }

    }
}

