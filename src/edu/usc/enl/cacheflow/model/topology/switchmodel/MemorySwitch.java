package edu.usc.enl.cacheflow.model.topology.switchmodel;

import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/23/12
 * Time: 12:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class MemorySwitch extends Switch {
    private int memoryCapacity;
    private MemoryState state;

    public MemorySwitch(String id, Aggregator aggregator, int memoryCapacity, Set<Rule> memoryTemplate) {
        super(id, aggregator);
        this.memoryCapacity = memoryCapacity;
        state = new MemoryState(Util.getNewCollectionInstance(memoryTemplate));
    }

    @Override
    public void reset() {
        super.reset();
        state.getRules().clear();
        state = new MemoryState(state.getRules());
    }

    @Override
    public void fillParam(Switch aSwitch) {
        this.memoryCapacity = ((MemorySwitch) aSwitch).memoryCapacity;
    }

    @Override
    public double getUsedResources(FeasibleState state) {
        if (memoryCapacity == 0) {
            if (((MemoryState) state).getRules().size() == 0) {
                return 0;
            } else {
                return Double.POSITIVE_INFINITY;
            }
        }
        return (1.0 * ((MemoryState) state).getRules().size() / memoryCapacity);
    }

    @Override
    public double getUsedAbsoluteResources(FeasibleState state) {
        return ((MemoryState) state).getRules().size();
    }

    @Override
    public Switch cloneEmpty(String id) {
        return new MemorySwitch(id, aggregator, memoryCapacity, state.getRules());
    }

    @Override
    public void setState(FeasibleState feasibleState) {
        this.state = (MemoryState) feasibleState;
    }

    @Override
    public FeasibleState getState() {
        return state;
    }

    /*@Override
    public FeasibleState isFeasible(Set<Rule> ruleSet, boolean checkResources, boolean selfCommit) throws InfeasibleStateException {
        final int size;
        try {

            size = ruleSet.size();
            if (size <= memoryCapacity || !checkResources) {
                if (selfCommit) {
                    final Set<Rule> myRules = getState().getRules();
                    if (myRules != ruleSet) {
                        myRules.clear();
                        myRules.addAll(ruleSet);
                    }
                    return getState();
                }
                return new MemoryState(ruleSet);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
        throw new InfeasibleStateException(size + " Memory usage in " + this.toString() + " with capacity " + memoryCapacity);
    }*/

    @Override
    public boolean canSaveMore() {
        return state.getRules().size() < memoryCapacity;
    }

    public int getMemoryCapacity() {
        return memoryCapacity;
    }

    public void toString(PrintWriter p) {
        p.println(id + ", Memory, " + memoryCapacity);
    }

    public static class MemoryState extends FeasibleState {
        protected Set<Rule> rules;

        public MemoryState(Set<Rule> rules) {
            this.rules = rules;
        }

        public Set<Rule> getRules() {
            return rules;
        }

        public void fillRules(Set<Rule> rules) {
            this.rules.clear();
            this.rules.addAll(rules);
        }

        @Override
        public String toString() {
            return "rules=" + rules.size();
        }

        @Override
        public void getRules(Set<Rule> toFill) {
            toFill.addAll(rules);
        }

        @Override
        public FeasibleState clone() {
            final Set<Rule> rules1 = Util.getNewCollectionInstance(rules);
            rules1.addAll(rules);
            return new MemoryState(rules1);
        }
    }
}
