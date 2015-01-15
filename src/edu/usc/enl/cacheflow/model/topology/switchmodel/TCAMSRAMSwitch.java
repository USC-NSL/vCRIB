package edu.usc.enl.cacheflow.model.topology.switchmodel;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/23/12
 * Time: 12:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class TCAMSRAMSwitch extends Switch {
    public static final String USED_TCAM_MEMORY_STAT = "Used TCAM Memory Capacity";
    public static final String USED_SRAM_MEMORY_STAT = "Used SRAM Memory Capacity";
    private int sramCapacity;
    private int tcamCapacity;
    private TCAMSRAMState state;

    public TCAMSRAMSwitch(String id, Aggregator aggregator, int sramCapacity, int tcamCapacity, Set<Rule> memoryTemplate) {
        super(id, aggregator);
        this.sramCapacity = sramCapacity;
        this.tcamCapacity = tcamCapacity;
        state = new TCAMSRAMState(Util.getNewCollectionInstance(memoryTemplate), Util.getNewCollectionInstance(memoryTemplate));
    }

    @Override
    public void reset() {
        super.reset();
        state.getSramRules().clear();
        state.getTcamRules().clear();
        state = new TCAMSRAMState(state.getSramRules(), state.getTcamRules());
    }

    @Override
    public void fillParam(Switch aSwitch) {
        this.tcamCapacity = ((TCAMSRAMSwitch) aSwitch).tcamCapacity;
        this.sramCapacity = ((TCAMSRAMSwitch) aSwitch).sramCapacity;
    }

    /*@Override
    public FeasibleState isFeasible(Set<Rule> ruleSet, boolean checkResources, boolean selfCommit) throws InfeasibleStateException {
        int exactRules = 0;
        int wildcardRules = 0;
        try {
            //find sram rules;
            for (Rule rule : ruleSet) {

                if (rule.getWildCardBitPattern() == 0) {
                    exactRules++;
                } else {
                    wildcardRules++;
                }

            }
            if (exactRules <= sramCapacity && wildcardRules <= tcamCapacity || !checkResources) {
                if (selfCommit) {
                    final TCAMSRAMState state1 = (TCAMSRAMState) getState();
                    final Set<Rule> myRules = getState().getRules();
                    if (myRules != ruleSet) {
                        myRules.clear();
                        myRules.addAll(ruleSet);
                    }
                    state1.tcamUsage = wildcardRules;
                    state1.sramUsage = exactRules;
                    return state1;
                }
                return new TCAMSRAMState(ruleSet, wildcardRules, exactRules);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
        throw new InfeasibleStateException("TCAM SRAM memory in " + toString() + " for " + exactRules + " exact rules and " +
                wildcardRules + " wildcard rules");
    }*/

    @Override
    public boolean canSaveMore() {
        return state.getSramRules().size() < sramCapacity || state.getTcamRules().size() < tcamCapacity;
    }

    public int getSramCapacity() {
        return sramCapacity;
    }

    public int getTcamCapacity() {
        return tcamCapacity;
    }

    @Override
    public double getUsedResources(FeasibleState state) {
        return (1.0 * ((TCAMSRAMState) state).getSramRules().size() / sramCapacity +
                1.0 * ((TCAMSRAMState) state).getTcamRules().size() / tcamCapacity);
    }

    @Override
    public double getUsedAbsoluteResources(FeasibleState state) {
        return ((TCAMSRAMState) state).getSramRules().size() + ((TCAMSRAMState) state).getTcamRules().size();
    }

    @Override
    public Switch cloneEmpty(String id) {
        return new TCAMSRAMSwitch(id, aggregator, sramCapacity, tcamCapacity, state.getTcamRules());
    }

    public Statistics getStats() {
        Statistics stat = super.getStats();
        stat.addStat(USED_TCAM_MEMORY_STAT, state.getTcamRules().size());
        stat.addStat(USED_SRAM_MEMORY_STAT, state.getSramRules().size());

        return stat;
    }

    @Override
    public void setState(FeasibleState feasibleState) {
        state = (TCAMSRAMState) feasibleState;
    }

    @Override
    public FeasibleState getState() {
        return state;
    }

    public void toString(PrintWriter p) {
        p.println(id + ",TCAMSRAM," + sramCapacity + "," + tcamCapacity);
    }

    public static class TCAMSRAMState extends FeasibleState {
        private Set<Rule> sramRules;
        private Set<Rule> tcamRules;

        private TCAMSRAMState(Set<Rule> sramRules, Set<Rule> tcamRules) {
            this.sramRules = sramRules;
            this.tcamRules = tcamRules;
        }

        public Set<Rule> getSramRules() {
            return sramRules;
        }

        public void setSramRules(Set<Rule> sramRules) {
            this.sramRules = sramRules;
        }

        public Set<Rule> getTcamRules() {
            return tcamRules;
        }

        public void setTcamRules(Set<Rule> tcamRules) {
            this.tcamRules = tcamRules;
        }

        @Override
        public String toString() {
            return super.toString() + " TCAM=" + tcamRules.size() + ", SRAM=" + sramRules.size();
        }


        @Override
        public void getRules(Set<Rule> toFill) {
            toFill.addAll(getTcamRules());
            toFill.addAll(getSramRules());
        }

        @Override
        public FeasibleState clone() {
            final Set<Rule> rulesTcam = Util.getNewCollectionInstance(sramRules);
            rulesTcam.addAll(sramRules);
            final Set<Rule> rulesSram = Util.getNewCollectionInstance(tcamRules);
            rulesSram.addAll(tcamRules);
            return new TCAMSRAMState(rulesSram, rulesTcam);
        }
    }
}
