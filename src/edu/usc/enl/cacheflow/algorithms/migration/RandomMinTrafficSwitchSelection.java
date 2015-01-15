package edu.usc.enl.cacheflow.algorithms.migration;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/4/12
 * Time: 11:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class RandomMinTrafficSwitchSelection {
    private final int threadNum;
    private final double beta;
    private final Random random;
    private final boolean selectMax;

    private long[] switchTrafficMap;
    private double[] switchProbability;
    private Map<Switch, Integer> switchIndexMap;
    private boolean computeTrafficForFeasiblesOnly;
    private boolean[] feasibleSwitches;
    protected final Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap;

    private AbstractMigrator migrator;

    public RandomMinTrafficSwitchSelection(Random random, int threadNum, double beta, Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) {
        this.random = random;
        this.threadNum = threadNum;
        this.ruleFlowMap = ruleFlowMap;
        this.beta = beta;
        this.selectMax = false;
    }

    public RandomMinTrafficSwitchSelection(Random random, int threadNum, boolean selectMax, Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) {
        this.random = random;
        this.threadNum = threadNum;
        this.ruleFlowMap = ruleFlowMap;
        this.selectMax = selectMax;
        this.beta = 1;
    }

    public void init(List<Switch> switches, AbstractMigrator migrator) {
        this.migrator = migrator;
        switchTrafficMap = new long[switches.size()];
        switchProbability = new double[switches.size()];

        switchIndexMap = new HashMap<Switch, Integer>(switches.size(), 1);
        int i = 0;
        for (Switch aSwitch : switches) {
            switchIndexMap.put(aSwitch, i++);
        }

        feasibleSwitches = new boolean[switches.size()];
    }

    public int getRandomFeasibleSwitch(Partition partition, long partitionOldTraffic, Switch currentHost,
                                       List<Switch> sameAsAllConsiderableSwitches, boolean computeTrafficForFeasiblesOnly,
                                       Map<Switch, Long> trafficHint, double denominator) {
        this.computeTrafficForFeasiblesOnly = computeTrafficForFeasiblesOnly;
        Arrays.fill(switchProbability, 0);
        Arrays.fill(feasibleSwitches, true);
        Util.IntegerWrapper index = new Util.IntegerWrapper(0);
        List<trafficFeasibleTestThread> threads = new ArrayList<trafficFeasibleTestThread>(threadNum);
        for (int i = 0; i < threadNum; i++) {
            threads.add(new trafficFeasibleTestThread(index, sameAsAllConsiderableSwitches,
                    partition, ruleFlowMap.get(partition), partitionOldTraffic, currentHost, trafficHint));
        }
        for (trafficFeasibleTestThread thread : threads) {
            thread.start();
        }

        try {
            for (trafficFeasibleTestThread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (selectMax) {
            //return the maximum index
            int maxIndex = -1;
            double maxValue = 0;
            for (int i = 0, switchTrafficMapLength = switchTrafficMap.length; i < switchTrafficMapLength; i++) {
                if (maxValue < switchTrafficMap[i] || maxIndex < 0) {
                    maxValue = switchTrafficMap[i];
                    maxIndex = i;
                }
            }
            if (maxValue == 0) {//all are infeasible
                return -1;
            }

            return maxIndex;
        } else {
            //now fill cumulative list
            long minFeasibleTraffic = Long.MAX_VALUE;
            double averageFeasibleTraffic = 0;
            {
                int feasibleNum = 0;
                int i = 0;
                for (long traffic : switchTrafficMap) {
                    if (feasibleSwitches[i]) {
                        if (minFeasibleTraffic > traffic) {
                            minFeasibleTraffic = traffic;
                        }
                        averageFeasibleTraffic += traffic;
                        feasibleNum++;
                    }
                    i++;
                }
                averageFeasibleTraffic /= feasibleNum;
            }

            double maxNotCurrentHost = -1;

            final Integer currentHostIndex = switchIndexMap.get(currentHost);
            double currentHostScore = 0;
            for (int i = 0; i < switchTrafficMap.length; i++) {
                if (feasibleSwitches[i]) {
                    switchProbability[i] = convertTrafficToRate(beta, partitionOldTraffic - switchTrafficMap[i] - (partitionOldTraffic - minFeasibleTraffic), denominator);
                }
                if (i != currentHostIndex) {
                    if (maxNotCurrentHost < switchProbability[i]) {
                        maxNotCurrentHost = switchProbability[i];
                    } else {
                        currentHostScore = switchProbability[i];
                    }
                }
                switchProbability[i] += (i == 0 ? 0 : switchProbability[i - 1]);
            }
            if (Double.isInfinite(switchProbability[switchProbability.length - 1])) {
                //fill the switch
                Arrays.fill(switchProbability, 0);
                maxNotCurrentHost = -1;
                int i = 0;
                for (Switch Switch : sameAsAllConsiderableSwitches) {
                    if (feasibleSwitches[i]) {
                        double v = beta * (partitionOldTraffic - switchTrafficMap[i]) / denominator;
                        if (v >= 0) {
                            switchProbability[i] = 1 + v + v * v / 2 + v * v * v / 6;
                        } else {
                            switchProbability[i] = 1.0 / (1 + v + v * v / 2 + v * v * v / 6);
                        }
                        i++;
                    }
                    if (i != currentHostIndex) {
                        if (maxNotCurrentHost < switchProbability[i]) {
                            maxNotCurrentHost = switchProbability[i];
                        }
                    }
                    switchProbability[i] += (i == 0 ? 0 : switchProbability[i - 1]);
                }
            }

            if (switchProbability[switchProbability.length - 1] == 0) {
                return -1;
            }


            /*if (switchTrafficMap[currentHostIndex]==minFeasibleTraffic) {
                migrationNotFeasible = true;
            }*/

            int randomIndex = Util.randomSelect(switchProbability, random);

            return randomIndex;
        }
    }

    public Long getTrafficIf(int candidateSwitchIndex) {
        return switchTrafficMap[candidateSwitchIndex];
    }

    public double getBeta() {
        return beta;
    }

    private class trafficFeasibleTestThread extends Thread {
        private final Util.IntegerWrapper currentIndex;
        private final List<Switch> switches;
        private final Partition partition;
        private Map<Rule, Collection<Flow>> trafficMap;
        private final long partitionOldTraffic;
        private final Switch currentHost;
        private Map<Switch, Long> trafficHint;

        private trafficFeasibleTestThread(Util.IntegerWrapper currentIndex, List<Switch> switches,
                                          Partition partition, Map<Rule, Collection<Flow>> trafficMap,
                                          long partitionOldTraffic, Switch currentHost, Map<Switch, Long> trafficHint) {
            this.currentIndex = currentIndex;
            this.switches = switches;
            this.partition = partition;
            this.trafficMap = trafficMap;
            this.partitionOldTraffic = partitionOldTraffic;
            this.currentHost = currentHost;
            this.trafficHint = trafficHint;
        }

        @Override
        public void run() {
            int index = 0;
            while (true) {
                synchronized (currentIndex) {
                    index = currentIndex.getValue();
                    currentIndex.setValue(index + 1);
                }
                if (index >= switches.size()) {
                    break;
                }
                Switch candidateSwitch = switches.get(index);
                long traffic = 0;
                if (candidateSwitch.equals(currentHost)) {
                    //don't check feasiblity as it may become infeasible
                    //but it must be in the set
                    traffic = partitionOldTraffic;
                } else {
                    //first check feasibility
                    if (!migrator.isNewFeasible(partition, candidateSwitch, false)) {
                        feasibleSwitches[index] = false;
                        if (computeTrafficForFeasiblesOnly) {
//                            switchProbability[index] = 0;
                            continue;
                        }
                    }
                    if (trafficHint != null) {
                        traffic = trafficHint.get(candidateSwitch);
                    } else {
                        //now compute overhead
                        traffic += migrator.topology.getTrafficForHosting(partition, candidateSwitch);
                    }

                }
                switchTrafficMap[index] = traffic;
                //switchProbability[index] = infeasible ? 0 : convertTrafficToRate(beta, partitionOldTraffic - traffic, denominator);

            }
        }

    }

    public static double convertTrafficToRate(double beta, long value, double avg) {
        return Math.exp(beta * value / avg);
    }


    @Override
    public String toString() {
        return "Random Min Traffic";
    }
}
