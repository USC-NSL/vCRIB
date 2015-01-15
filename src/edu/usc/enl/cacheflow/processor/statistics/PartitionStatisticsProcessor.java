package edu.usc.enl.cacheflow.processor.statistics;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.processor.Processor;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/12/11
 * Time: 9:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class PartitionStatisticsProcessor extends StatisticsProcessor<List<Partition>> {
    public static final String NUMBER_OF_PARTITIONS_STAT = "Number of Partitions";
    public static final String MAX_SIZE_OF_PARTITIONS_STAT = "Max Size of Partitions";
    public static final String MEAN_SIZE_OF_PARTITIONS_STAT = "Mean Size of Partitions";
    public static final String STD_SIZE_OF_PARTITIONS_STAT = "Std Size of Partitions";
    public static final String SUM_SIZE_OF_PARTITIONS_STAT = "Sum Size of Partitions";
    public static final String FORWARDING_RULES_WILDCARD_STAT = "Forwarding Rules " + Partition.WILDCARD_SETS_STAT;
    public static final String MEAN_WILDCARD_PATTERN_INSIDE_STAT = "Mean " + Partition.WILDCARD_SETS_STAT;
    public static final String STD_WILDCARD_PATTERN_INSIDE_STAT = "Std " + Partition.WILDCARD_SETS_STAT;
    public static final String MEAN_SIMILARITY = "Mean Similarity";

    public PartitionStatisticsProcessor(List<Partition> input, Map<String, Object> parameters) {
        super(input, parameters);
    }

    public PartitionStatisticsProcessor(Processor<?, List<Partition>> processorInput, Map<String, Object> parameters) {
        super(processorInput, parameters);
    }

    @Override
    public Statistics getStat(List<Partition> partitions) throws Exception {

        Statistics stats = new Statistics();
        stats.addStat(NUMBER_OF_PARTITIONS_STAT, partitions.size());

        List<Statistics> statsList = new ArrayList<Statistics>(partitions.size());
        for (Partition partition : partitions) {
            statsList.add(partition.getStats());
        }
        {
            final Double mean = Statistics.getMean(statsList, Partition.NUMBER_OF_RULES_STAT);
            stats.addStat(MEAN_SIZE_OF_PARTITIONS_STAT, mean);
            stats.addStat(STD_SIZE_OF_PARTITIONS_STAT, Statistics.getVar(statsList, Partition.NUMBER_OF_RULES_STAT, mean));
            stats.addStat(SUM_SIZE_OF_PARTITIONS_STAT, Statistics.getSum(statsList, Partition.NUMBER_OF_RULES_STAT));
            stats.addStat(MAX_SIZE_OF_PARTITIONS_STAT, Statistics.getMax(statsList, Partition.NUMBER_OF_RULES_STAT));
        }

        {
            final Double mean = Statistics.getMean(statsList, Partition.WILDCARD_SETS_STAT);
            stats.addStat(MEAN_WILDCARD_PATTERN_INSIDE_STAT, mean);
            stats.addStat(STD_WILDCARD_PATTERN_INSIDE_STAT, Statistics.getVar(statsList, Partition.WILDCARD_SETS_STAT, mean));
        }
        List<Rule> forwardingRules = new ArrayList<Rule>(partitions.size());
        int i = -1;
        for (Partition partition : partitions) {
            forwardingRules.add(new Rule(DenyAction.getInstance(), partition.getProperties(), 0, i--));
        }

        stats.addStat(FORWARDING_RULES_WILDCARD_STAT, RulesStatisticsProcessor.getNumberOfUniqueWildcardSets2(forwardingRules));

        stats.addStat(MEAN_SIMILARITY, computeMutualSimilarity(partitions, null));

        return stats;
    }

    public double computeMutualSimilarity(List<Partition> partitions, List<Rule> rules) {
        if (partitions.size() == 0) {
            return 0;
        }
        final List<MatrixRuleSet> ruleSets = new ArrayList<>(partitions.size());
        if (partitions.get(0).getRules() instanceof MatrixRuleSet) {
            for (Partition partition : partitions) {
                ruleSets.add((MatrixRuleSet) partition.getRules());
            }
        } else {
            if (rules == null) {
                Set<Rule> rules1 = new HashSet<>();
                for (Partition partition : partitions) {
                    rules1.addAll(partition.getRules());
                }
                rules = new ArrayList<>(rules1);
                Collections.sort(rules, Rule.ID_COMPARATOR);
            }

            int i = 0;

            MatrixRuleSet.setRuleSet(rules);
            for (Partition partition : partitions) {
                MatrixRuleSet matrixRuleSet = new MatrixRuleSet();
                matrixRuleSet.addAll(partition.getRules());
                ruleSets.add(matrixRuleSet);
            }
        }


        Thread[] threads = new Thread[Util.threadNum];
        final Util.IntegerWrapper index = new Util.IntegerWrapper(0);
        final Util.DoubleWrapper sumSumSim = new Util.DoubleWrapper(0);
        for (int j = 0; j < threads.length; j++) {
            threads[j] = new Thread() {
                @Override
                public void run() {
                    while (true) {
                        MatrixRuleSet ruleSet1;
                        int currentIndex;
                        synchronized (index) {
                            currentIndex = index.getValue();
                            if (currentIndex >= ruleSets.size() - 1) {
                                break;
                            }
                            ruleSet1 = ruleSets.get(currentIndex);
                            index.setValue(currentIndex + 1);
                            if (currentIndex % 1000 == 0) {
                                System.out.println(currentIndex);
                            }
                        }
                        int sumSim = 0;
                        Iterator<MatrixRuleSet> itr2 = ruleSets.listIterator(currentIndex + 1);
                        while (itr2.hasNext()) {
                            MatrixRuleSet ruleSet2 = itr2.next();
                            int similarity = ruleSet1.getSimilarity(ruleSet2);
                            sumSim += similarity;
                        }
                        synchronized (sumSumSim) {
                            sumSumSim.increment(sumSim);
                        }
                    }
                }
            };
        }
        Util.runThreads(threads);


        /*for (Iterator<MatrixRuleSet> itr1 = ruleSets.iterator(); itr1.hasNext(); i++) {
            if (i < partitions.size() - 1) {
                MatrixRuleSet ruleset1 = itr1.next();
                Iterator<MatrixRuleSet> itr2 = ruleSets.listIterator(i + 1);
                while (itr2.hasNext()) {
                    MatrixRuleSet ruleset2 = itr2.next();
                    sumSim += ruleset1.getSimilarity(ruleset2);
                }
            }
        }*/
        return sumSumSim.getValue() * 2d / partitions.size() / (partitions.size() - 1);
    }
}
