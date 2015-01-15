package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.equalsize;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PartitionTree2;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/30/12
 * Time: 12:13 AM
 * To change this template use File | Settings | File Templates.
 */
public class EqualSizePartition {
    public List<Partition> sequentialPartition(Collection<Rule> rules2, int numberOfPartitions) throws Exception {
        int totalCuts = (int) (Math.log(numberOfPartitions) / Math.log(2));
        final List<DimensionInfo> dimensionInfos = Util.getDimensionInfos();
        Map<DimensionInfo, Integer> cutPerDimension = getWeightCutsPerDimension(totalCuts, dimensionInfos, rules2);
        //Map<DimensionInfo, Integer> cutPerDimension = getEqualCutsPerDimensions(totalCuts, dimensionInfos);

        Partition initialPartition = getInitialPartition(rules2, dimensionInfos);

        //do partition in each level
        List<Partition> currentPartitions = Arrays.asList(initialPartition);
        int dimIndex = 0;
        for (DimensionInfo info : dimensionInfos) {
            List<Partition> nextLevelPartitions = new LinkedList<Partition>();
            final Integer cuts = cutPerDimension.get(info);
            if (cuts > 0) {
                final List<RangeDimensionRange> ranges = info.getDimensionRange().partition(cuts);
                for (Partition currentPartition : currentPartitions) {
                    //divide rules
                    final Map<RangeDimensionRange, ? extends Collection<Rule>> newRules = Rule.categorizeRuleSpace(currentPartition.getRules(), ranges, dimIndex);
                    for (RangeDimensionRange range : ranges) {
                        final List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(currentPartition.getProperties());
                        properties.set(dimIndex, range);
                        nextLevelPartitions.add(new Partition(newRules.get(range), properties));
                    }
                }
                currentPartitions = nextLevelPartitions;
            }
            dimIndex++;
        }

        Set<Long> wc = new HashSet<Long>();
        for (Partition partition : currentPartitions) {
            final long wildcardPattern = partition.getWildcardPattern();
            //System.out.println(RangeDimensionRange.reverseWildcardPattern(wildcardPattern, Util.getDimensionInfos()));
            wc.add(wildcardPattern);
        }

        System.out.println(wc.size());

        return currentPartitions;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    private Map<DimensionInfo, Integer> getWeightCutsPerDimension(int totalCuts, List<DimensionInfo> dimensionInfos,
                                                                  Collection<Rule> rules) {
        int index = 0;
        double sum = 0;
        final Map<DimensionInfo, Double> weights = new HashMap<DimensionInfo, Double>();
        final double log2 = Math.log(2);
        for (DimensionInfo info : dimensionInfos) {
            double weight = Math.log(PartitionTree2.findRanges(rules, index).size()) / log2;
            sum += weight;
            weights.put(info, weight); //temporary weight
            index++;
        }

        for (Map.Entry<DimensionInfo, Double> entry : weights.entrySet()) {
            entry.setValue(totalCuts * entry.getValue() / sum);
        }

        int outputSum = 0;
        Map<DimensionInfo, Integer> cutPerDimension = new HashMap<DimensionInfo, Integer>();
        for (Map.Entry<DimensionInfo, Double> entry : weights.entrySet()) {
            final int value = entry.getValue().intValue();
            cutPerDimension.put(entry.getKey(), value);
            outputSum += value;
        }


        //distribute the remaining priority based on their weights %1
        LinkedList<DimensionInfo> sortedList = new LinkedList<DimensionInfo>(cutPerDimension.keySet());
        Collections.sort(sortedList, new Comparator<DimensionInfo>() {
            public int compare(DimensionInfo o1, DimensionInfo o2) {
                final double v = weights.get(o2) % 1 - weights.get(o1) % 1;
                if (v > 0) {
                    return 1;
                } else if (v < 0) {
                    return -1;
                }
                return 0;
            }
        });

        while (outputSum < totalCuts) {
            final DimensionInfo info = sortedList.pop();
            cutPerDimension.put(info, cutPerDimension.get(info) + 1);
            sortedList.add(info);
            outputSum++;
        }

        for (Map.Entry<DimensionInfo, Integer> entry : cutPerDimension.entrySet()) {
            entry.setValue((int) Math.pow(2, entry.getValue()));
        }
        return cutPerDimension;
    }

    private Map<DimensionInfo, Integer> getEqualCutsPerDimensions(int totalCuts, List<DimensionInfo> dimensionInfos) {
        Map<DimensionInfo, Integer> cutPerDimension = new HashMap<DimensionInfo, Integer>();
        for (DimensionInfo info : dimensionInfos) {
            cutPerDimension.put(info, (int) (Math.pow(2, totalCuts / dimensionInfos.size())));
        }
        return cutPerDimension;
    }

    private Partition getInitialPartition(Collection<Rule> rules2, List<DimensionInfo> dimensionInfos) {
        List<RangeDimensionRange> properties = new LinkedList<RangeDimensionRange>();
        for (DimensionInfo info : dimensionInfos) {
            properties.add(info.getDimensionRange());
        }
        return new Partition(rules2, properties);
    }
}
