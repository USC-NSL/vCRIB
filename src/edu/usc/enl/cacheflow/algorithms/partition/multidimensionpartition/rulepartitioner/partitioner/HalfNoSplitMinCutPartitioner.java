package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.partitioner;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/23/12
 * Time: 8:53 PM
 */
public class HalfNoSplitMinCutPartitioner {
    protected double balanceWeight = 0;
    protected double addWildcardWeight = 0;
    protected Partition bestPartition1;
    protected Partition bestPartition2;
    protected long minHalfWildCard = -1;
    public long currentPartitionWC;

    public HalfNoSplitMinCutPartitioner(double balanceWeight, double addWildcardWeight) {
        this.balanceWeight = balanceWeight;
        this.addWildcardWeight = addWildcardWeight;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public void partition(Partition partition, Map<Long, Integer> wildCardPatterns) {
        bestPartition1=null;
        bestPartition2=null;
        try {
            int minDimension = -1;
            double minMetric = 0;
            //in each dimension of partition check the half
            final List<DimensionInfo> dimensionInfos = Util.getDimensionInfos();
            List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(partition.getProperties());
            final Collection<Rule> rules = partition.getRules();
            currentPartitionWC = partition.getWildcardPattern();
            int currentPartitionWCFreq = wildCardPatterns.get(currentPartitionWC);
            for (int dim = 0; dim < dimensionInfos.size(); dim++) {
                RangeDimensionRange range = partition.getProperty(dim);
                //find half
                if (range.getSize() == 1) {
                    continue;
                }
                long half = range.getStart() + range.getSize() / 2;
                int right = 0;
                int left = 0;
                for (Rule rule : rules) {
                    RangeDimensionRange property = rule.getProperty(dim);
                    if (property.getEnd() >= half) {
                        right++;
                    }
                    if (property.getStart() < half) {
                        left++;
                    }
                }
                RangeDimensionRange halfDimensionRange = new RangeDimensionRange(range.getStart(), half - 1, range.getInfo());
                properties.set(dim, halfDimensionRange);

                //compute metric
                final long halfWildCard = RangeDimensionRange.computeWildcardPattern(properties);
                Integer newWildcardFreq = wildCardPatterns.get(halfWildCard);
                int wildcardDiff = (newWildcardFreq == null || newWildcardFreq == 0) ? 1 : 0;//will it add wildcard or not
                if (currentPartitionWCFreq == 1) {//it will be removed
                    wildcardDiff--;
                }
                double metric = balanceWeight * Math.abs(left - right) + (left + right) + (-wildcardDiff * addWildcardWeight);
                //System.out.print("dim: " + dim+"=("+range.getStart()+","+half+","+range.getEnd() + ")," + metric + "=(" + left + "," + right + "," + wildcardDiff + "), ");
                if (minDimension < 0 || minMetric > metric) {
                    minDimension = dim;
                    minHalfWildCard = halfWildCard;
                    minMetric = metric;
                }
                properties.set(dim, partition.getProperty(dim));//revert properties to the same as the partition
            }
            if (minDimension > -1) {
                //System.out.println();
                //System.out.print(minDimension+ " ");
                RangeDimensionRange range = partition.getProperty(minDimension);
                //find half
                long half = range.getStart() + range.getSize() / 2;
                List<Rule> rightRules = new ArrayList<Rule>(rules.size());
                List<Rule> leftRules = new ArrayList<Rule>(rules.size());
                for (Rule rule : rules) {
                    RangeDimensionRange property = rule.getProperty(minDimension);
                    if (property.getEnd() >= half) {
                        rightRules.add(rule);
                    }
                    if (property.getStart() < half) {
                        leftRules.add(rule);
                    }
                }


                RangeDimensionRange halfDimensionRange = new RangeDimensionRange(range.getStart(), half - 1, range.getInfo());
                properties = new ArrayList<RangeDimensionRange>(properties);
                properties.set(minDimension, halfDimensionRange);
                bestPartition1 = new Partition(leftRules, properties);


                halfDimensionRange = new RangeDimensionRange(half, range.getEnd(), range.getInfo());
                properties = new ArrayList<RangeDimensionRange>(properties);
                properties.set(minDimension, halfDimensionRange);
                bestPartition2 = new Partition(rightRules, properties);
            }
        } catch (UnalignedRangeException e) {
            e.printStackTrace();
        }
    }

    public long getCurrentPartitionWC() {
        return currentPartitionWC;
    }

    public Partition getBestPartition1() {
        return bestPartition1;
    }

    public Partition getBestPartition2() {
        return bestPartition2;
    }

    public long getMinHalfWildCard() {
        return minHalfWildCard;
    }
}
