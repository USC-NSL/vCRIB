package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.partitioner;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/25/12
 * Time: 2:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class RuleMinCutPartitioner extends RuleBipartitePartitioner {
    protected double balanceWeight = 0;
    protected double searchArea = 0;


    public RuleMinCutPartitioner(Aggregator aggregator, double balanceWeight, double searchArea) {
        super(aggregator);
        this.balanceWeight = balanceWeight;
        this.searchArea = searchArea;
    }

    public void partition(Collection<Rule> rules) throws Exception {
        //find points in each dimension

        bestPartitionRules1 = null;
        bestPartitionRules2 = null;

        long minEndpoint = 0;
        int minEndPointDimension = -1;
        double minWeight = Double.MAX_VALUE;
        long minMin = 0;
        long minMax = 0;

        final List<DimensionInfo> dimensionInfos = Util.getDimensionInfos();
        PreparePoints preparePoints = new PreparePoints(rules);
        for (int dim = 0; dim < dimensionInfos.size(); dim++) {            
            //final DimensionInfo dimensionInfo = dimensionInfos.get(dim);
            preparePoints.invoke(dim);
            if (preparePoints.skip()) continue;
            int start = preparePoints.getStart();
            int end = preparePoints.getEnd();
            List<Long> endpointsList = preparePoints.getEndpointsList();
            long min = preparePoints.getMin();
            long max = preparePoints.getMax();


            //count the number of rules that cut each interior endpoint
            for (int i = start; i < end; i++) {
                Long endpoint = endpointsList.get(i);
                int sum = 0;
                int left = 0;
                int right = 0;
                for (Rule rule : rules) {
                    final RangeDimensionRange property = rule.getProperty(dim);
                    if (property.getEnd() > endpoint) {
                        right++;
                    }
                    if (property.getStart() <= endpoint) {
                        left++;
                    }
                    if (property.getStart() <= endpoint && property.getEnd() > endpoint) {
                        sum++;
                    }
                }
                final double weight = (1 - balanceWeight) * sum + balanceWeight * (Math.abs(left - right));
                if (minWeight > weight) {

                    minWeight = weight;
                    minEndpoint = endpoint;
                    minEndPointDimension = dim;
                    minMin = min;
                    minMax = max;
                }
            }
        }
        if (minEndPointDimension==-1){
            System.out.println();
        }
        DimensionInfo info = dimensionInfos.get(minEndPointDimension);
        List<RangeDimensionRange> superRanges = Arrays.asList(new RangeDimensionRange(minMin+1, minEndpoint, info),
                new RangeDimensionRange(minEndpoint + 1, minMax, info));
        final Map<RangeDimensionRange, Collection<Rule>> newRules = Rule.partitionRuleSpace(rules, superRanges, minEndPointDimension);
        final Iterator<Collection<Rule>> iterator = newRules.values().iterator();
        bestPartitionRules1 = aggregate(iterator.next());
        bestPartitionRules2 = aggregate(iterator.next());

    }

    protected class PreparePoints {
        private boolean skip;
        private Collection<Rule> rules;
        private List<Long> endpointsList;
        private long min;
        private long max;
        private int start;
        private int end;

        public PreparePoints(Collection<Rule> rules) {
            this.rules = rules;
        }

        public boolean skip() {
            return skip;
        }

        public List<Long> getEndpointsList() {
            return endpointsList;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }

        public PreparePoints invoke(int dim) {
            //find median for each dimension
            endpointsList = new ArrayList<Long>(rules.size() * 2);
            for (Rule rule : rules) {
                final RangeDimensionRange property = rule.getProperty(dim);
                endpointsList.add(property.getStart()-1);
                endpointsList.add(property.getEnd());
            }
            //Remove min max needs sorting
            Collections.sort(endpointsList);
            min = endpointsList.get(0);
            max = endpointsList.get(endpointsList.size() - 1);
            removeMinMax(endpointsList, min, max);

            if (endpointsList.size() == 0) {//no interior median
                skip = true;
                return this;
            }

            start = 0;
            end = 0;

            if (searchArea < 1) {


                long median = endpointsList.get(endpointsList.size() / 2);
                //remove duplicates
                //DON'T MOVE THIS LINE TO UP
                endpointsList = new ArrayList<Long>(new HashSet<Long>(endpointsList));
                //find the index of median
                int medianIndex = 0;
                for (Long aLong : endpointsList) {
                    if (aLong.equals(median)) {
                        break;
                    }
                    medianIndex++;
                }

                final double variation = searchArea / 2 * endpointsList.size();
                start = Math.max(medianIndex - (int) variation, 0);
                end = Math.min(medianIndex + (int) variation + 1, endpointsList.size());//+1 is to keep at least the median
            } else {
                endpointsList = new ArrayList<Long>(new HashSet<Long>(endpointsList));
                start = 0;
                end = endpointsList.size();
            }
            skip = false;
            return this;
        }
    }
}