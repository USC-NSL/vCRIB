package edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PartitionTree2;
import edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.partitioner.Partitioner;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/28/11
 * Time: 7:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class RuleSpacePartitioner {
    private List<DimensionInfo> dimensionsName;
    private Partitioner partitioner;
    private Aggregator aggregator;
    private Aggregator.AggregatorFactory aggregatorFactory;

    public RuleSpacePartitioner(List<DimensionInfo> dimensionsName, Partitioner partitioner, Aggregator aggregator) {
        this.dimensionsName = dimensionsName;
        this.partitioner = partitioner;
        this.aggregator = aggregator;
    }

    public RuleSpacePartitioner(List<DimensionInfo> dimensionsName, Partitioner partitioner, Aggregator.AggregatorFactory aggregatorFactory) {
        this.dimensionsName = dimensionsName;
        this.partitioner = partitioner;
        this.aggregatorFactory = aggregatorFactory;
    }

    public List<Partition> partition(int size, Collection<Rule> rules) throws Exception {
        // not important to have sorted rules

        int divideInEachStep = 2;
        List<Partition> partitions = new LinkedList<Partition>();


        aggregator.setTailInput(rules);
        rules = aggregator.run();

        if (rules.size() > size) {
            //need to make new partition
            List<Integer> sortedPermutation = PartitionTree2.findPermutation(rules, dimensionsName);
            int selectedDimension =sortedPermutation.get(0);
            List<RangeDimensionRange> ranges = PartitionTree2.findRanges(rules, selectedDimension);
            if (ranges.size() < divideInEachStep) {
                System.out.println("wrong choice of dimension");
            }

            // need to select a border
            List<RangeDimensionRange> superRanges = partitioner.partition(ranges, divideInEachStep, rules, aggregator);
            Map<RangeDimensionRange, Collection<Rule>> outputPartitions = Rule.partitionRuleSpace(rules, superRanges, selectedDimension);

            for (Collection<Rule> ruleList : outputPartitions.values()) {
                partitions.addAll(partition(size, ruleList));

            }
        } else {
            partitions.add(new Partition(rules));
        }

        return partitions;
    }

    public List<Partition> partitionThread(int size, Collection<Rule> rules) throws Exception {
        // not important to have sorted rules

        int divideInEachStep = 2;
        List<Partition> partitions = new LinkedList<Partition>();

        aggregator = aggregatorFactory.getProcessor();
        aggregator.setTailInput(rules);
        rules = aggregator.run();

        if (rules.size() > size) {
            //need to make new partition
            List<Integer> sortedPermutation = PartitionTree2.findPermutation(rules, dimensionsName);
            int selectedDimension =sortedPermutation.get(0);
            List<RangeDimensionRange> ranges = PartitionTree2.findRanges(rules, selectedDimension);
            if (ranges.size() < divideInEachStep) {
                System.out.println("wrong choice of dimension");
            }
            // need to select borders
            List<RangeDimensionRange> superRanges = partitioner.partition(ranges, divideInEachStep, rules, aggregator);

            //get rules in each range
            Map<RangeDimensionRange, Collection<Rule>> outputPartitions = Rule.partitionRuleSpace(rules, superRanges, selectedDimension);
            List<PartitionThread> threads = new ArrayList<PartitionThread>(outputPartitions.size());

            //keep the first partition for myself
            Iterator<Collection<Rule>> iterator = outputPartitions.values().iterator();
            Collection<Rule> myRuleList = iterator.next();

            //for other partitions create threads
            for (; iterator.hasNext(); ) {
                Collection<Rule> ruleList = iterator.next();
                PartitionThread partitionThread = new PartitionThread(size, ruleList);
                threads.add(partitionThread);
                partitionThread.start();
            }

            //do my role
            partitions.addAll(partition(size,myRuleList));

            //gather other threads output
            for (PartitionThread thread : threads) {
                thread.join();
                partitions.addAll(thread.outputPartition);
            }
        } else {
            partitions.add(new Partition(rules));
        }

        return partitions;
    }


    private class PartitionThread extends Thread {
        int size;
        Collection<Rule> ruleList;
        List<Partition> outputPartition;

        private PartitionThread(int size, Collection<Rule> ruleList) {
            this.size = size;
            this.ruleList = ruleList;
        }

        @Override
        public void run() {
            super.run();
            try {
                outputPartition = partitionThread(size, ruleList);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
