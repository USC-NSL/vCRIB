package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner;

import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.BipartitePartitioner;
import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.partitioner.RuleBipartitePartitioner;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/25/12
 * Time: 9:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class BipartitePartitionMemory extends AbstractBipartitePartition{
    private BipartitePartitioner generalizedPartitioner;
    private int memory;
    private int sources;

    public BipartitePartitionMemory(BipartitePartitioner generalizedPartitioner, int memory, int sources) {
        this.generalizedPartitioner = generalizedPartitioner;
        this.memory = memory;
        this.sources = sources;
    }


    public List<Partition> sequentialPartition(Collection<Rule> rules2) throws Exception {
        int totalUntilNow = rules2.size();

        PriorityQueue<Collection<Rule>> toBePartitioned = new PriorityQueue<Collection<Rule>>(10, new BipartitePartition.ListSizeComparator());
        toBePartitioned.add(rules2);
        List<Partition> partitions = new ArrayList<Partition>();
        while (toBePartitioned.size() > 0) {
            Collection<Rule> rules = toBePartitioned.poll();
            if (rules.size() == 0) {
                continue;
            }
            if (rules.size() <= 2) {//not partition less than 2
                partitions.add(new Partition(rules));
                continue;
            }
            ((RuleBipartitePartitioner) generalizedPartitioner).partition(rules);
            Collection<Rule> bestPartitionRules1 = generalizedPartitioner.getBestPartitionRules1();
            Collection<Rule> bestPartitionRules2 = generalizedPartitioner.getBestPartitionRules2();

            //System.out.println(rules.size()+" -> "+bestPartitionRules1.size()+", "+bestPartitionRules2.size());

            if (bestPartitionRules1 != null) {
                //keep only to be partitioned
                int newTotal = totalUntilNow + bestPartitionRules1.size() + bestPartitionRules2.size() - rules.size();
                final int memoryForRules = getMemoryForRules(partitions.size() + toBePartitioned.size() + 2);
                if (newTotal < memoryForRules) {
                    toBePartitioned.add(bestPartitionRules1);
                    toBePartitioned.add(bestPartitionRules2);
                    totalUntilNow = newTotal;
                } else {
                    partitions.add(new Partition(rules));
                }
            } else {
                Util.logger.severe("No partition point found");
                System.exit(1);
            }
        }
        System.out.println(partitions.size() + " created with size " + totalUntilNow + " from " + rules2.size() + " rules");
        return partitions;
    }

    private int getMemoryForRules(int numberOfPartitions) {
        return memory - numberOfPartitions * sources;
    }

}




