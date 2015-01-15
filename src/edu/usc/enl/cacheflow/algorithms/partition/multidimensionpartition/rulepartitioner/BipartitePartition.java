package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner;

import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.BipartitePartitioner;
import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.partitioner.RuleBipartitePartitioner;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/25/12
 * Time: 9:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class BipartitePartition extends AbstractBipartitePartition{
    final private boolean doThreading;
    private BipartitePartitioner bipartitePartitioner;
    private GeneralizedPartitionerFactory partitionerFactory = null;
    private int maxSize;

    public BipartitePartition(GeneralizedPartitionerFactory partitionerFactory, int maxSize) {
        this.partitionerFactory = partitionerFactory;
        this.bipartitePartitioner = partitionerFactory.create();
        doThreading = true;
        this.maxSize = maxSize;
    }

    public BipartitePartition(BipartitePartitioner bipartitePartitioner, int maxSize) {
        this.bipartitePartitioner = bipartitePartitioner;
        doThreading = false;
        this.maxSize = maxSize;
    }


    public List<Partition> sequentialPartition(Collection<Rule> rules2) throws Exception {
        int sum = 0;
        PriorityQueue<Collection<Rule>> toBePartitioned = new PriorityQueue<Collection<Rule>>(10, new ListSizeComparator());
        toBePartitioned.add(rules2);
        List<Partition> partitions = new ArrayList<Partition>();
        while (toBePartitioned.size() > 0) {
            Collection<Rule> rules = toBePartitioned.poll();
            if (rules.size() == 0) {
                continue;
            }
            if (rules.size() <= maxSize) {
                partitions.add(new Partition(rules));
                continue;
            }
            ((RuleBipartitePartitioner) bipartitePartitioner).partition(rules);
            Collection<Rule> bestPartitionRules1 = bipartitePartitioner.getBestPartitionRules1();
            Collection<Rule> bestPartitionRules2 = bipartitePartitioner.getBestPartitionRules2();

            //System.out.println(rules.size()+" -> "+bestPartitionRules1.size()+", "+bestPartitionRules2.size());

            if (bestPartitionRules1 != null) {
                //keep only to be partitioned
                if (bestPartitionRules1.size() > maxSize) {
                    toBePartitioned.add(bestPartitionRules1);
                    //partitions.addAll(partition(bestPartitionRules1, maxSize));
                } else {
                    partitions.add(new Partition(bestPartitionRules1));
                    sum += bestPartitionRules1.size();
                }
                if (bestPartitionRules2.size() > maxSize) {
                    //partitions.addAll(partition(bestPartitionRules2, maxSize));
                    toBePartitioned.add(bestPartitionRules2);
                } else {
                    partitions.add(new Partition(bestPartitionRules2));
                    sum += bestPartitionRules2.size();
                }
                //System.out.println(sum);
            } else {
                Util.logger.severe("No partition point found");
                System.exit(1);
            }
        }
        return partitions;
    }

    public static class ListSizeComparator implements Comparator<Collection<?>> {

        public int compare(Collection<?> o1, Collection<?> o2) {
            return -(o1.size() - o2.size());
        }
    }

    public List<Partition> partition(Collection<Rule> rules) throws Exception {

        if (rules.size() == 0) {
            return new ArrayList<Partition>();
        }
        if (rules.size() <= maxSize) {
            return Arrays.asList(new Partition(rules));
        }
        ((RuleBipartitePartitioner) bipartitePartitioner).partition(rules);
        Collection<Rule> bestPartitionRules1 = bipartitePartitioner.getBestPartitionRules1();
        Collection<Rule> bestPartitionRules2 = bipartitePartitioner.getBestPartitionRules2();

        List<Partition> partitions = new ArrayList<Partition>();
        if (bestPartitionRules1 != null) {
            //keep only to be partitioned
            if (bestPartitionRules1.size() > maxSize && bestPartitionRules2.size() > maxSize) {
                if (doThreading) {
                    try {
                        //need threading
                        PartitionThread partitionThread = new PartitionThread(maxSize, bestPartitionRules1, partitionerFactory);
                        partitionThread.start();

                        partitions.addAll(partition(bestPartitionRules2));

                        partitionThread.join();
                        partitions.addAll(partitionThread.outputPartition);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    if (bestPartitionRules1.size() > maxSize) {
                        partitions.addAll(partition(bestPartitionRules1));
                    } else {
                        partitions.add(new Partition(bestPartitionRules1));
                    }
                    if (bestPartitionRules2.size() > maxSize) {
                        partitions.addAll(partition(bestPartitionRules2));
                    } else {
                        partitions.add(new Partition(bestPartitionRules2));
                    }
                }
            } else {
                if (bestPartitionRules1.size() > maxSize) {
                    partitions.addAll(partition(bestPartitionRules1));
                } else {
                    partitions.add(new Partition(bestPartitionRules1));
                }
                if (bestPartitionRules2.size() > maxSize) {
                    partitions.addAll(partition(bestPartitionRules2));
                } else {
                    partitions.add(new Partition(bestPartitionRules2));
                }
            }

        } else {
            Util.logger.severe("No median found");
            System.exit(1);
        }

        return partitions;
    }

    public static abstract class GeneralizedPartitionerFactory {
        public abstract BipartitePartitioner create();
    }

    private static class PartitionThread extends Thread {
        int size;
        Collection<Rule> ruleList;
        List<Partition> outputPartition;
        private BipartitePartition.GeneralizedPartitionerFactory partitionerFactory;

        public PartitionThread(int size, Collection<Rule> ruleList, BipartitePartition.GeneralizedPartitionerFactory partitionerFactory) {
            this.size = size;
            this.ruleList = ruleList;
            this.partitionerFactory = partitionerFactory;
        }

        @Override
        public void run() {
            super.run();
            try {
                BipartitePartition balancedPartitioner = new BipartitePartition(partitionerFactory,size);
                outputPartition = balancedPartitioner.partition(ruleList);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}




