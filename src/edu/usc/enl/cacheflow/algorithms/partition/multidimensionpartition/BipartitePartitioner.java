package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition;

import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/25/12
 * Time: 1:53 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class BipartitePartitioner {
    protected Collection<Rule> bestPartitionRules1;
    protected Collection<Rule> bestPartitionRules2;
    private Aggregator aggregator;

    public BipartitePartitioner(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    public void setAggregator(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    public Collection<Rule> getBestPartitionRules1() {
        return bestPartitionRules1;
    }

    public Collection<Rule> getBestPartitionRules2() {
        return bestPartitionRules2;
    }



    protected void removeMinMax(List<Long> sortedEndPointList, long min, long max) {

        //remove min and max
        {
            final Iterator<Long> iterator = sortedEndPointList.iterator();
            while (iterator.hasNext()) {
                long next = iterator.next();
                if (next == min) {
                    iterator.remove();
                } else if (next > min) {
                    break;
                }
            }

        }
        {
            final ListIterator<Long> iterator = sortedEndPointList.listIterator(sortedEndPointList.size());
            while (iterator.hasPrevious()) {
                long next = iterator.previous();
                if (next == max) {
                    iterator.remove();
                } else if (next < min) {
                    break;
                }
            }
        }
    }

    protected Collection<Rule> aggregate(Collection<Rule> rules1) throws Exception {
        aggregator.setTailInput(rules1);
        rules1 = aggregator.run();
        return rules1;
    }
}
