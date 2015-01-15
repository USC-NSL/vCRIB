package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;

import java.util.Collection;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/23/12
 * Time: 9:00 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractBipartitePartition {
    public abstract List<Partition> sequentialPartition(Collection<Rule> rules2) throws Exception;
}
