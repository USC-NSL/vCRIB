package edu.usc.enl.cacheflow.algorithms.partition;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/29/12
 * Time: 5:52 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class RuleSpacePartition {
    public abstract List<Partition> partition(List<Rule> rules, int maxSize);
}
