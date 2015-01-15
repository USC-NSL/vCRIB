package edu.usc.enl.cacheflow.model.rule;


import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/8/12
 * Time: 9:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class Cluster {
    private Collection<Partition> partitions;
    private Set<Rule> rules;

    public Cluster(Collection<Partition> partitions) {
        this.partitions = partitions;
        if (partitions.size() > 0) {
            final Collection<Rule> firstRules = partitions.iterator().next().getRules();
            if (firstRules instanceof Set) {
                rules = (Set<Rule>) Util.getNewCollectionInstance(firstRules);
            }
        }
        if (rules == null) {
            rules = new HashSet<Rule>();
        }
        for (Partition partition : partitions) {
            rules.addAll(partition.getRules());
        }
    }

    public int getRulesNum() {
        return rules.size();
    }

    public int getSize() {
        return partitions.size();
    }

    public Collection<Partition> getPartitions() {
        return partitions;
    }

    public Set<Rule> getRules() {
        return rules;
    }
}
