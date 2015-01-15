package edu.usc.enl.cacheflow.model.exceptions;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/17/11
 * Time: 12:30 AM
 * To change this template use File | Settings | File Templates.
 */
public class NoMatchRuleException extends Exception{
    private Partition partition;
    private Flow flow;
    public NoMatchRuleException(Partition partition, Flow flow) {
        super("No match rule for flow "+flow.toString()+ " in the partition "+ partition.toString());
        this.flow=flow;
        this.partition=partition;
    }
}
