package edu.usc.enl.cacheflow.processor.flow.classifier;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/8/12
 * Time: 5:17 PM
 * To change this template use File | Settings | File Templates.
 */
public interface PartitionClassifier {

    public Map<Partition, Map<Rule, Collection<Flow>>> classify(Collection<Flow> flows, Collection<Partition> partitions);
}
