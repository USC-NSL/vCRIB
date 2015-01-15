package edu.usc.enl.cacheflow.algorithms.placement;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/26/11
 * Time: 9:32 PM
 * To change this template use File | Settings | File Templates.
 */
public interface SolutionObserver {
    public void processSolution(Map<Partition, Switch> solution, Topology topology);
}
