package edu.usc.enl.cacheflow.algorithms;

import edu.usc.enl.cacheflow.algorithms.replication.Replicator;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/24/12
 * Time: 10:57 AM
 * To change this template use File | Settings | File Templates.
 */
public interface PostPlacer {

    public  Map<Partition, Map<Switch, Switch>> postPlace(Set<Switch> availableSwitches, Assignment assignment, PrintWriter trendWriter);

    public  Statistics getStats(Map<String,Object> parameters);


}
