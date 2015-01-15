package edu.usc.enl.cacheflow.model.factory;

import edu.usc.enl.cacheflow.model.rule.Cluster;
import edu.usc.enl.cacheflow.model.rule.Partition;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/8/12
 * Time: 10:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class ClusterFactory extends FileFactory<Cluster> {
    private Map<Integer, Partition> idPartitionMap;

    public ClusterFactory(StopCondition stopCondition, Collection<Partition> partitions) {
        super(stopCondition);
        idPartitionMap = new HashMap<Integer, Partition>();
        for (Partition partition : partitions) {
            idPartitionMap.put(partition.getId(), partition);
        }
    }

    @Override
    protected Cluster create(String s) {
        StringTokenizer st = new StringTokenizer(s,",");
        List<Partition> partitions = new LinkedList<Partition>();
        while (st.hasMoreElements()) {
            /////////////////////////////////////////////////////////////
            int key = Integer.parseInt(st.nextElement().toString());
            partitions.add(idPartitionMap.get(key));
        }
        return new Cluster(partitions);
    }
}
