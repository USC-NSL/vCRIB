package edu.usc.enl.cacheflow.scripts.test;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/27/12
 * Time: 10:56 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestFatTree {
    public static void main(String[] args) {
        String topologyFile = "input/examples/fattree4.txt";

        try {
            //load topology
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                    Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()),topologyFile,new HashMap<String, Object>(), new LinkedList<Topology>()).get(0);
            final List<Switch> switches = topology.getSwitches();
            final List<List<Link>> allPath = topology.getAllPath(switches.get(switches.size() - 3), switches.get(switches.size() - 4));
            for (List<Link> path : allPath) {
                for (Link link : path) {
                    System.out.println(link);
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
