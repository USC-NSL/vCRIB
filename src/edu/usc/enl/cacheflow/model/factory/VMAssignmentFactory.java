package edu.usc.enl.cacheflow.model.factory;

import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 9/16/12
 * Time: 9:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class VMAssignmentFactory extends FileFactory<Map<Long, Switch>> {
    private final Topology topology;

    public VMAssignmentFactory(EndOfFileCondition stopCondition, Topology topology) {
        super(stopCondition);
        this.topology = topology;
    }

    @Override
    protected Map<Long, Switch> create(String s) {
        return null;
    }

    @Override
    public <C extends Collection<Map<Long, Switch>>> C createBody(BufferedReader reader, C toFill) throws IOException {
        Map<Long, Switch> placement = new HashMap<>();
        while (!stopCondition.stop(reader)) {
            String line = reader.readLine();
            StringTokenizer st = new StringTokenizer(line, ",");
            final String switchId = st.nextToken();
            final Switch host = topology.getSwitchMap().get(switchId);
            if (host==null){
                Util.logger.severe("Could not find switch with id "+switchId+" in the topology file.");
                System.exit(1);
            }
            while (st.hasMoreTokens()) {
                placement.put(Long.parseLong(st.nextToken()), host);
            }
        }
        toFill.add(placement);
        return toFill;
    }
}
