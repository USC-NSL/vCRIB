package edu.usc.enl.cacheflow.model.factory;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchhelper.SwitchHelper;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/8/12
 * Time: 5:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class AssignmentFactory extends FileFactory<Assignment> {
    private List<Partition> partitions;
    private Topology topology;

    public AssignmentFactory(StopCondition stopCondition, Topology topology, List<Partition> partitions) {
        super(stopCondition);
        this.partitions = partitions;
        this.topology = topology;
    }

    private AssignmentFactory(StopCondition stopCondition) {
        super(stopCondition);
    }

    private void init(Topology topology, List<Partition> partitions) {
        this.topology = topology;
        this.partitions = partitions;
    }

    @Override
    public <C extends Collection<Assignment>> C createBody(BufferedReader reader, C toFill) throws IOException {
        Map<Partition, Switch> placement = new HashMap<>(partitions.size());
        while (!stopCondition.stop(reader)) {
            String line = reader.readLine();
            StringTokenizer st = new StringTokenizer(line, ",");
            final String switchId = st.nextToken();
            final Switch host = topology.getSwitchMap().get(switchId);
            while (st.hasMoreTokens()) {
                try {
                    placement.put(partitions.get(Integer.parseInt(st.nextToken()) - 1), host);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        toFill.add(new Assignment(placement));
        return toFill;
    }

    @Override
    protected Assignment create(String s) {
        return null;
    }

    public static class LoadPlacer extends Placer {
        protected final AssignmentFactory assignmentFileFactory;
        protected final String inputFolder;
        protected final Map<String, Object> parameters;
        protected final List<Partition> partitions;
        protected final Assignment assignment;


        public LoadPlacer(boolean checkLinks, Map<Partition, Map<Switch, Rule>> forwardingRules,
                          String inputFolder, Map<String, Object> parameters,
                          List<Partition> partitions, Map<Switch, Collection<Partition>> sourcePartitions) {
            super(checkLinks, forwardingRules, sourcePartitions, Util.threadNum);
            this.assignmentFileFactory = new AssignmentFactory(new EndOfFileCondition());
            this.inputFolder = inputFolder;
            this.parameters = parameters;
            this.partitions = partitions;
            assignment = null;
        }

        public LoadPlacer(boolean checkLinks, Map<Partition, Map<Switch, Rule>> forwardingRules,
                          Assignment assignment,
                          Map<Switch, Collection<Partition>> sourcePartitions) {
            super(checkLinks, forwardingRules, sourcePartitions, Util.threadNum);
            this.inputFolder = null;
            this.parameters = null;
            this.assignmentFileFactory = null;
            this.partitions = null;
            this.assignment = assignment;
        }

        @Override
        public Collection<Switch> getLastAvailableSwitches() {
            return topology.getSwitches();
        }

        @Override
        public Assignment place2(Topology topology, Collection<Partition> partitions) throws NoAssignmentFoundException {
            Assignment currentAssignment = assignment;
            try {
                if (currentAssignment == null) {
                    currentAssignment = loadAssignment(topology, assignmentFileFactory, inputFolder, this.partitions, parameters);
                    if (currentAssignment == null) {
                        throw new NoAssignmentFoundException();
                    }
                }

                for (Switch aSwitch : topology.getSwitches()) {
                    final SwitchHelper<Switch> helper = topology.getHelper(aSwitch);
                    helper.initToNotOnSrc(aSwitch, sourcePartitions.get(aSwitch), true);
                    final Set<Partition> rPlacement = currentAssignment.getRplacement().get(aSwitch);
                    if (rPlacement != null) {
                        try {
                            topology.getHelper(aSwitch).isAddMultipleFeasible(aSwitch, rPlacement, forwardingRules, true);
                        } catch (Switch.InfeasibleStateException e) {
                            e.printStackTrace();
                            helper.initToNotOnSrc(aSwitch, sourcePartitions.get(aSwitch), true);
                            topology.getHelper(aSwitch).isAddMultipleFeasible(aSwitch, rPlacement, forwardingRules, true);
                            throw new NoAssignmentFoundException(e);
                        }
                    }
                }
                currentAssignment.updateForwardingRules(forwardingRules);
                return currentAssignment;
            } catch (Switch.InfeasibleStateException e) {
                throw new NoAssignmentFoundException(e);
            }
        }

        public static Assignment loadAssignment(Topology topology, AssignmentFactory assignmentFileFactory, String inputFolder,
                                                List<Partition> partitions, Map<String, Object> parameters) throws NoAssignmentFoundException {
            assignmentFileFactory.init(topology, partitions);
            try {
                final File inputFolderFile = new File(inputFolder);
                for (File file : inputFolderFile.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.contains("assignment");
                    }
                })) {
                    Map<String, Object> parameters2 = new HashMap<>();
                    Util.loadParameters(assignmentFileFactory, file.getPath(), parameters2);
                    if (Util.haveEqualParameters(parameters, parameters2, "topology\\..*")) {
                        System.out.println("loading from " + file);
                        return Util.loadFileFilterParam(assignmentFileFactory, file.getPath(), parameters, new LinkedList<Assignment>(), "placement.*").get(0);
                    }
                }
            } catch (IOException e) {
                throw new NoAssignmentFoundException(e);
            }
            throw new NoAssignmentFoundException("Assignment not found");
        }
    }
}
