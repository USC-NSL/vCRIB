package edu.usc.enl.cacheflow.model.topology;

import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import javax.swing.*;
import java.awt.*;
import java.util.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/25/12
 * Time: 4:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class Large4LevelFatTree extends AbstractFatTree {
    private List<Switch> cores = new ArrayList<Switch>();
    private DirectedSparseGraph<Switch, Link> graph;
    private Map<String, Switch> switchIDSwitchMap = new HashMap<String, Switch>();
    private int digits;
    private Map<Switch, Integer> switchPodNumber = new HashMap<Switch, Integer>();
    private Map<Switch, Byte> switchType = new HashMap<Switch, Byte>();
    private final static byte EDGE = 0;
    private final static byte AGG2 = 1;
    private final static byte AGG1 = 2;
    private final static byte CORE = 3;


    public static final String SUM_CORE_TRAFFIC = "Sum Core Traffic";
    public static final String SUM_AGG_TRAFFIC = "Sum Agg Traffic";
    public static final String SUM_EDGE_TRAFFIC = "Sum Edge Traffic";


    public Large4LevelFatTree() {
    }

    @Override
    public List<List<Link>> getAllPath(SwitchPair pair) {
        return new ArrayList<List<Link>>();
    }

    public int getCoresSize() {
        return cores.size();
    }

    public void addCoreSwitch(Switch aSwitch) {
        cores.add(aSwitch);
        super.addSwitch(aSwitch);
    }

    public List<Switch> getCores() {
        return cores;
    }

    private Integer getIthNum(String input, int prefixLength, int i) {
        return Integer.parseInt(input.substring(prefixLength + (i + 1) + digits * i, prefixLength + (i + 1) + digits * (i + 1)));
    }

    @Override
    public int getPathLength(Switch switch1, Switch switch2) {
        if (switch1.equals(switch2)) {
            return 0;
        }
        if (switchType.get(switch1) == EDGE) {
            if (switchType.get(switch2) == EDGE) {
                if (switchPodNumber.get(switch1).equals(switchPodNumber.get(switch2))) {
                    if (switch1.getLinks().get(0).getEnd().equals(switch2.getLinks().get(0).getEnd())) {
                        return 2;//same agg
                    } else {
                        return 4; //same pod
                    }
                } else {
                    return 6;
                }
            } else if (switchType.get(switch2) == AGG2) {
                if (switchPodNumber.get(switch1).equals(switchPodNumber.get(switch2))) {
                    if (switch1.getLinks().get(0).getEnd().equals(switch2)) {
                        return 1;//same agg
                    } else {
                        return 3; //same pod
                    }
                } else {
                    return 5;
                }
            } else if (switchType.get(switch2) == AGG1) {
                if (switchPodNumber.get(switch1).equals(switchPodNumber.get(switch2))) {
                    return 2;
                } else {
                    return 4;
                }
            } else {//core
                return 3;
            }
        } else if (switchType.get(switch1) == AGG2) {
            if (switchType.get(switch2) == AGG2) {
                if (switchPodNumber.get(switch1).equals(switchPodNumber.get(switch2))) {
                    return 2;
                } else {
                    return 4;
                }
            } else if (switchType.get(switch2) == AGG1) {
                if (switchPodNumber.get(switch1).equals(switchPodNumber.get(switch2))) {
                    return 1;
                } else {
                    return 3;
                }
            } else if (switchType.get(switch2) == CORE) {
                return 2;
            } else {
                return getPathLength(switch2, switch1);
            }
        } else if (switchType.get(switch1) == AGG1) {
            if (switchType.get(switch2) == AGG1) {
                return 2;
            } else if (switchType.get(switch2) == CORE) {
                return 1;
            } else {
                return getPathLength(switch2, switch1);
            }
        } else {//core
            if (switchType.get(switch2) == CORE) {
                return 2;
            } else {
                return getPathLength(switch2, switch1);
            }
        }
    }

    private void fillSwitchPodNumber() {
        digits = (int) Math.ceil(Math.log(getK()) / Math.log(10));
        for (Switch aSwitch : switches) {
            switchPodNumber.put(aSwitch, getIthNum(aSwitch.getId(), aSwitch.getId().indexOf("_"), 0));
            byte type = EDGE;
            if (aSwitch.getId().startsWith(AbstractFatTree.AGGREGATE1_STRING)) {
                type = AGG1;
            } else if (aSwitch.getId().startsWith(AbstractFatTree.AGGREGATE2_STRING)) {
                type = AGG2;
            } else if (aSwitch.getId().startsWith(AbstractFatTree.CORE_STRING)) {
                type = CORE;
            }
            switchType.put(aSwitch, type);
        }
    }


    public List<Link> getPath(Switch switch1, Switch switch2, Flow flow) {
        if (switch1.equals(switch2)) {
            return new ArrayList<Link>();
        }

        final int hash;
        if (flow == null) {
            hash = 0;
        } else {
            hash = Math.abs(flow.hashCode());
        }

        if (switchType.get(switch1) == EDGE) {
            List<Link> path = new ArrayList<Link>();
            path.add(switch1.getLinks().get(0));
            Switch srcAgg = path.get(0).getEnd();
            path.addAll(getPath(srcAgg, switch2, flow));
            return path;

        } else if (switchType.get(switch1) == AGG2) {
            Integer pod1Num = switchPodNumber.get(switch1);
            if (switchType.get(switch2) == AGG2) {
                List<Link> path = new ArrayList<Link>();
                if (pod1Num.equals(switchPodNumber.get(switch2))) {
                    //pick a random agg1 switch
                    if (switchIDSwitchMap.size() == 0) {
                        for (Switch aSwitch : switches) {
                            switchIDSwitchMap.put(aSwitch.getId(), aSwitch);
                        }
                    }
                    final Switch agg1 = switchIDSwitchMap.get(String.format(AbstractFatTree.AGGREGATE1_STRING + "_%0" + digits + "d_%0" + digits + "d",
                            pod1Num, (hash % (getK() / 2))));
                    path.add(getLinkTo(agg1, switch1.getLinks()));
                    path.add(getLinkTo(switch2, agg1.getLinks()));
                    return path;
                } else {
                    //pick a random core switch
                    Switch core = cores.get(hash % cores.size());
                    path.addAll(getPath(switch1, core, flow));
                    path.addAll(getPath(core, switch2, flow));
                    return path;
                }
            } else if (switchType.get(switch2) == AGG1) {
                List<Link> path = new ArrayList<Link>();
                if (pod1Num.equals(switchPodNumber.get(switch2))) {
                    path.add(getLinkTo(switch2, switch1.getLinks()));
                    return path;
                } else {
                    //find a core neighbor of agg1
                    int halfK = getK() / 2;
                    int coreIndex = hash % halfK;
                    int index = 0;
                    for (Link link : switch2.getLinks()) {
                        if (link.getEnd().getId().contains(AbstractFatTree.CORE_STRING)) {
                            if (index == coreIndex) {
                                path.addAll(getPath(switch1, link.getEnd(), flow));
                                path.add(link.getOtherSide());
                                return path;
                            }
                            index++;
                        }
                    }
                    throw new RuntimeException("core not found to agg switch " + switch2 + " with index " + coreIndex);
                }
            } else if (switchType.get(switch2) == CORE) {
                List<Link> path = new ArrayList<Link>();
                for (Link link1 : switch1.getLinks()) {
                    for (Link link2 : switch2.getLinks()) {
                        if (link1.getEnd().equals(link2.getEnd())) {
                            path.add(link1);
                            path.add(link2.getOtherSide());
                            return path;
                        }
                    }

                }
                throw new RuntimeException("Not supported routing");
            } else {
                List<Link> path = getPath(switch2, switch1, flow);
                for (int i = 0; i < path.size(); i++) {
                    path.set(i, path.get(i).getOtherSide());
                }
                Collections.reverse(path);
                return path;
            }
        } else if (switchType.get(switch1) == AGG1) {
            if (switchType.get(switch2) == AGG1) {
                throw new RuntimeException("Not supported routing");
            } else if (switchType.get(switch2) == CORE) {
                List<Link> path = new ArrayList<Link>();
                //check if the core is a neighbor
                for (Link link : switch1.getLinks()) {
                    if (link.getEnd().equals(switch2)) {
                        path.add(link);
                        return path;
                    }
                }
                throw new RuntimeException("Not supported routing");
            } else {
                List<Link> path = getPath(switch2, switch1, flow);
                for (int i = 0; i < path.size(); i++) {
                    path.set(i, path.get(i).getOtherSide());
                }
                Collections.reverse(path);
                return path;
            }
        } else {//core
            if (switchType.get(switch2) == CORE) {
                throw new RuntimeException("Not supported routing");
            } else {
                List<Link> path = getPath(switch2, switch1, flow);
                for (int i = 0; i < path.size(); i++) {
                    path.set(i, path.get(i).getOtherSide());
                }
                Collections.reverse(path);
                return path;
            }
        }
        //throw new RuntimeException("path not found from " + switch1 + " to" + switch2);
    }

    private int getK() {
        return 2 * (int) Math.sqrt(cores.size());
    }

    private Link getLinkTo(Switch agg1, List<Link> links) {
        for (Link link : links) {
            if (link.getEnd().equals(agg1)) {
                return link;
            }
        }
        return null;
    }


    @Override
    public void computeShortestPaths() {
        if (switchPodNumber.size() == 0) {
            fillSwitchPodNumber();
        }
    }

    @Override
    public JPanel draw(Dimension size) {
        return null;
    }

    @Override
    protected void getSwitchCategorizedStats(Statistics outputStatistics) {
        final List<Switch> edges = findEdges();
        //edge and internal memory usage

        List<Statistics> edgeStats = new LinkedList<Statistics>();
        List<Statistics> agg2Stats = new LinkedList<Statistics>();
        List<Statistics> agg1Stats = new LinkedList<Statistics>();
        List<Statistics> coreStats = new LinkedList<Statistics>();

        for (Switch aSwitch : switches) {
            if (switchType.get(aSwitch) == EDGE) {
                edgeStats.add(aSwitch.getStats());
            } else if (switchType.get(aSwitch) == AGG1) {
                agg1Stats.add(aSwitch.getStats());
            } else if (switchType.get(aSwitch) == AGG2) {
                agg2Stats.add(aSwitch.getStats());
            } else {
                coreStats.add(aSwitch.getStats());
            }

        }

        addToOutput(edgeStats, EDGE_STRING, outputStatistics);
        addToOutput(agg2Stats, AGGREGATE2_STRING, outputStatistics);
        addToOutput(agg1Stats, AGGREGATE1_STRING, outputStatistics);
        addToOutput(coreStats, CORE_STRING, outputStatistics);
    }

    private void addToOutput(List<Statistics> switchStats, String prefix, Statistics outputStatistics) {
        final Collection<String> statNames = switchStats.get(0).getStatNames();
        for (String statName : statNames) {
            final Double mean = Statistics.getMean(switchStats, statName);
            outputStatistics.addStat("Mean " + prefix + " " + statName, mean);
            outputStatistics.addStat("Max " + prefix + " " + statName, Statistics.getMax(switchStats, statName));
        }
    }

    @Override
    protected void getLinkStatistics(Statistics outputStatistics) {
        super.getLinkStatistics(outputStatistics);
        long sumEdge = 0;
        long sumAgg = 0;
        long sumCore = 0;
        for (Link link : links) {
            if (switchType.get(link.getStart()) == EDGE ||
                    switchType.get(link.getEnd()) == EDGE) {
                sumEdge += link.getUsedCapacity();
            } else if (switchType.get(link.getStart()) == CORE ||
                    switchType.get(link.getEnd()) == CORE) {
                sumCore += link.getUsedCapacity();
            } else {
                sumAgg += link.getUsedCapacity();
            }
        }

        outputStatistics.addStat(SUM_EDGE_TRAFFIC, sumEdge);
        outputStatistics.addStat(SUM_AGG_TRAFFIC, sumAgg);
        outputStatistics.addStat(SUM_CORE_TRAFFIC, sumCore);
    }
}
