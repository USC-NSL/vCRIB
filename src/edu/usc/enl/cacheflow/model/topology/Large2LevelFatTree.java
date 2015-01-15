package edu.usc.enl.cacheflow.model.topology;

import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.util.EdgeType;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/25/12
 * Time: 4:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class Large2LevelFatTree extends AbstractFatTree {
    private List<Switch> cores = new ArrayList<Switch>();
    private DirectedSparseGraph<Switch, Link> graph;


    @Override
    public List<List<Link>> getAllPath(SwitchPair pair) {
        final ArrayList<List<Link>> output = new ArrayList<List<Link>>();

        return output;
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

    @Override
    public int getPathLength(Switch switch1, Switch switch2) {
        if (switch1.equals(switch2)) {
            return 0;
        }
        if (switch1.getLinks().size() == 1) {
            if (switch2.getLinks().size() == 1) {
                if (switch1.getLinks().get(0).getEnd().equals(switch2.getLinks().get(0).getEnd())) {
                    return 2;
                } else {
                    return 4;
                }
            } else if (cores.contains(switch2)) {
                return 2;
            } else {//aggregate
                if (switch2.equals(switch1.getLinks().get(0).getEnd())) {
                    return 1;
                } else {
                    return 3;
                }
            }
        } else if (cores.contains(switch1)) {
            if (switch2.getLinks().size() == 1) {
                return 2;
            } else if (cores.contains(switch2)) {
                return 2;
            } else {
                return 1;
            }
        } else {//aggregate
            if (switch2.getLinks().size() == 1) {
                return getPathLength(switch2, switch1);
            } else if (cores.contains(switch2)) {
                return getPathLength(switch2, switch1);
            } else {
                return 2;
            }
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
        if (switch1.getLinks().size() == 1) {//its a host
            if (switch2.getLinks().size() == 1) {//its a host
                if (switch1.getLinks().get(0).getEnd().equals(switch2.getLinks().get(0).getEnd())) {//same agg
                    List<Link> path = new ArrayList<Link>();
                    path.add(switch1.getLinks().get(0));
                    path.add(switch2.getLinks().get(0).getOtherSide());
                    return path;
                } else {

                    final Switch core = cores.get(hash % cores.size());
                    List<Link> path = new ArrayList<Link>();
                    edgeToCore(switch1, core, path, false);
                    edgeToCore(switch2, core, path, true);
                    return path;
                }
            } else if (cores.contains(switch2)) {
                List<Link> path = new ArrayList<Link>();
                edgeToCore(switch1, switch2, path, false);
                return path;
            } else {//aggregate
                if (switch2.equals(switch1.getLinks().get(0).getEnd())) {
                    List<Link> path = new ArrayList<Link>();
                    path.add(switch1.getLinks().get(0));
                    return path;
                } else {
                    final Switch core = cores.get(hash % cores.size());
                    List<Link> path = new ArrayList<Link>();
                    edgeToCore(switch1, core, path, false);
                    for (Link link : core.getLinks()) {
                        if (link.getEnd().equals(switch2)) {
                            path.add(link);
                            break;
                        }
                    }
                    return path;
                }
            }
        } else if (cores.contains(switch1)) {
            if (switch2.getLinks().size() == 1) {
                final List<Link> path = getPath(switch2, switch1, flow);
                for (int i = 0; i < path.size(); i++) {
                    path.set(i, path.get(i).getOtherSide());
                }
                Collections.reverse(path);
                return path;
            } else if (cores.contains(switch2)) {
                List<Link> path = new ArrayList<Link>();
                final Link aggLink = switch1.getLinks().get(hash % switch1.getLinks().size());
                path.add(aggLink);
                for (Link link : aggLink.getEnd().getLinks()) {
                    if (link.getEnd().equals(switch2)) {
                        path.add(link);
                        return path;
                    }
                }
            } else {
                for (Link link : switch1.getLinks()) {
                    if (link.getEnd().equals(switch2)) {
                        List<Link> path = new ArrayList<Link>();
                        path.add(link);
                        return path;
                    }
                }
            }
        } else {//aggregate
            if (switch2.getLinks().size() == 1) {
                final List<Link> path = getPath(switch2, switch1, flow);
                for (int i = 0; i < path.size(); i++) {
                    path.set(i, path.get(i).getOtherSide());
                }
                Collections.reverse(path);
                return path;
            } else if (cores.contains(switch2)) {
                final List<Link> path = getPath(switch2, switch1, flow);
                for (int i = 0; i < path.size(); i++) {
                    path.set(i, path.get(i).getOtherSide());
                }
                Collections.reverse(path);
                return path;
            } else {
                final Switch core = cores.get(hash % cores.size());
                List<Link> path = new ArrayList<Link>();
                for (Link link : switch1.getLinks()) {
                    if (link.getEnd().equals(core)) {
                        path.add(link);
                        break;
                    }
                }
                for (Link link : core.getLinks()) {
                    if (link.getEnd().equals(switch2)) {
                        path.add(link);
                    }
                }
                return path;
            }
        }

        throw new RuntimeException("path not found form " + switch1 + " to " + switch2);
    }

    private void edgeToCore(Switch switch1, Switch switch2, List<Link> output, boolean reverse) {
        final Link aggLink = switch1.getLinks().get(0);


        if (reverse) {
            final List<Link> links1 = switch2.getLinks();
            for (Link link : links1) {
                if (link.getEnd().equals(aggLink.getEnd())) {
                    output.add(link);
                    break;
                }
            }
            output.add(aggLink.getOtherSide());
        } else {
            output.add(aggLink);
            final List<Link> links1 = switch2.getLinks();
            for (Link link : links1) {
                if (link.getEnd().equals(aggLink.getEnd())) {
                    output.add(link.getOtherSide());
                    return;
                }
            }
        }
    }

    @Override
    public void computeShortestPaths() {

    }

    private Graph<Switch, Link> getGraph() {
        if (graph == null) {
            createGraph();
        }
        return graph;
    }

    private void createGraph() {
        graph = new DirectedSparseGraph<Switch, Link>();
        for (Switch aSwitch : switches) {
            graph.addVertex(aSwitch);
        }
        for (Switch aSwitch : switches) {
            for (Link link : aSwitch.getLinks()) {
                graph.addEdge(link, link.getStart(), link.getEnd(), EdgeType.DIRECTED);
            }
        }
    }

    @Override
    public JPanel draw(Dimension size) {
        return null;
    }
}
