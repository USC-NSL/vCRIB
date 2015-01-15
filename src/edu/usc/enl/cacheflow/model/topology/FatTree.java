package edu.usc.enl.cacheflow.model.topology;

import edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.util.EdgeType;
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
public class FatTree extends AbstractFatTree {
    private Set<Switch> cores = new HashSet<Switch>();
    private DirectedSparseGraph<Switch, Link> graph;
    private Map<SwitchPair, List<List<Link>>> shortestPaths = new HashMap<SwitchPair, List<List<Link>>>();


    @Override
    public List<List<Link>> getAllPath(SwitchPair pair) {
        return shortestPaths.get(pair);
    }

    public int getCoresSize() {
        return cores.size();
    }

    public void addCoreSwitch(Switch aSwitch) {
        cores.add(aSwitch);
        super.addSwitch(aSwitch);
    }

    public Set<Switch> getCores() {
        return cores;
    }

    @Override
    public void computeShortestPaths() {
        Graph<Switch, Link> graph = getGraph();

        DijkstraShortestPath<Switch, Link> alg = new DijkstraShortestPath<Switch, Link>(graph);
        Set<Switch> switchSet = new HashSet<Switch>(switches);
        switchSet.removeAll(cores);
        for (Switch core : cores) {
            for (Switch aSwitch : switchSet) {
                List<Link> path = alg.getPath(core, aSwitch);
                SwitchPair switchPair = new SwitchPair(core, aSwitch);
                List<List<Link>> pathes = new LinkedList<List<Link>>();
                pathes.add(path);
                shortestPaths.put(switchPair, pathes);

                path = alg.getPath(aSwitch, core);
                pathes = new LinkedList<List<Link>>();
                pathes.add(path);
                switchPair = new SwitchPair(aSwitch, core);
                shortestPaths.put(switchPair, pathes);
            }
        }

        //find noncore to noncore
        SwitchPair pair1 = new SwitchPair(null, null);
        for (Switch switch1 : switchSet) {
            for (Switch switch2 : switchSet) {
                if (switch1.equals(switch2)) {
                    shortestPaths.put(new SwitchPair(switch1, switch2), new LinkedList<List<Link>>());
                    continue;
                }
                //find switch1 to all core
                List<List<Link>> pathes = new LinkedList<List<Link>>();
                int pathLength = Integer.MAX_VALUE;
                for (Switch core : cores) {
                    pair1.first = switch1;
                    pair1.second = core;
                    final List<List<Link>> upPathes = shortestPaths.get(pair1);
                    pair1.first = core;
                    pair1.second = switch2;
                    final List<List<Link>> downPathes = shortestPaths.get(pair1);


                    final List<Link> upPath = new ArrayList<Link>(upPathes.iterator().next());
                    final List<Link> downPath = new ArrayList<Link>(downPathes.iterator().next());
                    final ListIterator<Link> upIterator = upPath.listIterator(upPath.size());
                    final ListIterator<Link> downIterator = downPath.listIterator();
                    while (upIterator.hasPrevious() && downIterator.hasNext()) {
                        if (upIterator.previous().getStart().equals(downIterator.next().getEnd())) {
                            upIterator.remove();
                            downIterator.remove();
                        }
                    }

                    List<Link> newPath = new ArrayList<Link>(upPath);
                    newPath.addAll(downPath);
                    if (pathLength > newPath.size()) {
                        pathes.clear();
                        pathes.add(newPath);
                        pathLength = newPath.size();
                    } else if (pathLength == newPath.size()) {
                        pathes.add(newPath);
                    }
                }

                //now that we have the shortest pathes, we need to remove duplicates
                HashSet<List<Link>> set = new HashSet<List<Link>>(pathes);

                shortestPaths.put(new SwitchPair(switch1, switch2), new ArrayList<List<Link>>(set));
            }
        }
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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
