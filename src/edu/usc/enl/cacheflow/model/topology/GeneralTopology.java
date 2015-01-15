package edu.usc.enl.cacheflow.model.topology;

import edu.uci.ics.jung.algorithms.layout.ISOMLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.util.EdgeType;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.*;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.ui.topology.PopupGraphMousePlugin;

import javax.swing.*;
import java.awt.*;
import java.awt.event.InputEvent;
import java.awt.event.MouseEvent;
import java.util.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/25/12
 * Time: 4:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class GeneralTopology extends Topology {


    private DirectedSparseGraph<Switch, Link> graph;
    private Map<Topology.SwitchPair, List<Link>> shortestPaths = new HashMap<Topology.SwitchPair, List<Link>>();

    @Override
    public List<List<Link>> getAllPath(SwitchPair pair) {
        return Collections.singletonList(shortestPaths.get(pair));
    }

    /**
     * computes the shortest path between all pair of nodes in the topology. Use getPath method to retrieve the result
     */
    public void computeShortestPaths() {
        Graph<Switch, Link> graph = getGraph();

        DijkstraShortestPath<Switch, Link> alg = new DijkstraShortestPath<Switch, Link>(graph);
        for (Switch switch1 : switches) {
            for (Switch switch2 : switches) {
                if (switch1.equals(switch2)) {
                    shortestPaths.put(new SwitchPair(switch1, switch2), new LinkedList<Link>());
                    continue;
                }
                List<Link> path = alg.getPath(switch1, switch2);
                SwitchPair switchPair = new SwitchPair(switch1, switch2);
                shortestPaths.put(switchPair, new LinkedList<Link>(path));
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

    public JPanel draw(Dimension size) {
        Graph<Switch, Link> graph = getGraph();

        Layout<Switch, Link> layout = new ISOMLayout<Switch, Link>(graph);
        //new DAGLayout<Switch, Link>(graph);
        //new CircleLayout<Switch, Link>(graph);
        layout.setSize(new Dimension(size.width - 30, size.height - 30));
        //BasicVisualizationServer<Switch, Link> vv = new BasicVisualizationServer<Switch, Link>(layout);
        VisualizationViewer<Switch, Link> vv = new VisualizationViewer<Switch, Link>(layout);

        //add mouse events using plugin
        PluggableGraphMouse gm = new PluggableGraphMouse();
        gm.add(new TranslatingGraphMousePlugin(MouseEvent.BUTTON1_MASK));
        gm.add(new ScalingGraphMousePlugin(new CrossoverScalingControl(), 0, 1.1f, 0.9f));
        gm.add(new PickingGraphMousePlugin(InputEvent.BUTTON1_MASK | InputEvent.SHIFT_MASK, InputEvent.BUTTON3_MASK | InputEvent.CTRL_MASK));
        gm.add(new PopupGraphMousePlugin());
        //gm.add(new ShearingGraphMousePlugin());
        vv.setGraphMouse(gm);

        vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller<Switch>() {
            @Override
            public String transform(Switch aSwitch) {
                return aSwitch.toString();// + " (" + aSwitch.getUsedCapacity() + "/" + aSwitch.getMemoryCapacity() + ")";
            }
        });
        vv.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller<Link>() {
            @Override
            public String transform(Link link) {
                return (int) link.getUsedCapacity() + "/" + (int) link.getCapacity();
            }
        });
        vv.setPreferredSize(size);
        return vv;
    }
}
