package edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 10/5/11
 * Time: 9:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class PathAwareSwitchSelection extends SwitchSelection {
    private boolean sourcePriority = false;
    protected Map<Partition,Map<Rule, Collection<Flow>>> ruleFlowMap;

    public PathAwareSwitchSelection(boolean sourcePriority) {
        this.sourcePriority = sourcePriority;
    }

    @Override
    public void init(Topology topology) {
        super.init(topology);
        this.ruleFlowMap = ruleFlowMap;

    }

    @Override
    public String toString() {
        return "Path Aware"+(sourcePriority?"(Source)":"");
    }

    @Override
    public <T extends List<Switch>> T sortSwitches(T toFill, Map<Partition, Switch> placement,
                                                   Partition partition) {
        Switch controllerWasThere = removeController(toFill);

        //label the graph
        Set<Switch> sources = new HashSet<Switch>();
        Map<Switch, Double> switchLabelMap = labelGraph(sources, partition);


        //keep/add only available switches
        Map<Switch, Double> availableSwitchLabelMap = new HashMap<Switch, Double>();
        for (Switch availableSwitch : toFill) {
            Double weight = switchLabelMap.get(availableSwitch);
            if (weight == null) {//is available but not any path
                weight = 0d;
            }
            availableSwitchLabelMap.put(availableSwitch, weight);
        }

        //sort maps based on values
        toFill.clear();
        if (sourcePriority) {
            //separate sources
            TreeMap<Switch, Double> sourcesSorted_map = separateSources(sources, availableSwitchLabelMap,partition);
            toFill.addAll(sourcesSorted_map.keySet());
        }

        ValueComparator bvc = new ValueComparator(availableSwitchLabelMap);
        TreeMap<Switch, Double> sorted_map = new TreeMap<Switch, Double>(bvc);
        sorted_map.putAll(availableSwitchLabelMap);
        toFill.addAll(sorted_map.keySet());

        //return controller
        if (controllerWasThere != null) {
            toFill.add(controllerWasThere);
        }

        return toFill;
    }

    protected TreeMap<Switch, Double> separateSources(Set<Switch> sources, Map<Switch, Double> availableSwitchLabelMap, Partition partition) {
        ValueComparator bvc = new ValueComparator(null);
        TreeMap<Switch, Double> sorted_map = new TreeMap<Switch, Double>(bvc);
        Map<Switch, Double> availableSourceSwitchLabelMap = new HashMap<Switch, Double>();
        for (Switch source : sources) {
            Double sourceLabel = availableSwitchLabelMap.get(source);
            if (sourceLabel != null) {
                //if source is available
                availableSourceSwitchLabelMap.put(source, availableSwitchLabelMap.get(source));
                availableSwitchLabelMap.remove(source);
            }
        }

        bvc.setBase(availableSourceSwitchLabelMap);

        sorted_map.putAll(availableSourceSwitchLabelMap);
        return sorted_map;
    }


    protected Map<Switch, Double> labelGraph(Set<Switch> sources, Partition partition) {
        Map<Switch, Double> switchLabelMap = new HashMap<Switch, Double>();
        for (Map.Entry<Rule, Collection<Flow>> entry : ruleFlowMap.get(partition).entrySet()) {
            for (Flow flow : entry.getValue()) {
                Switch source = flow.getSource();

                sources.add(source);
                Switch destination = flow.getDestination();
                Collection<Link> path = topology.getPath(source, destination,flow);
                if (path.size() == 0) {
                    Double label = switchLabelMap.get(source);
                    if (label == null) {
                        label = 0d;
                    }
                    label += 2 * flow.getTraffic();
                    switchLabelMap.put(source, label);
                } else {
                    {
                        Double sourceLabel = switchLabelMap.get(source);
                        if (sourceLabel == null) {
                            sourceLabel = 0d;
                        }
                        sourceLabel += flow.getTraffic();
                        switchLabelMap.put(source, sourceLabel);
                    }
                    for (Link link : path) {
                        Switch node = link.getEnd();
                        Double label = switchLabelMap.get(node);
                        if (label == null) {
                            label = 0d;
                        }
                        label += flow.getTraffic();
                        switchLabelMap.put(node, label);
                    }
                }
            }
        }
        return switchLabelMap;
    }
}
