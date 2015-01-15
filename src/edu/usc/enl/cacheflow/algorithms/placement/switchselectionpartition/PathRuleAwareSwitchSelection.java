package edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/31/11
 * Time: 9:11 AM
 * To change this template use File | Settings | File Templates.
 */
public class PathRuleAwareSwitchSelection extends PathAwareSwitchSelection {

    public PathRuleAwareSwitchSelection() {
        super(true);
    }

    @Override
    protected TreeMap<Switch, Double> separateSources(Set<Switch> sources, Map<Switch, Double> availableSwitchLabelMap, Partition partition) {
        final Set<Switch> availableSwitches = availableSwitchLabelMap.keySet();

        Map<Switch, Double> switchLabelMap = new HashMap<Switch, Double>();
        for (Map.Entry<Rule, Collection<Flow>> entry : ruleFlowMap.get(partition).entrySet()) {
            for (Flow flow : entry.getValue()) {
                Switch source = flow.getSource();
                if (entry.getKey().getAction().doAction(flow) == null &&
                        availableSwitches.contains(source)) {
                    Double label = switchLabelMap.get(source);
                    if (label == null) {
                        label = 0d;
                    }
                    label += flow.getTraffic();
                    switchLabelMap.put(source, label);
                }
            }
        }
        return super.separateSources(sources, switchLabelMap, partition);
    }

    @Override
    public String toString() {
        return "Path Rule Aware";
    }
}
