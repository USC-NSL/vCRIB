package edu.usc.enl.cacheflow.processor.network;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.model.rule.action.Action;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.model.rule.action.ForwardAction;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.OVSSwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classifier.OVSClassifier;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/12/11
 * Time: 9:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class RunFlowsOnNetworkProcessor2 {
    //map for tracking the traffic overhead
    private Map<Integer, Map<Integer, Long>> originalHopsNewHopsTraffic = new TreeMap<Integer, Map<Integer, Long>>();
    //-1 is deny, 0, 2,4,6 tooo 0, 1, 2, 3, 4,
    private Map<String, Map<String, Long>> newflowsMap = new TreeMap<String, Map<String, Long>>();
    //D, AL, AO tooo Source/Dest, Edge, Switch


    public Map<Integer, Map<Integer, Long>> getOriginalHopsNewHopsTraffic() {
        return originalHopsNewHopsTraffic;
    }

    public Map<String, Map<String, Long>> getNewflowsMap() {
        return newflowsMap;
    }

    public Action getAction(Flow flow, List<Rule> rules) {
        for (Rule aggregatedRule : rules) {
            if (aggregatedRule.match(flow)) {
                return aggregatedRule.getAction();
            }
        }

        return null;
    }

    public void process(Topology topology, Collection<Flow> flows) throws Switch.InfeasibleStateException {

        //sort switches rules
        Map<Switch, OVSClassifier> switchesRules = new HashMap<Switch, OVSClassifier>();
        final Set<Rule> finalRules = new HashSet<Rule>();
        for (Switch aSwitch : topology.getSwitches()) {
            finalRules.clear();
            aSwitch.getState().getRules(finalRules);
            switchesRules.put(aSwitch, new OVSClassifier(finalRules));
        }

        Map<String, Switch> switchMap = topology.getSwitchMap();
        switchMap.put(topology.getControllerSwitch().toString(), topology.getControllerSwitch());
        //reset links
        for (Link link : topology.getLinks()) {
            link.getFlows().clear();
            link.setUsedCapacity(0);
        }

        Map<Switch, Integer> newFlows = new HashMap<Switch, Integer>();//for ovs switches
        for (Switch aSwitch : topology.getSwitches()) {
            newFlows.put(aSwitch, 0);
        }

        //for each flow update the used capacity of links it traverse
        for (Flow flow : flows) {
            Switch currentSwitch = flow.getSource();//TODO need to support tunneling
            //currentSwitch.addFlow(flow);
            if (flow.isNew()) {
                newFlows.put(currentSwitch, newFlows.get(currentSwitch) + 1);
            }
//            if (currentSwitch.getId().equals("Edge_15_15_01")) {
//                System.out.println("src " + flow);
//            }

            while (true) {
                Action action = switchesRules.get(currentSwitch).classify(flow).getAction();
                if (action instanceof ForwardAction && (!currentSwitch.equals(flow.getSource()) ||
                        ((ForwardAction) action).getDestination().equals(currentSwitch.getId()))) {
                    throw new RuntimeException("double forwarding");
                }
                if (action.equals(DenyAction.getInstance())) {
                    updateStats(flow, true, topology, currentSwitch);
                    break;
                } else if (action.equals(AcceptAction.getInstance())) {
                    updateStats(flow, false, topology, currentSwitch);
                    Collection<Link> path = null;
                    path = topology.getPath(currentSwitch, flow.getDestination(), flow);
                    for (Link link : path) {
                        link.setUsedCapacity(link.getUsedCapacity() + flow.getTraffic());
                        //link.addFlow(flow);
                        /*if (link.getEnd().getId().equals("Edge_15_15_01")) {
                            System.out.println("dst " + flow);
                        }*/
                        if (flow.isNew()) {
                            newFlows.put(link.getEnd(), newFlows.get(link.getEnd()) + 1);
                        }
                    }
                    break;
                } else if (action instanceof ForwardAction) {
                    Switch destination = switchMap.get(((ForwardAction) action).getDestination());

                    if (destination.equals(topology.getControllerSwitch())) {
                        Util.logger.warning("Forward to controller " + flow + " on switch " + currentSwitch);
                        //find the partition of that flow
//                        for (Map.Entry<Partition, Map<Rule, List<Flow>>> partitionMapEntry : matched.entrySet()) {
//                            for (List<Flow> flowList : partitionMapEntry.getValue().values()) {
//                                if (flowList.contains(flow)){
//                                    System.out.println(partitionMapEntry.getKey());
//                                }
//                            }
//                        }

//                        Link currentCPULink = currentSwitch.getComputationCapacityLink();
//                        currentCPULink.setUsedCapacity(currentCPULink.getUsedCapacity() + flow.getTraffic());
//                        currentCPULink.addFlow(flow);
//                        Link controllerCPULink = topology.getControllerSwitch().getComputationCapacityLink();
//                        controllerCPULink.setUsedCapacity(controllerCPULink.getUsedCapacity() + flow.getTraffic());
//                        controllerCPULink.addFlow(flow);
                        //controller cannot forward decision it either accepts or denies
                        break;//TODO what about link from controller to switches
                    } else {
                        Collection<Link> path = topology.getPath(currentSwitch, destination, flow);
                        for (Link link : path) {
                            link.setUsedCapacity(link.getUsedCapacity() + flow.getTraffic());
                            //link.addFlow(flow);
                            /*if (link.getEnd().getId().equals("Edge_15_15_01")) {
                                System.out.println("host " + flow);
                            }*/
                            if (flow.isNew()) {
                                newFlows.put(link.getEnd(), newFlows.get(link.getEnd()) + 1);
                            }
                        }
                        currentSwitch = destination;
                    }
                }
            }
        }
        for (Map.Entry<Switch, Integer> entry : newFlows.entrySet()) {
            final Switch aSwitch = entry.getKey();
            if (aSwitch instanceof OVSSwitch && ((OVSSwitch.OVSState) aSwitch.getState()).getNewFlows() != entry.getValue()) {
                throw new RuntimeException(aSwitch + " new flows is not correct " +
                        ((OVSSwitch.OVSState) aSwitch.getState()).getNewFlows() + " != " + entry.getValue());
            }
        }
    }

    private void updateStats(Flow flow, boolean deny, Topology topology, Switch host) {
        int originalPathLength = topology.getPathLength(flow.getSource(), flow.getDestination());
        int newPathLength = topology.getPathLength(flow.getSource(), host);
        if (deny) {
            originalPathLength = -1;
        } else {
            newPathLength += topology.getPathLength(host, flow.getDestination());
        }
        Map<Integer, Long> map = originalHopsNewHopsTraffic.get(originalPathLength);
        if (map == null) {
            map = new TreeMap<Integer, Long>();
            originalHopsNewHopsTraffic.put(originalPathLength, map);
            for (int i = 0; i < 13; i++) {
                map.put(i, 0l);
            }
        }
        map.put(newPathLength, map.get(newPathLength) + flow.getTraffic());

        // for flows stats
        String category;
        if (deny) {
            category = "Deny";
        } else if (originalPathLength <= 0) {//local
            category = "Accept/Local";
        } else {
            category = "Accept/Other";
        }

        String newOutput;
        if (host.equals(flow.getSource())) {
            newOutput = "Source";
        } else if (host.equals(flow.getDestination())) {
            newOutput = "Destination";
        } else {
            newOutput = host.getId().substring(0, host.getId().indexOf("_"));
        }
        Map<String, Long> map2 = newflowsMap.get(category);
        if (map2 == null) {
            map2 = new TreeMap<String, Long>();
            if (newflowsMap.size() > 0) {
                final Map<String, Long> firstMap = newflowsMap.values().iterator().next();
                for (String key : firstMap.keySet()) {
                    map2.put(key, 0l);
                }
            }
            newflowsMap.put(category, map2);
        }
        Long value = map2.get(newOutput);

        if (value == null) {
            //put in all enteries
            for (Map<String, Long> hostNameValue : newflowsMap.values()) {
                hostNameValue.put(newOutput, 0l);
            }
            value = 0l;
        }
        map2.put(newOutput, value + flow.getTraffic());
    }

    public void print(PrintWriter p) {
        {
            Map<Integer, Map<Integer, Long>> map = getOriginalHopsNewHopsTraffic();

            for (Integer s : map.get(map.keySet().iterator().next()).keySet()) {
                p.print("," + s);
            }
            p.println();

            for (Map.Entry<Integer, Map<Integer, Long>> entry : map.entrySet()) {
                p.print(entry.getKey());
                for (Map.Entry<Integer, Long> entry2 : entry.getValue().entrySet()) {
                    p.print("," + entry2.getValue());
                }
                p.println();
            }
        }
        p.println("-------------------");
        {
            Map<Integer, Map<Integer, Long>> map = getOriginalHopsNewHopsTraffic();

            for (Integer s : map.get(map.keySet().iterator().next()).keySet()) {
                p.print("," + s);
            }
            p.println();

            for (Map.Entry<Integer, Map<Integer, Long>> entry : map.entrySet()) {
                Integer originalHops = entry.getKey();
                p.print(originalHops);
                for (Map.Entry<Integer, Long> entry2 : entry.getValue().entrySet()) {
                    Integer newHops = entry2.getKey();
                    if (newHops == 0) {
                        p.print("," + 0);
                    } else {
                        p.print("," + entry2.getValue() * (newHops - originalHops));
                    }
                }
                p.println();
            }
        }
        p.println("-------------------");
        {
            Map<String, Map<String, Long>> map = getNewflowsMap();
            for (String s : map.get(map.keySet().iterator().next()).keySet()) {
                p.print("," + s);
            }
            p.println();

            for (Map.Entry<String, Map<String, Long>> entry : map.entrySet()) {
                p.print(entry.getKey());
                for (Map.Entry<String, Long> entry2 : entry.getValue().entrySet()) {
                    p.print("," + entry2.getValue());
                }
                p.println();
            }
        }
        p.flush();
    }

}
