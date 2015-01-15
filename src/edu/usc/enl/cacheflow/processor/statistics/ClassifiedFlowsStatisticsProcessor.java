package edu.usc.enl.cacheflow.processor.statistics;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/6/12
 * Time: 11:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class ClassifiedFlowsStatisticsProcessor extends StatisticsProcessor<Map<Partition, Map<Rule, Collection<Flow>>>> {
    public static final String PARTITION_WITH_FLOWS_STAT = "Partitions with Flow";
    public static final String RULES_WITH_FLOWS_STAT = "Rules with Flow";
    public static final String MACHINE_PAIR_STAT = "Average Machine Pair";
    private static final String ACCEPT_FLOWS_STAT = "Accept Flows";
    private static final String FLOWS_PER_PARTITION_STAT = "Flows per Partition";
    private static final String PARTITIONS_PER_SOURCE_STAT = "Partitions per Source";
    private static final String LOCALITY_PREFIX_STAT = "Locality_";
    private Topology topology;

    public ClassifiedFlowsStatisticsProcessor(Map<Partition, Map<Rule, Collection<Flow>>> input, Map<String, Object> parameters,
                                              Topology topology) {
        super(input, parameters);
        this.topology = topology;
    }

    @Override
    protected Statistics getStat(Map<Partition, Map<Rule, Collection<Flow>>> input) throws Exception {
        Set<Rule> rulesWithFlow = new HashSet<Rule>();
        Map<Partition, Set<SwitchTuple>> machinePairPerPartition = new HashMap<Partition, Set<SwitchTuple>>(input.size(), 1);
        Map<Partition, Integer> flowsPerPartition = new HashMap<Partition, Integer>(input.size(), 1);
        Map<Switch, Set<Partition>> switchPartitionMap = new HashMap<Switch, Set<Partition>>();
        Map<Integer, Integer> localityMap = new HashMap<Integer, Integer>();
        for (Partition partition : input.keySet()) {
            flowsPerPartition.put(partition, 0);
        }
        long sumAcceptFlows = 0;

        Map<String, Integer> edgeRackPodNumSum = new HashMap<String, Integer>();
        edgeRackPodNumSum.put("SEdge", 0);
        edgeRackPodNumSum.put("SRack", 0);
        edgeRackPodNumSum.put("SPod", 0);
        edgeRackPodNumSum.put("DEdge", 0);
        edgeRackPodNumSum.put("DRack", 0);
        edgeRackPodNumSum.put("DPod", 0);

        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry0 : input.entrySet()) {
            Map<Rule, Collection<Flow>> ruleListMap = entry0.getValue();
            final Partition partition = entry0.getKey();
            Map<String, Set<String>> edgeRackPodNum = new HashMap<String, Set<String>>();
            edgeRackPodNum.put("SEdge", new HashSet<String>());
            edgeRackPodNum.put("SRack", new HashSet<String>());
            edgeRackPodNum.put("SPod", new HashSet<String>());
            edgeRackPodNum.put("DEdge", new HashSet<String>());
            edgeRackPodNum.put("DRack", new HashSet<String>());
            edgeRackPodNum.put("DPod", new HashSet<String>());
            for (Map.Entry<Rule, Collection<Flow>> entry : ruleListMap.entrySet()) {
                final Collection<Flow> flows = entry.getValue();
                if (flows.size() > 0) {
                    final Rule rule = entry.getKey();
                    if (rule.getAction() instanceof AcceptAction) {
                        sumAcceptFlows += flows.size();
                    }
                    flowsPerPartition.put(partition, flowsPerPartition.get(partition) + flows.size());
                    rulesWithFlow.add(rule);
                    Set<SwitchTuple> switchTuples = machinePairPerPartition.get(partition);
                    if (switchTuples == null) {
                        switchTuples = new HashSet<SwitchTuple>();
                        machinePairPerPartition.put(partition, switchTuples);
                    }
                    for (Flow flow : flows) {
                        switchTuples.add(new SwitchTuple(flow.getSource(), flow.getDestination()));
                        Set<Partition> partitions = switchPartitionMap.get(flow.getSource());
                        if (partitions == null) {
                            partitions = new HashSet<Partition>();
                            switchPartitionMap.put(flow.getSource(), partitions);
                        }
                        partitions.add(partition);

                        edgeRackPodNum.get("SEdge").add(flow.getSource().getId());
                        Switch sAgg = flow.getSource().getLinks().get(0).getEnd();
                        edgeRackPodNum.get("SRack").add(sAgg.getId());
                        edgeRackPodNum.get("SPod").add(sAgg.getId().substring(0, sAgg.getId().lastIndexOf("_")));

                        edgeRackPodNum.get("DEdge").add(flow.getDestination().getId());
                        Switch dAgg = flow.getDestination().getLinks().get(0).getEnd();
                        edgeRackPodNum.get("DRack").add(dAgg.getId());
                        edgeRackPodNum.get("DPod").add(dAgg.getId().substring(0, dAgg.getId().lastIndexOf("_")));


                        if (topology != null) {
                            final int pathLength = topology.getPathLength(flow.getSource(), flow.getDestination());
                            Integer value = localityMap.get(pathLength);
                            if (value == null) {
                                localityMap.put(pathLength, 1);
                            } else {
                                localityMap.put(pathLength, value + 1);
                            }
                        }
                    }
                }
            }
            if (flowsPerPartition.get(partition) > 0) {
                for (Map.Entry<String, Integer> entry : edgeRackPodNumSum.entrySet()) {
                    entry.setValue(entry.getValue() + edgeRackPodNum.get(entry.getKey()).size());
                }
            }
        }

        Statistics statistics = new Statistics();
        {
            int sumPartitionsWithFlow = 0;
            {
                for (Integer integer : flowsPerPartition.values()) {
                    sumPartitionsWithFlow += integer > 0 ? 1 : 0;
                }
                statistics.addStat(PARTITION_WITH_FLOWS_STAT, sumPartitionsWithFlow);
            }
            statistics.addStat("Max " + FLOWS_PER_PARTITION_STAT, Statistics.getMax(flowsPerPartition.values()));
            final Double mean = Statistics.getMean(flowsPerPartition.values());
            statistics.addStat("Mean " + FLOWS_PER_PARTITION_STAT, mean);
            statistics.addStat("Var " + FLOWS_PER_PARTITION_STAT, Statistics.getVar(flowsPerPartition.values(), mean));

            for (Map.Entry<String, Integer> entry : edgeRackPodNumSum.entrySet()) {
                statistics.addStat("Mean " + entry.getKey() + " per partition", 1.0 * entry.getValue() / sumPartitionsWithFlow);
            }
        }

        statistics.addStat(RULES_WITH_FLOWS_STAT, rulesWithFlow.size());
        {
            List<Integer> machinePairPerPartitionList = new ArrayList<Integer>(machinePairPerPartition.size());
            for (Set<SwitchTuple> switchTuples : machinePairPerPartition.values()) {
                machinePairPerPartitionList.add(switchTuples.size());
            }
            final Double mean = Statistics.getMean(machinePairPerPartitionList);
            statistics.addStat("Mean " + MACHINE_PAIR_STAT, mean);
            statistics.addStat("Var " + MACHINE_PAIR_STAT, Statistics.getVar(machinePairPerPartitionList, mean));
        }
        {
            List<Integer> partitionPerSourceList = new ArrayList<Integer>(switchPartitionMap.size());
            for (Set<Partition> partitions : switchPartitionMap.values()) {
                partitionPerSourceList.add(partitions.size());
            }
            final Double mean = Statistics.getMean(partitionPerSourceList);
            statistics.addStat("Mean " + PARTITIONS_PER_SOURCE_STAT, mean);
            statistics.addStat("Var " + PARTITIONS_PER_SOURCE_STAT, Statistics.getVar(partitionPerSourceList, mean));
        }
        statistics.addStat(ACCEPT_FLOWS_STAT, sumAcceptFlows);
        for (Map.Entry<Integer, Integer> entry : localityMap.entrySet()) {
            statistics.addStat(LOCALITY_PREFIX_STAT + entry.getKey(), entry.getValue());
        }
        return statistics;
    }

    private class SwitchTuple {
        Switch s1;
        Switch s2;

        private SwitchTuple(Switch s1, Switch s2) {
            this.s1 = s1;
            this.s2 = s2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SwitchTuple that = (SwitchTuple) o;

            if (s1 != null ? !s1.equals(that.s1) : that.s1 != null) return false;
            if (s2 != null ? !s2.equals(that.s2) : that.s2 != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = s1 != null ? s1.hashCode() : 0;
            result = 31 * result + (s2 != null ? s2.hashCode() : 0);
            return result;
        }
    }
}
