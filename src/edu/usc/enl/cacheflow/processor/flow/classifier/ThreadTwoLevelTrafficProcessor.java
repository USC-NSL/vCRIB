package edu.usc.enl.cacheflow.processor.flow.classifier;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/16/12
 * Time: 6:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class ThreadTwoLevelTrafficProcessor implements PartitionClassifier {
    private RuleClassifier classifierPartition;
    private RuleClassifier classifierRule;
    private int threadNum;

    public ThreadTwoLevelTrafficProcessor(RuleClassifier classifierPartition,
                                          RuleClassifier classifierRule,
                                          int threadNum) {
        super();
        this.classifierPartition = classifierPartition;
        this.classifierRule = classifierRule;
        this.threadNum = threadNum;
    }

    //ALL INPUT RULES IDS MUST BE POSITIVE

    public Map<Partition, Map<Rule, Collection<Flow>>> classify(Collection<Flow> flows, Collection<Partition> partitions) {

        List<Rule> rules = new LinkedList<Rule>();
        //create forwarding rules
        Map<Rule, Partition> RulePartitionMap = new HashMap<Rule, Partition>(partitions.size(), 1);
        int j = -1;
        for (Partition partition : partitions) {
            final Rule forwardingRule = new Rule(DenyAction.getInstance(), partition.getProperties(), 0, j--);
            rules.add(forwardingRule);
            RulePartitionMap.put(forwardingRule, partition);
        }

        final Map<Rule, Collection<Flow>> partitionFlowMap = classifierPartition.classify(flows, rules);
        List<Rule> keys = new ArrayList<Rule>(partitionFlowMap.keySet());
        Map<Partition, Map<Rule, Collection<Flow>>> partitionTraffic = new ConcurrentHashMap<Partition, java.util.Map<Rule, Collection<Flow>>>(partitions.size(), 1);
        Map<Rule, Collection<Flow>> emptyMap = new HashMap<Rule, Collection<Flow>>();
        for (Partition partition : partitions) {
            partitionTraffic.put(partition, emptyMap);
        }

        List<Thread> threads = new ArrayList<Thread>(threadNum);
        int perThread = (int) (Math.ceil(1.0 * keys.size() / threadNum));
        for (int i = 0; i < threadNum; i++) {
            threads.add(new ProcessThread(i * perThread, Math.min((i + 1) * perThread, keys.size()), partitionFlowMap,
                    partitionTraffic, RulePartitionMap, classifierRule.cloneNew(), keys));
        }

        for (Thread thread : threads) {
            thread.start();
            System.out.println(thread + " started");
        }

        try {
            for (Thread thread : threads) {
                thread.join();
                System.out.println(thread + " join");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        partitionFlowMap.clear();
        classifierPartition = null;
        classifierRule = null;
        threads.clear();

        return partitionTraffic;
    }

    static class ProcessThread extends Thread {
        private final int start;
        private final int finish;
        private final Map<Rule, Collection<Flow>> partitionFlowMap;
        private final Map<Partition, Map<Rule, Collection<Flow>>> partitionTraffic;
        private final Map<Rule, Partition> rulePartitionMap;
        private final RuleClassifier classifierRule;
        private final List<Rule> keys;

        ProcessThread(int start, int finish, Map<Rule, Collection<Flow>> partitionFlowMap, Map<Partition, Map<Rule,
                Collection<Flow>>> partitionTraffic, Map<Rule, Partition> rulePartitionMap,
                      RuleClassifier classifierRule, List<Rule> keys) {
            this.start = start;
            this.finish = finish;
            this.partitionFlowMap = partitionFlowMap;
            this.partitionTraffic = partitionTraffic;
            this.rulePartitionMap = rulePartitionMap;
            this.classifierRule = classifierRule;
            this.keys = keys;
        }

        @Override
        public void run() {
            int i = 0;
            for (Rule entry : keys) {
                if (i >= start && i < finish) {
                    Partition partition = rulePartitionMap.get(entry);
                    final Map<Rule, Collection<Flow>> rulesFlowMap = classifierRule.classify(partitionFlowMap.get(entry), partition.getRules());
                    partitionTraffic.put(partition, rulesFlowMap);
                }
                i++;
            }
        }
    }

}
