package edu.usc.enl.cacheflow.processor.rule.generator;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.model.rule.action.Action;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.IPAssigner;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/15/12
 * Time: 9:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class OnlyTenantAccessControlRuleFlowGenerator {
    private List<Rule> rules;
    private List<Flow> flows;
    private long currentIP;

    public List<Tenant> tenants;

    public void generateRulesAndFlows(Random random,
                                      int minIPRangePerTenant, int maxIPRangePerTenant,
                                      int minIPRangeSize, int maxIPRangeSize, int tenantNum, double acceptProb,
                                      CustomRandomFlowDistribution flowDistribution, int flowPerMachine,
                                      double intraTenantTrafficProb, IPAssigner ipAssigner, Topology topology) {
        rules = generateRules(random, minIPRangePerTenant, maxIPRangePerTenant, minIPRangeSize, maxIPRangeSize,
                tenantNum, acceptProb);
        flows = generateFlows(random, flowDistribution, flowPerMachine,
                intraTenantTrafficProb, ipAssigner, topology);
    }

    public List<Rule> getRules() {
        return rules;
    }

    public List<Flow> getFlows() {
        return flows;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public List<Rule> generateRules(Random random, int minIPRangePerTenant, int maxIPRangePerTenant,
                                    int minIPRangeSize, int maxIPRangeSize, int tenantNum, double acceptProb) {
        tenants = new ArrayList<Tenant>(tenantNum);
        generateTenantSizes(random, minIPRangePerTenant, maxIPRangePerTenant, minIPRangeSize, maxIPRangeSize, tenantNum);
        /*final Map<Long, LinkedList<Tenant>> sizeTenantMap = categorizeTenantsOnRangeSizes(tenants);
        shuffleLists(sizeTenantMap, random);
        largestFirst(random, sizeTenantMap);*/
        selectIPRangesEmptyFromStart(random, tenants);

        List<Rule> output = new LinkedList<Rule>();
        for (int i = 0, tenantListSize = tenants.size(); i < tenantListSize; i++) {
            Tenant tenant1 = tenants.get(i);
            for (Tenant tenant2 : tenants) {
                final Action action;
                //with prob x accept
                if (random.nextDouble() < acceptProb) {
                    action = AcceptAction.getInstance();
                } else {
                    action = DenyAction.getInstance();
                }
                for (RangeDimensionRange range1 : tenant1.ipranges) {
                    for (RangeDimensionRange range2 : tenant2.ipranges) {
                        List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(5);
                        properties.add(new RangeDimensionRange(range1.getStart(), range1.getEnd(), Util.SRC_IP_INFO));
                        properties.add(new RangeDimensionRange(range2.getStart(), range2.getEnd(), Util.DST_IP_INFO));
                        properties.add(Util.PROTOCOL_INFO.getDimensionRange());
                        properties.add(Util.SRC_PORT_INFO.getDimensionRange());
                        properties.add(Util.DST_PORT_INFO.getDimensionRange());
                        output.add(new Rule(action, properties, 1000, Rule.maxId + 1));
                    }
                }
            }
        }

        //generate default rule
        final long maxIP = (long) Math.pow(2, Math.ceil(Math.log(currentIP) / Math.log(2))) - 1;
        Util.SRC_IP_INFO.getDimensionRange().setEnd(maxIP);
        Util.DST_IP_INFO.getDimensionRange().setEnd(maxIP);
        {
            List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(5);
            properties.add(Util.SRC_IP_INFO.getDimensionRange());
            properties.add(Util.DST_IP_INFO.getDimensionRange());
            properties.add(Util.PROTOCOL_INFO.getDimensionRange());
            properties.add(Util.SRC_PORT_INFO.getDimensionRange());
            properties.add(Util.DST_PORT_INFO.getDimensionRange());
            Rule defaultRule = new Rule(DenyAction.getInstance(), properties, output.size(), Rule.maxId + 1);
            output.add(defaultRule);
        }

        return output;
    }


    public List<Flow> generateFlows(Random random, CustomRandomFlowDistribution flowDistribution,
                                    int flowPerMachine, double intraTenantTrafficProb, IPAssigner ipAssigner, Topology topology) {
        final List<Switch> edges = topology.findEdges();
        Map<Long, Tenant> IPTenantMap = new HashMap<Long, Tenant>();
        Map<Long, Switch> reverseSwitchIPList = new HashMap<Long, Switch>();
        for (Tenant tenant : tenants) {
            tenant.resetIPs();
        }
        Map<Switch, List<Long>> switchIPList = assignIPRanges(tenants, random, edges, flowDistribution, IPTenantMap,
                reverseSwitchIPList, ipAssigner, topology);
        //now that have IPs per machine create traffic
        List<Flow> output = new ArrayList<Flow>(flowPerMachine * edges.size());
        for (Switch edge : edges) {
            Set<Flow> flowsForMachine = getFlowsFor(random, flowPerMachine, switchIPList, IPTenantMap,
                    reverseSwitchIPList, edge, intraTenantTrafficProb, tenants);
            output.addAll(flowsForMachine);
        }
        return output;
    }

    private Set<Flow> getFlowsFor(Random random, int flowPerMachine, Map<Switch, List<Long>> switchIPList,
                                  Map<Long, Tenant> IPTenantMap, Map<Long, Switch> reverseSwitchIPList,
                                  Switch edge, double intraTenantTrafficProb,
                                  List<Tenant> tenants) {
        Set<Flow> flowsForMachine = new HashSet<Flow>(flowPerMachine);
        final List<Long> ips = switchIPList.get(edge);
        Collections.shuffle(ips, random);
        int ipIndex = 0;
        while (flowsForMachine.size() < flowPerMachine) {
            //pick a random source IP
            final Long srcIP = ips.get(ipIndex);
            ipIndex = (ipIndex + 1) % ips.size();
            //pick a destination IP based on inter/intra tenant probability
            double d = random.nextDouble();
            Tenant srcTenant = IPTenantMap.get(srcIP);
            long destIP;
            if (d < intraTenantTrafficProb) {
                //pick an ip from this tenant
                destIP = srcTenant.ips.get(random.nextInt(srcTenant.ips.size()));
            } else {
                //select a random tenant
                //select a random IP from that tenant
                Tenant destTenant = null;
                do {
                    destTenant = tenants.get(random.nextInt(tenants.size()));
                } while (destTenant.equals(srcTenant));
                destIP = destTenant.ips.get(random.nextInt(destTenant.ips.size()));
            }

            Long[] properties = new Long[5];
            properties[0] = srcIP;
            properties[1] = destIP;
            properties[2] = Util.PROTOCOL_INFO.getDimensionRange().getRandomNumber(random);
            properties[3] = Util.SRC_PORT_INFO.getDimensionRange().getRandomNumber(random);
            properties[4] = Util.DST_PORT_INFO.getDimensionRange().getRandomNumber(random);

            flowsForMachine.add(new Flow(1, edge, reverseSwitchIPList.get(destIP), properties));
        }
        return flowsForMachine;
    }

    private Map<Switch, List<Long>> assignIPRanges(List<Tenant> tenants, Random random,
                                                   List<Switch> edges, CustomRandomFlowDistribution flowDistribution,
                                                   Map<Long, Tenant> IPTenantMap, Map<Long, Switch> reverseSwitchIPList,
                                                   IPAssigner ipAssigner, Topology topology) {
        //need
        Map<Switch, Integer> vmsPerSourceMap = new HashMap<>();
        int needIP = flowDistribution.getVMsPerSource(random, edges, vmsPerSourceMap);
        Set<Long> ips = new HashSet<Long>(needIP);
        Collections.shuffle(tenants, random);
        int tenantIndex = 0;
        while (ips.size() < needIP) {
            //select a random tenant
            Tenant tenant = tenants.get(tenantIndex);
            tenantIndex = (tenantIndex + 1) % tenants.size();
            final long randomIP = tenant.getRandomIP(random);
            int ipsSizeBefore = ips.size();
            ips.add(randomIP);
            if (ips.size() > ipsSizeBefore) {
                IPTenantMap.put(randomIP, tenant);
                tenant.addIP(randomIP);
            }
        }

        Map<Switch, List<Long>> switchListMap = ipAssigner.assignIPs(random, edges, ips, topology, vmsPerSourceMap);
        for (Map.Entry<Switch, List<Long>> entry : switchListMap.entrySet()) {
            for (Long ip : entry.getValue()) {
                reverseSwitchIPList.put(ip, entry.getKey());
            }
        }

        return switchListMap;

    }

    private List<List<Long>> blockify(List<Long> ipsSorted, int numberOfBlocks) {
        final int IPperBlock = (int) Math.floor(1.0 * ipsSorted.size() / numberOfBlocks);
        List<List<Long>> blocks = new LinkedList<List<Long>>();
        for (int i1 = 0; i1 < numberOfBlocks; i1++) {
            //find IPs that are in the block
            List<Long> ipsInBlock = ipsSorted.subList(IPperBlock * i1, Math.min(ipsSorted.size(), IPperBlock * (i1 + 1)));
            //Collections.shuffle(ipsInBlock, random);
            blocks.add(ipsInBlock);
        }
        return blocks;
    }


    private void selectIPRangesEmptyFromStart(Random random, List<Tenant> tenantList) {

        List<Tenant> tenantList2 = new ArrayList<Tenant>(tenantList);
        for (Tenant tenant : tenantList2) {
            Collections.shuffle(tenant.rangeSizes, random);
        }
        currentIP = 0;
        List<Long> sortedAssignedRanges = new ArrayList<Long>(tenantList.size());
        while (tenantList2.size() > 0 && currentIP < Util.SRC_IP_INFO.getDimensionRange().getEnd()) {
            final int tenantIndex = random.nextInt(tenantList2.size());
            Tenant tenant = tenantList2.get(tenantIndex);
            final Long size = tenant.rangeSizes.remove(tenant.rangeSizes.size() - 1);
            //find a good current IP
            long newCurrentIP = 0;
            boolean good = false;
            int index = 0;
            while (!good) {
                index = Collections.binarySearch(sortedAssignedRanges, newCurrentIP);
                if (index >= 0) {
                    newCurrentIP = Math.max(newCurrentIP + size, sortedAssignedRanges.get(index + 1) + 1);
                } else {
                    // not there but can be in the middle of a range
                    index = -(index + 1);
                    if (index >= sortedAssignedRanges.size()) {
                        //good
                        good = true;
                        currentIP = newCurrentIP + size;
                    } else {
                        if (index % 2 == 1) {
                            //in the middle
                            newCurrentIP += size;
                        } else {
                            //check end;
                            int endIndex = Collections.binarySearch(sortedAssignedRanges, newCurrentIP + size);
                            if (endIndex < 0) {
                                endIndex = -(endIndex + 1);
                            }
                            if (endIndex == index) {
                                //good
                                good = true;
                            }
                        }
                    }
                }
            }

            sortedAssignedRanges.add(index, newCurrentIP);
            sortedAssignedRanges.add(index + 1, newCurrentIP + size - 1);
            tenant.addRange(new RangeDimensionRange(newCurrentIP, newCurrentIP + size - 1, Util.SRC_IP_INFO));
            if (tenant.rangeSizes.size() == 0) {
                tenantList2.remove(tenantIndex);
            }

            if (currentIP >= Util.SRC_IP_INFO.getDimensionRange().getEnd()) {
                System.out.println("Could not find match");
                throw new RuntimeException("Could not find ip ranges");
            }
        }
    }

    private void generateTenantSizes(Random random, int minIPRangePerTenant,
                                     int maxIPRangePerTenant, int minIPRangeSize,
                                     int maxIPRangeSize, int tenantNum) {
        //select tenant ipranges
        for (int i = 0; i < tenantNum; i++) {
            int rangesNum = (int) (minIPRangePerTenant + (maxIPRangePerTenant - minIPRangePerTenant) * random.nextDouble());
            Tenant tenant = new Tenant(i + "");
            tenants.add(tenant);
            for (int j = 0; j < rangesNum; j++) {
                long rangeSize = (long) (minIPRangeSize + (maxIPRangeSize - minIPRangeSize) * random.nextDouble());
                rangeSize = (long) Math.pow(2, rangeSize);
                tenant.rangeSizes.add(rangeSize);
            }
        }
    }


    private class Tenant {
        List<RangeDimensionRange> ipranges = new ArrayList<RangeDimensionRange>();
        String id;
        List<Long> ips = new ArrayList<Long>();
        List<Long> rangeSizes = new ArrayList<Long>();

        public void addRange(RangeDimensionRange range) {
            ipranges.add(range);
        }

        @Override
        public String toString() {
            return id;
        }

        private Tenant(String id) {
            this.id = id;
        }

        public void addIP(long ip) {
            ips.add(ip);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tenant tenant = (Tenant) o;

            if (id != null ? !id.equals(tenant.id) : tenant.id != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }

        public long getRandomIP(Random random) {
            final RangeDimensionRange rangeDimensionRange = ipranges.get(random.nextInt(ipranges.size()));
            return rangeDimensionRange.getRandomNumber(random);
        }

        public void resetIPs() {
            ips.clear();
        }
    }
}
