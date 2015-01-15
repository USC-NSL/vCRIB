package edu.usc.enl.cacheflow.processor.rule.generator;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
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
public class TenantAccessControlRuleFlowGenerator {
    private List<Rule> rules;
    private List<Flow> flows;
    public long currentIP;
    private double skipToCompleteARange = 0;//skip even if there is available choices
    public RangeDimensionRange[] protocolChoices;
    public TreeMap<Double, RangeDimensionRange> probProtocol;


    public void generateRulesAndFlows(Random random, Topology topology,
                                      int minIPRangePerTenant, int maxIPRangePerTenant,
                                      int minIPRangeSize, int maxIPRangeSize, int tenantNum, double interTenantRuleAcceptProb,
                                      CustomRandomFlowDistribution flowDistribution, int flowPerMachine,
                                      int detailAcceptRuleNum, int detailDenyRuleNum, double detailRuleProb,
                                      double intraTenantTrafficProb, double interTenantAcceptProb,
                                      IPAssigner ipAssigner) {
        final RangeDimensionRange protRange = Util.PROTOCOL_INFO.getDimensionRange();
        protocolChoices = new RangeDimensionRange[]{
                protRange,
                new RangeDimensionRange(1, 1, protRange.getInfo()),
                new RangeDimensionRange(6, 6, protRange.getInfo()),
                new RangeDimensionRange(17, 17, protRange.getInfo())
        };

        List<Tenant> tenants = new ArrayList<Tenant>(tenantNum);
        rules = generateRules(random, tenants, minIPRangePerTenant, maxIPRangePerTenant, minIPRangeSize, maxIPRangeSize,
                tenantNum, interTenantRuleAcceptProb, detailAcceptRuleNum, detailDenyRuleNum, detailRuleProb);
        flows = generateFlows(tenants, topology, random, flowDistribution, flowPerMachine,
                intraTenantTrafficProb, interTenantAcceptProb, ipAssigner);
    }

    public List<Rule> getRules() {
        return rules;
    }

    public List<Flow> getFlows() {
        return flows;
    }

    private List<Rule> generateRules(Random random, List<Tenant> tenants, int minIPRangePerTenant, int maxIPRangePerTenant,
                                     int minIPRangeSize, int maxIPRangeSize, int tenantNum, double interTenantRuleAcceptProb,
                                     int detailAcceptRuleNum, int detailDenyRuleNum, double detailRuleProb) {
        generateTenantSizes(random, minIPRangePerTenant, maxIPRangePerTenant, minIPRangeSize, maxIPRangeSize, tenantNum, tenants);
        /*final Map<Long, LinkedList<Tenant>> sizeTenantMap = categorizeTenantsOnRangeSizes(tenants);
        shuffleLists(sizeTenantMap, random);
        largestFirst(random, sizeTenantMap);*/
        selectIPRangesEmptyFromStart(random, tenants);


        List<Rule> output = new LinkedList<Rule>();
        for (int i = 0, tenantListSize = tenants.size(); i < tenantListSize; i++) {
            Tenant tenant1 = tenants.get(i);
            for (int i1 = 0, tenantListSize1 = tenants.size(); i1 < tenantListSize1; i1++) {
                Tenant tenant2 = tenants.get(i1);
                final Action action;
                /*if (tenant1.equals(tenant2)) {
                    //accept
                    action = AcceptAction.getInstance();
                    //tenant1.detailTenant.add(tenant1);
                } else {*/
                //with prob x accept
                if (random.nextDouble() < interTenantRuleAcceptProb) {
                    action = AcceptAction.getInstance();
                } else {
                    action = DenyAction.getInstance();
                }
                //}
                if (action instanceof AcceptAction) {
                    for (RangeDimensionRange range1 : tenant1.ipranges) {
                        for (RangeDimensionRange range2 : tenant2.ipranges) {
                            List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(5);
                            properties.add(new RangeDimensionRange(range1.getStart(), range1.getEnd(), Util.SRC_IP_INFO));
                            properties.add(new RangeDimensionRange(range2.getStart(), range2.getEnd(), Util.DST_IP_INFO));
                            properties.add(Util.PROTOCOL_INFO.getDimensionRange());
                            properties.add(Util.SRC_PORT_INFO.getDimensionRange());
                            properties.add(Util.DST_PORT_INFO.getDimensionRange());
                            output.add(new Rule(action, properties, 1000,Rule.maxId+1));
                        }
                    }
                }
                if (random.nextDouble() < detailRuleProb) {
                    //create detail rules
                    // tenant1.detailTenant.add(tenant2);
                    final int ruleNum;
                    final Action oppositeAction;
                    if (action instanceof AcceptAction) {
                        ruleNum = detailAcceptRuleNum;
                        oppositeAction = DenyAction.getInstance();
                    } else {
                        ruleNum = detailDenyRuleNum;
                        oppositeAction = AcceptAction.getInstance();
                    }
                    Set<Rule> detailRules = new HashSet<Rule>();
                    for (int k = 0; k < ruleNum; k++) {
                        //select the range of tenant
                        RangeDimensionRange range1 = tenant1.ipranges.get(random.nextInt(tenant1.ipranges.size()));
                        RangeDimensionRange range2 = tenant2.ipranges.get(random.nextInt(tenant2.ipranges.size()));

                        //generate ip ranges
                        RangeDimensionRange srcIPRange = getRandomRange(random, range1, Util.SRC_IP_INFO);
                        RangeDimensionRange dstIPRange = getRandomRange(random, range2, Util.DST_IP_INFO);

                        //generate protocol and destport
                        final RangeDimensionRange protocol = protocolChoices[random.nextInt(protocolChoices.length)];
                        final RangeDimensionRange destPortRange = getDestPortRange(protocol, random);
                        //break the rule
                        final List<RangeDimensionRange> destPortRanges = RangeDimensionRange.breakRange(destPortRange);
                        //add rules
                        for (RangeDimensionRange dstPortRange : destPortRanges) {
                            List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(5);
                            properties.add(srcIPRange);
                            properties.add(dstIPRange);
                            properties.add(protocol);
                            properties.add(Util.SRC_PORT_INFO.getDimensionRange());
                            properties.add(dstPortRange);
                            detailRules.add(new Rule(oppositeAction, properties, 2000,Rule.maxId+1));
                        }
                    }
                    output.addAll(detailRules);
                } else {
//                    if (action instanceof DenyAction) {
//                        tenant1.denyTenants.add(tenant2);
//                    } else {
//                        tenant1.detailTenant.add(tenant1);
//                    }
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
            Rule defaultRule = new Rule(DenyAction.getInstance(), properties, output.size(),Rule.maxId+1);
            output.add(defaultRule);
        }


        //////////////////////////
        final double residual = 1d / output.size();
        Map<RangeDimensionRange, Double> protocolProb = new HashMap<RangeDimensionRange, Double>();
        for (Rule rule : output) {
            final int protocolIndex = Util.getDimensionInfoIndex(Util.PROTOCOL_INFO);
            final RangeDimensionRange property = rule.getProperty(protocolIndex);
            Double prob = protocolProb.get(property);
            if (prob == null) {
                protocolProb.put(property, residual);
            } else {
                protocolProb.put(property, prob + residual);
            }
        }
        probProtocol = new TreeMap<Double, RangeDimensionRange>();
        double sum = 0;
        RangeDimensionRange lastRange = null;
        for (Map.Entry<RangeDimensionRange, Double> entry : protocolProb.entrySet()) {
            sum += Math.min(1, Math.round(entry.getValue() * 1000) / 1000.0);
            probProtocol.put(sum, entry.getKey());
            lastRange = entry.getKey();
        }
        probProtocol.put(1d, lastRange);
        return output;
    }

    private RangeDimensionRange getRandomRange(Random random, RangeDimensionRange range1, DimensionInfo info) {
        long randomNumber = range1.getRandomNumber(random);
        final int wcBits = random.nextInt(RangeDimensionRange.binlog(range1.getSize()));
        randomNumber = (randomNumber >>> wcBits) << wcBits;
        return new RangeDimensionRange(randomNumber, randomNumber + (1l << wcBits) - 1, info);
    }

    private RangeDimensionRange getDestPortRange(RangeDimensionRange protocolRange, Random random) {
        final RangeDimensionRange completeRange = Util.DST_PORT_INFO.getDimensionRange();
        if (protocolRange.getStart() == 0 && protocolRange.getEnd() == 255) {
            return completeRange;
        }
        if (protocolRange.getStart() == 1 && protocolRange.getEnd() == 1) {
            return completeRange;
        }
        if (protocolRange.getStart() == 6 && protocolRange.getEnd() == 6) {
            final long start = completeRange.getRandomNumber(random);
            final long end = (long) (random.nextDouble() * (completeRange.getEnd() + 1 - start));
            return new RangeDimensionRange(start, end, Util.DST_PORT_INFO);
        }
        if (protocolRange.getStart() == 17 && protocolRange.getEnd() == 17) {
            final long start = completeRange.getRandomNumber(random);
            final long end = (long) (random.nextDouble() * (completeRange.getEnd() + 1 - start));
            return new RangeDimensionRange(start, end, Util.DST_PORT_INFO);
        }
        return completeRange;
    }


    private List<Flow> generateFlows(List<Tenant> tenants, Topology topology, Random random, CustomRandomFlowDistribution flowDistribution,
                                     int flowPerMachine,
                                     double intraTenantTrafficProb, double interTenantAcceptProb, IPAssigner ipAssigner) {
        final List<Switch> edges = topology.findEdges();
        Map<Long, Tenant> IPTenantMap = new HashMap<Long, Tenant>();
        Map<Long, Switch> reverseSwitchIPList = new HashMap<Long, Switch>();
        Map<Switch, List<Long>> switchIPList = assignIPRanges(tenants, random, flowDistribution, topology, edges, IPTenantMap, reverseSwitchIPList,
                ipAssigner);
        //now that have IPs per machine create traffic
        List<Flow> output = new ArrayList<Flow>(flowPerMachine * edges.size());
        for (Switch edge : edges) {
            Set<Flow> flowsForMachine = getFlowsFor(random, flowPerMachine, switchIPList, IPTenantMap,
                    reverseSwitchIPList, edge, intraTenantTrafficProb, interTenantAcceptProb, tenants);
            output.addAll(flowsForMachine);
        }
        return output;
    }

    private Set<Flow> getFlowsFor(Random random, int flowPerMachine, Map<Switch, List<Long>> switchIPList,
                                  Map<Long, Tenant> IPTenantMap, Map<Long, Switch> reverseSwitchIPList,
                                  Switch edge, double intraTenantTrafficProb, double interTenantAcceptProb,
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

            /*else if (d < interTenantAcceptProb + intraTenantTrafficProb) {
                //pick an ip from other tenants that have accept from me
                final List<Long> acceptIPs = tenant.getAcceptIPs();
                if (acceptIPs.size() == 0) {
                    throw new RuntimeException("No accept IP for tenant " + tenant);
                }
                destIP = acceptIPs.get(random.nextInt(acceptIPs.size()));
            } else {
                ///pick an ip from other tenants that have deny from me
                final List<Long> denyIPs = tenant.getDenyIPs();
                if (denyIPs.size() == 0) {
                    throw new RuntimeException("No deny IP for tenant " + tenant);
                }
                destIP = denyIPs.get(random.nextInt(denyIPs.size()));
            }*/

            Long[] properties = new Long[5];
            properties[0] = srcIP;
            properties[1] = destIP;
            properties[2] = probProtocol.ceilingEntry(random.nextDouble()).getValue().getRandomNumber(random);
            properties[3] = Util.SRC_PORT_INFO.getDimensionRange().getRandomNumber(random);
            properties[4] = Util.DST_PORT_INFO.getDimensionRange().getRandomNumber(random);

            flowsForMachine.add(new Flow(1, edge, reverseSwitchIPList.get(destIP), properties));
        }
        return flowsForMachine;
    }

    private Map<Switch, List<Long>> assignIPRanges(List<Tenant> tenants, Random random, CustomRandomFlowDistribution flowDistribution, Topology topology,
                                                   List<Switch> edges,
                                                   Map<Long, Tenant> IPTenantMap, Map<Long, Switch> reverseSwitchIPList, IPAssigner ipAssigner) {
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

    private void selectIPRangesBuffer(Random random, Map<Long, LinkedList<Tenant>> tenantIPRangeSizes) {

        //select tenant ipranges

        currentIP = 0;

        List<Long> allRangesSizes = new ArrayList<Long>(tenantIPRangeSizes.keySet());
        Collections.sort(allRangesSizes);
        List<Long> availableRanges = allRangesSizes;
        while (allRangesSizes.size() > 0 && currentIP < Util.SRC_IP_INFO.getDimensionRange().getEnd()) {
            final Long size = availableRanges.get(random.nextInt(availableRanges.size()));
            /*final Long size = allRangesSizes.get(random.nextInt(allRangesSizes.size()));
            if (!availableRanges.contains(size)) {
                final int bits = RangeDimensionRange.binlog(size) - 1;
                currentIP = currentIP >>> bits;
                currentIP = currentIP << bits;
                currentIP += size;
                //previousSize *= 2;
                availableRanges = updateAvailableRanges(random, allRangesSizes);
                continue;
            }*/
            final LinkedList<Tenant> tenants = tenantIPRangeSizes.get(size);
            final Tenant tenant = tenants.pop();
            tenant.addRange(new RangeDimensionRange(currentIP, currentIP + size - 1, Util.SRC_IP_INFO));
            if (tenants.size() == 0) {
                allRangesSizes.remove(Collections.binarySearch(allRangesSizes, size));
            }
            currentIP = currentIP + size;
            //what sizes are available for the current IP for the next selection?
            availableRanges = updateAvailableRanges(random, allRangesSizes);
            if (currentIP >= Util.SRC_IP_INFO.getDimensionRange().getEnd()) {
                System.out.println("Could not find match");
                throw new RuntimeException("Could not find ip ranges");
            }
        }

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

    private List<Tenant> selectIPRangesSkip(Random random, List<Tenant> tenantList,
                                            Map<Long, LinkedList<Tenant>> tenantIPRangeSizes
    ) {

        List<Tenant> tenantList2 = new ArrayList<Tenant>(tenantList);
        for (Tenant tenant : tenantList2) {
            Collections.shuffle(tenant.rangeSizes, random);
        }
        currentIP = 0;
        List<Long> allRangesSizes = new ArrayList<Long>(tenantIPRangeSizes.keySet());
        Collections.sort(allRangesSizes);
        List<Long> availableRanges = allRangesSizes;
        while (tenantList2.size() > 0 && currentIP < Util.SRC_IP_INFO.getDimensionRange().getEnd()) {
            final int tenantIndex = random.nextInt(tenantList2.size());
            Tenant tenant = tenantList2.get(tenantIndex);
            final Long size = tenant.rangeSizes.remove(tenant.rangeSizes.size() - 1);
            if (!availableRanges.contains(size)) {
                final int bits = RangeDimensionRange.binlog(size);
                currentIP = currentIP >>> bits;
                currentIP = currentIP << bits;
                currentIP += size;
                //previousSize *= 2;
                // availableRanges = updateAvailableRanges(random, allRangesSizes);
            }
            tenant.addRange(new RangeDimensionRange(currentIP, currentIP + size - 1, Util.SRC_IP_INFO));
            if (tenant.rangeSizes.size() == 0) {
                tenantList2.remove(tenantIndex);
            }
            currentIP = currentIP + size;
            //what sizes are available for the current IP for the next selection?
            availableRanges = updateAvailableRanges(random, allRangesSizes);
            if (currentIP >= Util.SRC_IP_INFO.getDimensionRange().getEnd()) {
                System.out.println("Could not find match");
                throw new RuntimeException("Could not find ip ranges");
            }
        }

        return tenantList;
    }

    private void largestFirst(Random random, Map<Long, LinkedList<Tenant>> tenantIPRangeSizes) {


        currentIP = 0;

        List<Long> allRangesSizes = new ArrayList<Long>(tenantIPRangeSizes.keySet());
        Collections.sort(allRangesSizes);
        Collections.reverse(allRangesSizes);
        List<Long> availableRanges = allRangesSizes;
        for (Long size : allRangesSizes) {
            for (Tenant tenant : tenantIPRangeSizes.get(size)) {
                if (!availableRanges.contains(size)) {
                    //go to next level
                    final int bits = RangeDimensionRange.binlog(size) - 1;
                    currentIP = currentIP >>> bits;
                    currentIP = currentIP << bits;
                    currentIP += size;
                    availableRanges = updateAvailableRanges(random, allRangesSizes);
                }
                tenant.addRange(new RangeDimensionRange(currentIP, currentIP + size - 1, Util.SRC_IP_INFO));
                currentIP = currentIP + size;
                //what sizes are available for the current IP for the next selection?
                availableRanges = updateAvailableRanges(random, allRangesSizes);
                if (currentIP >= Util.SRC_IP_INFO.getDimensionRange().getEnd()) {
                    System.out.println("Could not find match");
                    throw new RuntimeException("Could not find ip ranges");
                }
            }
        }

    }

    private void shuffleLists(Map<Long, ? extends List<Tenant>> lists, Random random) {
        for (List<Tenant> list : lists.values()) {
            Collections.shuffle(list, random);
        }
    }

    private Map<Long, LinkedList<Tenant>> categorizeTenantsOnRangeSizes(List<Tenant> tenants) {
        Map<Long, LinkedList<Tenant>> output = new HashMap<Long, LinkedList<Tenant>>();
        for (Tenant tenant : tenants) {
            for (Long rangeSize : tenant.rangeSizes) {
                LinkedList<Tenant> tenantsList = output.get(rangeSize);
                if (tenantsList == null) {
                    tenantsList = new LinkedList<Tenant>();
                    output.put(rangeSize, tenantsList);
                }
                tenantsList.add(tenant);
            }
        }
        return output;
    }

    private void generateTenantSizes(Random random, int minIPRangePerTenant,
                                     int maxIPRangePerTenant, int minIPRangeSize,
                                     int maxIPRangeSize, int tenantNum, List<Tenant> tenantList) {
        //select tenant ipranges
        for (int i = 0; i < tenantNum; i++) {
            int rangesNum = (int) (minIPRangePerTenant + (maxIPRangePerTenant - minIPRangePerTenant) * random.nextDouble());
            Tenant tenant = new Tenant(i + "");
            tenantList.add(tenant);
            for (int j = 0; j < rangesNum; j++) {
                long rangeSize = (long) (minIPRangeSize + (maxIPRangeSize - minIPRangeSize) * random.nextDouble());
                rangeSize = (long) Math.pow(2, rangeSize);
                tenant.rangeSizes.add(rangeSize);
            }
        }
    }

    private List<Long> updateAvailableRanges(Random random, List<Long> allRangesSizes) {
        List<Long> availableRanges;
        while (true) {
            final long maxAvailableSize = Long.lowestOneBit(currentIP);//it is always >0 as currentIP>0
            int index = Collections.binarySearch(allRangesSizes, maxAvailableSize);
            if (index < 0) {
                index = -(index + 1);//based on the definition of insertion point
            } else {
                index++;//to include this one too
            }
            availableRanges = allRangesSizes.subList(0, index);
            if (availableRanges.size() == 0 && allRangesSizes.size() > 0 || random.nextDouble() < skipToCompleteARange) {
                currentIP += maxAvailableSize;
            } else {
                break;
            }
        }
        return availableRanges;
    }

    private class Tenant {
        List<RangeDimensionRange> ipranges = new ArrayList<RangeDimensionRange>();
        String id;
        List<Long> ips = new ArrayList<Long>();
        List<Tenant> denyTenants = new LinkedList<Tenant>();
        List<Tenant> detailTenant = new LinkedList<Tenant>();
        private List<Long> acceptIPs;
        private List<Long> denyIPs;
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

        public List<Long> getAcceptIPs() {
            if (acceptIPs == null) {
                acceptIPs = new LinkedList<Long>();
                for (Tenant acceptTenant : detailTenant) {
                    acceptIPs.addAll(acceptTenant.ips);
                }
                return acceptIPs;
            } else {
                return acceptIPs;
            }
        }

        public List<Long> getDenyIPs() {
            if (denyIPs == null) {
                denyIPs = new LinkedList<Long>();
                for (Tenant denyTenant : denyTenants) {
                    denyIPs.addAll(denyTenant.ips);
                }
                return denyIPs;
            } else {
                return denyIPs;
            }
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

    }
}
