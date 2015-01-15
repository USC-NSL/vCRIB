package edu.usc.enl.cacheflow.processor.flow;

import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.CustomRandomGenerator;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/6/11
 * Time: 10:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class CustomRandomFlowDistribution {
    private CustomRandomGenerator<Integer> flowSizes;
    private CustomRandomGenerator<Integer> flowNumber;
    private CustomRandomGenerator<Integer> localizedFlowDistribution;
    private CustomRandomGenerator<Integer> newFlowDistribution;
    private CustomRandomGenerator<Integer> VMPerMachineDistribution;

    public CustomRandomFlowDistribution(List<String> input) {
        try {
            Map<Integer, Double> tempMap = new HashMap<Integer, Double>();
            for (String s : input.get(0).split("\\s+")) {
                StringTokenizer st = new StringTokenizer(s, ",");
                while (st.hasMoreTokens()) {
                    String tuple = st.nextToken();
                    final int commaIndex = tuple.indexOf(":");
                    tempMap.put(Integer.parseInt(tuple.substring(0, commaIndex)), Double.parseDouble(tuple.substring(commaIndex + 1)));
                }
                flowNumber = new CustomRandomGenerator<Integer>(tempMap);
            }

            tempMap.clear();
            for (String s : input.get(1).split("\\s+")) {
                StringTokenizer st = new StringTokenizer(s, ",");
                while (st.hasMoreTokens()) {
                    String tuple = st.nextToken();
                    final int commaIndex = tuple.indexOf(":");
                    tempMap.put(Integer.parseInt(tuple.substring(0, commaIndex)), Double.parseDouble(tuple.substring(commaIndex + 1)));
                }
                flowSizes = new CustomRandomGenerator<Integer>(tempMap);
            }

            tempMap.clear();
            for (String s : input.get(2).split("\\s+")) {
                StringTokenizer st = new StringTokenizer(s, ",");
                while (st.hasMoreTokens()) {
                    String tuple = st.nextToken();
                    final int commaIndex = tuple.indexOf(":");
                    tempMap.put(Integer.parseInt(tuple.substring(0, commaIndex)), Double.parseDouble(tuple.substring(commaIndex + 1)));
                }
                localizedFlowDistribution = new CustomRandomGenerator<Integer>(tempMap);
            }

            tempMap.clear();
            for (String s : input.get(3).split("\\s+")) {
                StringTokenizer st = new StringTokenizer(s, ",");
                while (st.hasMoreTokens()) {
                    String tuple = st.nextToken();
                    final int commaIndex = tuple.indexOf(":");
                    tempMap.put((int) Double.parseDouble(tuple.substring(0, commaIndex)), Double.parseDouble(tuple.substring(commaIndex + 1)));
                }
                newFlowDistribution = new CustomRandomGenerator<Integer>(tempMap);
            }
            tempMap.clear();
            for (String s : input.get(4).split("\\s+")) {
                StringTokenizer st = new StringTokenizer(s, ",");
                while (st.hasMoreTokens()) {
                    String tuple = st.nextToken();
                    final int commaIndex = tuple.indexOf(":");
                    tempMap.put((int) Double.parseDouble(tuple.substring(0, commaIndex)), Double.parseDouble(tuple.substring(commaIndex + 1)));
                }
                VMPerMachineDistribution = new CustomRandomGenerator<Integer>(tempMap);
            }
        } catch (CustomRandomGenerator.AllZeroWeightException e) {
            e.printStackTrace();
        }

    }

    public int getRandomFlowSize(double random) {
        return flowSizes.getRandom(random);
    }

    public int getRandomFlowNum(double random) {
        return flowNumber.getRandom(random);
    }

    public CustomRandomGenerator<Integer> getLocalizedFlowDistribution() {
        return localizedFlowDistribution;
    }

    public int getRandomNewFlowsNum(double random) {
        return newFlowDistribution.getRandom(random);
    }

    public int getNumVMsPerMachine(double random) {
        return VMPerMachineDistribution.getRandom(random);
    }

    public int getVMsPerSource(Random random, List<Switch> sources, Map<Switch, Integer> vmsPerSourceMap) {
        int vmsNum = 0;
        for (Switch edge : sources) {
            int v = getNumVMsPerMachine(random.nextDouble());
            vmsPerSourceMap.put(edge, v);
            vmsNum += v;
        }
        System.out.println(vmsNum);
        return vmsNum;
    }
}
