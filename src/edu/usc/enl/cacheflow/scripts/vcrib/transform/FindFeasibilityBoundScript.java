package edu.usc.enl.cacheflow.scripts.vcrib.transform;

import edu.usc.enl.cacheflow.processor.partition.transform.*;
import edu.usc.enl.cacheflow.scripts.stats.LoadDistributionStats;
import edu.usc.enl.cacheflow.util.CustomRandomGenerator;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 10/31/12
 * Time: 10:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class FindFeasibilityBoundScript extends MultipleTransformFeasibilityScript {

    public static void main(String[] args) throws Exception {
        new FindFeasibilityBoundScript().run(args);
    }

    public void run(String[] args) throws Exception {
        init(args);
        double trafficThreshold = 0;
        if (args.length > 9) {
            //this is traffic feasibilty bound
            trafficThreshold = Double.parseDouble(args[9]);
        }
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
        String partitionFileName = new File(partitionFile).getName();

        PartitionTransformer breakTransform = new BreakPartitionTransformer(true, true);
        PartitionTransformer extendTransform = new ExtendRulesPartitionTransformer(true,true,true);
        PartitionTransformer addSmallRules = new AddRemoveRandomRulesPartitionTransformer(
                new CustomRandomGenerator<Boolean>(new Boolean[]{Boolean.TRUE, Boolean.FALSE}, new double[]{1, 0}),
                new CustomRandomGenerator<Long>(new Long[]{1l}, new double[]{1}),
                new CustomRandomGenerator<Long>(new Long[]{1l}, new double[]{1l}), true,true);
        PartitionTransformer removeSmallRules = new AddRemoveRandomRulesPartitionTransformer(
                new CustomRandomGenerator<Boolean>(new Boolean[]{Boolean.TRUE, Boolean.FALSE}, new double[]{0, 1}),
                new CustomRandomGenerator<Long>(new Long[]{1l}, new double[]{1}),
                new CustomRandomGenerator<Long>(new Long[]{1l}, new double[]{1l}), true,true);

        int loadResolution = 128;
        int similarityResolution = 128;
        int tryNum = 0;

//        int resolution = similarityResolution;
//        int changeNum = resolution;
//        Exploration exploration =
//                new BreakExtendExploration(vmStartTopology, partitions, sourcePartitions,
//                        changeNum, breakTransform, extendTransform, Exploration.Action.NULL, false);

        int resolution = loadResolution;
        int changeNum = resolution;
        Exploration exploration =
                new AddRemoveExploration(vmStartTopology, partitions, sourcePartitions,
                        changeNum, breakTransform, addSmallRules, removeSmallRules, Exploration.Action.NULL, similarityResolution, false);
        while (changeNum >= resolution) {
            //save distribution of load on machines
            String currentOutputFolder = outputStatsFolder + "/" + tryNum;
            new File(currentOutputFolder).mkdirs();

            LoadDistributionStats.serverLoadDistribution(currentOutputFolder, vmStartTopology, partitions, sourcePartitions, partitionFileName);

            //save partitions
            String newPartitionFile = savePartitions(partitionsOutputFolder + "/" + tryNum, parameters, partitions, partitionFileName);

            //save partition stats
            savePartitionStats(tryNum, currentOutputFolder, newPartitionFile);

            // check feasibility
            simTopology.reset();
            //run feasibility test algorithm
            System.out.println(tryNum + " Finding feasibility");
            boolean feasible;
            if (trafficThreshold > 0) {
                feasible = trafficFeasibility(currentOutputFolder, trafficThreshold, newPartitionFile);
            } else {
                feasible = resourceFeasibility(partitionFileName, currentOutputFolder);
            }


            changeNum = exploration.explore(tryNum, feasible);
            tryNum++;
        }

    }


}
