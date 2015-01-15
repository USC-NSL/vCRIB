package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/1/12
 * Time: 7:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class NormalVMIPSelector extends VMIPSelector {
    private final double std;
    private final double mean;

    public NormalVMIPSelector(double std, double mean) {
        this.std = std;
        this.mean = mean;
    }

    @Override
    public Collection<Long> getIps(Collection<Rule> rules, Random random, int numberOfIPs) {
        Set<Long> output = new HashSet<Long>();
        while (output.size() < numberOfIPs) {
            //output.add(Util.SRC_IP_INFO.getDimensionRange().getRandomNumber(random));

            RangeDimensionRange dimensionRange = Util.SRC_IP_INFO.getDimensionRange();
            double randomNum;
            do {
                randomNum = random.nextGaussian() * std + mean;
            } while (randomNum < 0 || randomNum >= 1);
            output.add(dimensionRange.getStart() + (long) (dimensionRange.getSize() * randomNum));

            //output.add(dimensionRange.getRandomNumber(random));
        }
        return output;
    }

    @Override
    public String toString() {
        return "Random IP";
    }
}
