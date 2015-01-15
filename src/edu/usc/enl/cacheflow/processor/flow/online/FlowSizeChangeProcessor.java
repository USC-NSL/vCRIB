package edu.usc.enl.cacheflow.processor.flow.online;

import edu.usc.enl.cacheflow.model.Flow;

import java.util.List;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/27/12
 * Time: 3:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class FlowSizeChangeProcessor {
    private double changeRatio;
    private double changeAmplitude;

    public FlowSizeChangeProcessor(double changeRatio, double changeAmplitude) {
        this.changeRatio = changeRatio;
        this.changeAmplitude = changeAmplitude;
    }

    protected List<Flow> processRequirements(List<Flow> flows, Random random) throws Exception {
        if (flows.size() == 0) {
            return flows;
        }
        for (int i = 0; i < flows.size() * changeRatio; i++) {
            final Flow flow = flows.get(random.nextInt(flows.size()));
            flow.setTraffic(flow.getTraffic() + (long) (flow.getTraffic() * (random.nextDouble() * 2 * changeAmplitude - changeAmplitude)));
        }
        return flows;
    }
}
