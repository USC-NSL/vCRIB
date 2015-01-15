package edu.usc.enl.cacheflow.processor.statistics;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.processor.Processor;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/15/11
 * Time: 12:06 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class StatisticsProcessor<T> extends Processor<T, Statistics> {
    protected Map<String, Object> parameters;
    public long startTime;
    public static final String PROCESS_TIME_STAT = "time";

    protected StatisticsProcessor(T input, Map<String, Object> parameters) {
        super(input);
        this.parameters = parameters;
    }

    protected StatisticsProcessor(Processor<?, T> processorInput, Map<String, Object> parameters) {
        super(processorInput);
        this.parameters = parameters;
    }

    @Override
    public Statistics run() throws Exception {
        startTime = System.currentTimeMillis();
        return super.run();

    }

    @Override
    public Statistics process(T input) throws Exception {
        Statistics stat = getStat(input);
        stat.addStat(PROCESS_TIME_STAT, (System.currentTimeMillis() - startTime)/1000.0);
        for (Map.Entry<String, Object> paramNameValueEntry : parameters.entrySet()) {
            stat.addParameter(paramNameValueEntry.getKey(), paramNameValueEntry.getValue());
        }
        return stat;
    }

    protected abstract Statistics getStat(T input) throws Exception;
}
