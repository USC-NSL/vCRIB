package edu.usc.enl.cacheflow.model.exceptions;

import edu.usc.enl.cacheflow.model.Statistics;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/14/11
 * Time: 8:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class StatisticsParameterNotFoundException extends Exception {
    public StatisticsParameterNotFoundException(String parameter, Statistics statistics) {
        super("Parameter " + parameter + " not found in " + statistics);
    }
}
