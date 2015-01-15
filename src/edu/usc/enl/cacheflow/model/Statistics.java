package edu.usc.enl.cacheflow.model;

import edu.usc.enl.cacheflow.model.exceptions.StatisticsParameterNotFoundException;
import edu.usc.enl.cacheflow.model.topology.Topology;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static java.lang.Math.pow;


/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 5:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class Statistics {
    private final Map<String, Number> stats;
    private final Map<String, Parameter> parameters;

    public Statistics() {
        stats = new TreeMap<String, Number>();
        parameters = new HashMap<String, Parameter>();
    }

    public static double extractStatFromFile(String statsFile,String statName, int rowIndex) throws IOException {
        double value = 0;
        //read csv
        BufferedReader reader = new BufferedReader(new FileReader(statsFile));
        String header = reader.readLine();
        String data = "";
        for (int i = 0; i < rowIndex; i++) {
            data = reader.readLine();
        }
        reader.close();
        StringTokenizer stHeader = new StringTokenizer(header, ",");
        StringTokenizer stData = new StringTokenizer(data, ",");

        boolean valueFound = false;
        //go to column with traffic
        while (stHeader.hasMoreTokens() && stData.hasMoreTokens()) {
            if (stHeader.nextToken().equals(statName)) {
                //check traffic
                value = Double.parseDouble(stData.nextToken());
                valueFound = true;
                break;
            }
            stData.nextToken();
        }
        if (!valueFound) {
            throw new RuntimeException("Traffic stats " + Topology.TRAFFIC_STAT + " not found in the file " +statsFile);
        }
        return value;

    }

    public void setParameters(Map<String, Object> parameters) {
        for (Map.Entry<String, Object> parameterEntry : parameters.entrySet()) {
            addParameter(parameterEntry.getKey(), parameterEntry.getValue());
        }
    }

    public void addStat(String name, Number value) {
        stats.put(name, value);
    }

    public void addStat(String prefix, Statistics stat) {
        for (Map.Entry<String, Number> nameValueEntry : stat.stats.entrySet()) {
            stats.put(prefix + ":" + nameValueEntry.getKey(), nameValueEntry.getValue());
        }
    }

    public void addParameter(String name, Object value) {
        if (value instanceof Number) {
            addParameter(name, (Number) value);
        } else {
            addParameter(name, value.toString());
        }
    }

    public Collection<String> getStatNames() {
        return stats.keySet();
    }

    public void addParameter(String name, Number value) {
        parameters.put(name, new NumberParameter(value));
    }

    public void addParameter(String name, String value) {
        parameters.put(name, new StringParameter(value));
    }

    public Number getStat(String name) {
        return stats.get(name);
    }

    public void joinStats(Statistics other) {
        for (String name : other.getStatNames()) {
            stats.put(name, other.getStat(name));
        }
    }

    public static Double getMean(Collection<? extends Number> stats) {
        double sum = 0;
        int num = 0;
        for (Number stat : stats) {
            if (stat != null) {
                sum += stat.doubleValue();
                num++;
            }
        }
        return sum / num;
    }

    public static Double getSum(Collection<? extends Number> stats) {
        double sum = 0;
        for (Number stat : stats) {
            if (stat != null) {
                sum += stat.doubleValue();
            }
        }
        return sum;
    }

    public static Number getMin(Collection<? extends Number> stats) {
        Number min = null;
        for (Number stat : stats) {
            if (min == null) {
                min = stat;
                continue;
            }
            if (stat != null) {
                min = min.doubleValue() > stat.doubleValue() ? stat : min;
            }
        }
        return min;
    }

    public static Number getMax(Collection<? extends Number> stats) {
        Number max = null;
        for (Number stat : stats) {
            if (max == null) {
                max = stat;
                continue;
            }
            if (stat != null) {
                max = max.doubleValue() < stat.doubleValue() ? stat : max;
            }
        }
        return max;
    }

    public static Number getVar(Collection<? extends Number> stats, Number mean) {
        if (stats.size() == 0) {
            return 0;
        }
        double sum = 0;
        final double meanValue = mean.doubleValue();
        for (Number stat : stats) {
            sum += pow(stat.doubleValue() - meanValue, 2);
        }
        return sum / stats.size();
    }

    public static Double getMean(Collection<Statistics> stats, String statName) {
        double sum = 0;
        int num = 0;
        for (Statistics stat : stats) {
            Number value = stat.getStat(statName);
            if (value != null) {
                sum += value.doubleValue();
                num++;
            }
        }
        return sum / num;
    }

    public static Double getSum(Collection<Statistics> stats, String statName) {
        double sum = 0;
        for (Statistics stat : stats) {
            Number value = stat.getStat(statName);
            if (value != null) {
                sum += value.doubleValue();
            }
        }
        return sum;
    }

    public static Number getMin(Collection<Statistics> stats, String statName) {
        Number min = null;
        for (Statistics stat : stats) {
            Number value = stat.getStat(statName);
            if (min == null) {
                min = value;
                continue;
            }
            if (value != null) {
                min = min.doubleValue() > value.doubleValue() ? value : min;
            }
        }
        return min;
    }

    public static Number getVar(List<Statistics> statsList, String statName, Number mean) {
        if (statsList.size() == 0) {
            return 0;
        }
        double sum = 0;
        final double meanValue = mean.doubleValue();
        for (Statistics statistics : statsList) {
            final Number stat = statistics.getStat(statName);
            sum += pow(stat.doubleValue() - meanValue, 2);
        }
        return sum / statsList.size();
    }

    public Parameter getParameter(String name) {
        return parameters.get(name);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Number> nameStat : stats.entrySet()) {
            sb.append(nameStat.getKey()).append(":").append(nameStat.getValue()).append("\n");
        }
        if (stats.size() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }


    public static Map<List<Parameter>, List<Statistics>> categorize(Collection<String> parameterNames, Collection<Statistics> statisticses) throws StatisticsParameterNotFoundException {
        Map<List<Parameter>, List<Statistics>> outputMap = new TreeMap<List<Parameter>, List<Statistics>>(new ListParameterComparator());
        List<String> sortedParameterNames = new ArrayList<String>(parameterNames);
        Collections.sort(sortedParameterNames);
        for (Statistics statistics : statisticses) {
            List<Parameter> parameterValues = new ArrayList<Parameter>(sortedParameterNames.size());
            for (String parameterName : sortedParameterNames) {
                Parameter parameterValue = statistics.getParameter(parameterName);
                if (parameterValue == null) {
                    throw new StatisticsParameterNotFoundException(parameterName, statistics);
                }
                parameterValues.add(parameterValue);
            }
            List<Statistics> statisticses1 = outputMap.get(parameterValues);
            if (statisticses1 == null) {
                List<Statistics> stats = new LinkedList<Statistics>();
                stats.add(statistics);
                outputMap.put(parameterValues, stats);
            } else {
                statisticses1.add(statistics);
            }
        }
        return outputMap;
    }

    public static void unionStatistics(Collection<Statistics> stats) {
        //get a union stat names
        Set<String> statNames = new HashSet<String>();
        for (Statistics stat : stats) {
            statNames.addAll(stat.getStatNames());
        }
        for (String statName : statNames) {
            for (Statistics stat : stats) {
                if (stat.getStat(statName) == null) {
                    stat.addStat(statName, 0);
                }
            }
        }
    }

    public static String csvStatistics(Collection<String> parameterNames, Map<List<Parameter>, List<Statistics>> categorizedStats, Collection<String> metrics, boolean mean, boolean headerLine) {
        StringBuilder sb = new StringBuilder();
        if (headerLine) {
            List<String> sortedParameterNames = new ArrayList<String>(parameterNames);
            Collections.sort(sortedParameterNames);
            for (String parameterName : sortedParameterNames) {
                sb.append(parameterName).append(",");
            }
            for (String metric : metrics) {
                sb.append(metric).append(",");
            }
            sb.setCharAt(sb.length() - 1, '\n');
        }
        for (List<Parameter> parameters : categorizedStats.keySet()) {
            for (Parameter parameter : parameters) {
                sb.append(parameter).append(",");
            }
            for (String metric : metrics) {
                if (mean) {
                    sb.append(getMean(categorizedStats.get(parameters), metric)).append(",");
                } else {
                    sb.append(getSum(categorizedStats.get(parameters), metric)).append(",");
                }
            }
            sb.setCharAt(sb.length() - 1, '\n');
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    public static String getMeanLine(Map<List<Parameter>, List<Statistics>> categorizedStatistics, String metric) {
        StringBuilder sb = new StringBuilder();
        for (List<Statistics> statisticses : categorizedStatistics.values()) {
            sb.append(Statistics.getMean(statisticses, metric)).append(", ");
        }
        return sb.toString();
    }

    public static String getHeaderLine(Map<List<Parameter>, List<Statistics>> categorizedStatistics) {
        StringBuilder sbOutput = new StringBuilder();
        for (Map.Entry<List<Parameter>, List<Statistics>> parametersValueStats : categorizedStatistics.entrySet()) {
            //compute string representation of parametervalues
            StringBuilder sb = new StringBuilder();
            List<Parameter> key = parametersValueStats.getKey();
            for (int i = 0, keySize = key.size(); i < keySize; i++) {
                String paramValue = key.get(i).toString();
                sb.append(paramValue).append(i < keySize - 1 ? ":" : "");
            }
            sbOutput.append(sb.toString()).append(", ");
        }
        if (sbOutput.length() > 0) {
            sbOutput.deleteCharAt(sbOutput.length() - 1);
        }
        return sbOutput.toString();
    }

    public static Number getMax(List<Statistics> stats, String statName) {
        Number max = null;
        for (Statistics stat : stats) {
            Number value = stat.getStat(statName);
            if (max == null) {
                max = value;
                continue;
            }
            if (value != null) {
                max = max.doubleValue() < value.doubleValue() ? value : max;
            }
        }
        return max;
    }


    public static class ListParameterComparator implements Comparator<List<Parameter>> {

        public int compare(List<Parameter> o1, List<Parameter> o2) {
            final Iterator<Parameter> iterator2 = o2.iterator();
            for (Parameter p1 : o1) {
                Parameter p2 = iterator2.next();
                final int compareResult = p1.compareTo(p2);
                if (compareResult != 0) {
                    return compareResult;
                }
            }
            return 0;
        }
    }

    public String getParameterLine() {
        return getParameterLine(this.parameters);
    }

    public static String getParameterLine(Map<String, ? extends Object> parameters) {
        StringBuilder sb = new StringBuilder();
        List<String> sortedParameterNames = new ArrayList<String>(parameters.keySet());
        Collections.sort(sortedParameterNames);
        for (String name : sortedParameterNames) {
            sb.append(name).append("=").append(parameters.get(name)).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static String serializeMetric(Map<List<String>, List<Statistics>> categorizedStatistics, String metric, boolean writeHeader) {
        List<List<String>> outputTable = new ArrayList<List<String>>(categorizedStatistics.size());
        int numberOfColumns = 0;
        for (Map.Entry<List<String>, List<Statistics>> parametersValueStats : categorizedStatistics.entrySet()) {
            //compute string representation of parametervalues
            StringBuilder sb = new StringBuilder();
            List<String> key = parametersValueStats.getKey();
            for (int i = 0, keySize = key.size(); i < keySize; i++) {
                String paramValue = key.get(i);
                sb.append(paramValue).append(i < keySize - 1 ? ":" : "");
            }
            List<Statistics> stats = parametersValueStats.getValue();
            List<String> tableRow = new ArrayList<String>(stats.size());
            numberOfColumns = stats.size() + 1;
            outputTable.add(tableRow);
            tableRow.add(sb.toString());
            for (Statistics stat : stats) {
                tableRow.add("" + stat.getStat(metric));
            }
        }

        StringBuilder sb = new StringBuilder();

        //traverse table and compute the output
        for (int i = writeHeader ? 0 : 1; i < numberOfColumns; i++) {
            for (List<String> anOutputTable : outputTable) {
                sb.append(anOutputTable.get(i)).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            if (i < numberOfColumns - 1) {
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    public interface Parameter extends Comparable<Parameter> {
        public Object getValue();
    }

    private static class StringParameter implements Parameter {
        private String stringValue;

        private StringParameter(String stringValue) {
            this.stringValue = stringValue;
        }

        public int compareTo(Parameter o) {
            if (o instanceof StringParameter) {
                return (stringValue.compareTo(((StringParameter) o).stringValue));
            }
            throw new RuntimeException("Uncomparable parameters");
        }

        public Object getValue() {
            return stringValue;
        }

        @Override
        public String toString() {
            return getValue().toString();
        }
    }

    private static class NumberParameter implements Parameter {
        private Number numberValue;

        private NumberParameter(Number numberValue) {
            this.numberValue = numberValue;
        }

        public int compareTo(Parameter o) {
            if (o instanceof NumberParameter) {
                return (int) (numberValue.doubleValue() - ((NumberParameter) o).numberValue.doubleValue());
            }
            throw new RuntimeException("Uncomparable parameters");
        }

        public Object getValue() {
            return numberValue;
        }

        @Override
        public String toString() {
            return getValue().toString();
        }
    }


}
