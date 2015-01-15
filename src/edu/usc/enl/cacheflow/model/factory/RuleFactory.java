package edu.usc.enl.cacheflow.model.factory;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.Action;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/8/12
 * Time: 4:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class RuleFactory extends FileFactory<Rule> {
    private static final DimensionInfo HEADER_DIMENSION_INFO = new DimensionInfo("header", 0, 0);
    public List<DimensionInfo> dimensionInfoList;
    public Map<DimensionInfo, Integer> dimensionInfos;

    public RuleFactory(StopCondition stopCondition) {
        super(stopCondition);
    }

    public List<DimensionInfo> getDimensionInfos() {
        return dimensionInfoList;
    }

    @Override
    public void parseHeaderLine(BufferedReader reader, Map<String, Object> parameters) throws IOException {
        super.parseHeaderLine(reader, parameters);
        String[] dimensionsName = reader.readLine().split(",");
        dimensionInfoList = new ArrayList<DimensionInfo>(dimensionsName.length);
        List<RangeDimensionRange> ranges = parseRanges(reader.readLine(), Collections.nCopies(dimensionsName.length, HEADER_DIMENSION_INFO));
        int i = 0;
        for (String name : dimensionsName) {
            RangeDimensionRange range = ranges.get(i);
            DimensionInfo info = new DimensionInfo(name, range.getStart(), range.getEnd());
            dimensionInfoList.add(info);
            i++;
        }
        Util.setDimensionInfos(dimensionInfoList);
    }


    @Override
    protected Rule create(String s) {
        int priority;
        //has priority
        int idIndex = s.lastIndexOf(",") + 1;
        int id = Integer.parseInt(s.substring(idIndex));
        s = s.substring(0, idIndex - 1);

        final int priorityIndex = s.lastIndexOf(",") + 1;
        priority = Integer.parseInt(s.substring(priorityIndex));
        s = s.substring(0, priorityIndex - 1);

        //extract action
        int actionIndex = s.lastIndexOf(",") + 1;
        String action = s.substring(actionIndex);

        // parse ranges
        List<RangeDimensionRange> ranges2 = parseRanges(s.substring(0, actionIndex - 1), dimensionInfoList);

        return new Rule(Action.getAction(action), ranges2, priority, id);
    }

    public static List<RangeDimensionRange> parseRanges(String s, List<DimensionInfo> infos) {
        StringTokenizer st = new StringTokenizer(s, ",");
        int l = infos.size();
        List<RangeDimensionRange> output = new ArrayList<RangeDimensionRange>(l);
        for (DimensionInfo info : infos) {
            output.add(info.parseRange(st.nextToken()));
        }
        return output;
    }
}
