package edu.usc.enl.cacheflow.processor.rule;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.Action;
import edu.usc.enl.cacheflow.util.Util;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/20/11
 * Time: 11:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class CheckFlowOnRules {
    public static boolean check(List<Rule> rules1, List<Rule> rules2, int flowNumInSpace) {
        //generate flows
        Flow f = null;
        while ((f = getNextFlow(f, flowNumInSpace)) != null) {
            Action a1 = route(rules1, f);
            Action a2 = route(rules2, f);
            if (!a1.equals(a2)){
                System.out.println(a1+"!="+a2+"for "+f);
                return false;
            }
        }
        return true;
    }

    private static Action route(List<Rule> rules, Flow f) {
        for (Rule aggregatedRule : rules) {
            if (aggregatedRule.match(f)) {
                return aggregatedRule.getAction();
            }
        }
        return null;
    }

    private static Flow getNextFlow(Flow f, int flowNumInSpace) {
        SortedMap<DimensionInfo,Long> properties = new TreeMap<DimensionInfo,Long>();
        final List<DimensionInfo> infos = Util.getDimensionInfos();
        if (f == null) {
            for (DimensionInfo info : infos) {
                properties.put(info, info.getMin());
            }
            return new Flow(1, null, null,properties);
        }
        for (int i = infos.size() - 1; i >= 0; i--) {
            DimensionInfo dimensionInfo = infos.get(i);
            long step = dimensionInfo.getDimensionRange().getSize() / flowNumInSpace;
            if (f.getProperty(i) + step <= dimensionInfo.getMax()) {
                f.setProperty(i, f.getProperty(i) + step);
                for (int j = i + 1; j < infos.size(); j++) {
                    DimensionInfo info = infos.get(j);
                    f.setProperty(j, info.getMin());
                }
                return f;
            }
        }
        return null;
    }
}
