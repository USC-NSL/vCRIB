package edu.usc.enl.cacheflow.scripts.preliminaries.rule;

import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.transform.SimilarityTransformer;
import edu.usc.enl.cacheflow.util.CustomRandomGenerator;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 10/23/12
 * Time: 8:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class IncreaseSimilarity {
    public static void main(String[] args) throws CustomRandomGenerator.AllZeroWeightException, IOException {
        int randomSeedIndex = Integer.parseInt(args[0]);
        String inputRuleFile = args[1];
        String outputRuleFile = args[2];


        Util.setRandom(randomSeedIndex);

        Random random = Util.random;
        Map<String, Object> parameters = new HashMap<>();
        List<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()), inputRuleFile, parameters,
                new LinkedList<Rule>());
        CustomRandomGenerator<Long> toAdd = new CustomRandomGenerator<Long>(new Long[]{1l << 10, 1l << 11, 1l << 12}, new double[]{1d / 3, 1d / 3, 1d / 3});
        CustomRandomGenerator<Long> toRemove = new CustomRandomGenerator<Long>(new Long[]{ 2l, 4l, 8l, 16l, 1l << 5, 1l << 6, 1l << 7, 1l << 8},
                new double[]{1d / 8, 1d / 8, 1d / 8, 1d / 8, 1d / 8, 1d / 8, 1d / 8, 1d / 8});
        SimilarityTransformer transformer = new SimilarityTransformer(random);
        List<Rule> transformedRules = transformer.transform(rules, toRemove, toAdd, 1000);

        File file = new File(outputRuleFile);
        file.getParentFile().mkdirs();
        WriterSerializableUtil.writeFile(transformedRules, file, false, parameters);
    }
}
