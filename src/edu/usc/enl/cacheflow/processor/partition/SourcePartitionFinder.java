package edu.usc.enl.cacheflow.processor.partition;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 9/17/12
 * Time: 2:30 AM
 * To change this template use File | Settings | File Templates.
 */
public class SourcePartitionFinder {

    public int srcIPIndex;

    public Map<Switch, Collection<Partition>> process( Collection<Partition> partitions, Map<Long,Switch> sourceIPs) {
        srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);

        final OVSClassifier ovsClassifier = new OVSClassifier(partitions);
        Map<Switch, Collection<Partition>> output = new HashMap<>();

        for (Map.Entry<Long,Switch> entry : sourceIPs.entrySet()) {
            final Switch source = entry.getValue();
            Collection<Partition> sourcePartitions = output.get(source);
            if (sourcePartitions==null){
                sourcePartitions= new HashSet<>();
                output.put(source,sourcePartitions);
            }
            sourcePartitions.addAll(ovsClassifier.getPartition(entry.getKey()));
        }
        return output;
    }

    private class OVSClassifier {
        private Map<Integer, Bucket> buckets = new HashMap<Integer, Bucket>();

        protected Integer getPartitionWC(Partition partition) throws UnalignedRangeException {
            final RangeDimensionRange srcIPRange = partition.getProperty(srcIPIndex);
            return srcIPRange.getNumberOfWildcardBits();
        }

        public List<Partition> getPartition(Long srcIp) {
            List<Partition> output = new LinkedList<Partition>();
            for (Map.Entry<Integer, Bucket> entry : buckets.entrySet()) {
                final Long maskedProperties = mask(srcIp, entry.getKey());
                final List<Partition> rule = entry.getValue().getPartition(maskedProperties);
                if (rule != null) {
                    output.addAll(rule);
                }
            }
            if (output.size() == 0) {
                throw new RuntimeException("No match for flow " + srcIp);
            }
            return output;
        }

        public OVSClassifier(Collection<Partition> partitions) {
            try {
                for (Partition partition : partitions) {
                    final Integer wc = getPartitionWC(partition);
                    Bucket bucket = buckets.get(wc);
                    if (bucket == null) {
                        bucket = new Bucket();
                        buckets.put(wc, bucket);
                    }
                    bucket.addPartition(partition);
                }
            } catch (UnalignedRangeException e) {
                e.printStackTrace();
            }

        }

        private Long mask(Long srcIP, Integer tuple) {
            return srcIP >> tuple << tuple;
        }

        private class Bucket {
            private Map<Integer, Object> partitions = new HashMap<Integer, Object>();

            private Rule match(Flow flow) {
                for (Map.Entry<Integer, Object> entry : partitions.entrySet()) {
                    if (entry.getValue() instanceof Rule) {
                        if (((Rule) entry.getValue()).match(flow)) {
                            return (Rule) entry.getValue();
                        }
                    } else {
                        final List<Rule> list = (List<Rule>) entry.getValue();
                        for (Rule rule : list) {
                            if (rule.match(flow)) {
                                return rule;
                            }
                        }
                    }

                }
                return null;
            }

            private List<Partition> getPartition(Long properties) {

                final Object o = partitions.get(hash(properties));
                if (o == null) {
                    return null;
                } else if (o instanceof Partition) {
                    return match(properties, (Partition) o) ? Collections.singletonList((Partition) o) : null;
                }
                //its a list
                List<Partition> output = new LinkedList<Partition>();
                for (Partition partition : (List<Partition>) o) {
                    if (match(properties, partition)) {
                        output.add(partition);
                    }
                }
                return output.size() > 0 ? output : null;
            }

            private boolean match(Long property, Partition partition) {
                return partition.getProperty(srcIPIndex).match(property);
            }

            private int hash(Long property) {
                int output = 1;
                output = 31 * output + (int) (property ^ (property >>> 32));

                return output;
            }

            private int ruleHashCode(Partition partition) {
                int output = 1;
                List<RangeDimensionRange> properties = partition.getProperties();
                RangeDimensionRange range = properties.get(srcIPIndex);
                final long start = range.getStart();
                output = 31 * output + (int) (start ^ (start >>> 32));
                return output;
            }

            private void addPartition(Partition partition) {
                final int ruleStartHashCode = ruleHashCode(partition);
                final Object oldPartition = this.partitions.get(ruleStartHashCode);
                if (oldPartition != null) {
                    if (oldPartition instanceof Partition) {
                        final Partition oldPartition1 = (Partition) oldPartition;
                        List<Partition> collisionList = new ArrayList<Partition>();
                        collisionList.add(partition);
                        collisionList.add(oldPartition1);
                        this.partitions.put(ruleStartHashCode, collisionList);
                    } else {
                        //there is a collision list
                        List<Partition> collisionList = (List<Partition>) oldPartition;
                        collisionList.add(partition);
                    }
                } else {
                    this.partitions.put(ruleStartHashCode, partition);
                }
            }
        }

    }

    @Override
    public String toString() {
        return "VMStart";
    }
}
