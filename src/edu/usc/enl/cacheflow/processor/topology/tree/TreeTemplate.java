package edu.usc.enl.cacheflow.processor.topology.tree;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/5/11
 * Time: 3:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class TreeTemplate {
    private Map<String, List<String>> levelNodeProperties = new HashMap<String, List<String>>();
    private Map<NodePair, List<Long>> levelLinkProperties = new HashMap<NodePair, List<Long>>();
    private List<String> levelNames = new LinkedList<String>();
    public static final String NODES_DEFINITION_START = "#nodes";
    public static final String LINK_DEFINITION_START = "#linkes";
    public static final int LINK_CAPACITY_INDEX = 0;

    public TreeTemplate(String input) {
        boolean nodesDefinitionFound = false;
        boolean linksDefinitionFound = false;
        String[] split = input.split("\n");
        int splitLength = split.length;
        for (int i = 0; i < splitLength; i++) {
            String line = split[i];
            if (line.toLowerCase().startsWith(NODES_DEFINITION_START)) {
                //found levelNodeProperties definition
                nodesDefinitionFound = true;
            } else if (line.toLowerCase().startsWith(LINK_DEFINITION_START)) {
                linksDefinitionFound = true;
                nodesDefinitionFound = false;
            } else if (nodesDefinitionFound) {
                //handle node definition section
                String[] s = line.split("\\s+");
                List<String> values = new ArrayList<String>(s.length - 1);
                for (int i1 = 1, sLength = s.length; i1 < sLength; i1++) {
                    values.add(s[i1]);
                }
                levelNodeProperties.put(s[0], values);
                levelNames.add(s[0]);

            } else if (linksDefinitionFound) {
                String[] s = line.split("\\s+");
                List<Long> values = new ArrayList<Long>(s.length - 1);
                for (int i1 = 2, sLength = s.length; i1 < sLength; i1++) {
                    String s1 = s[i1];
                    values.add(Long.parseLong(s1));
                }
                if (s[0].compareTo(s[1]) >= 0) {
                    levelLinkProperties.put(new NodePair(s[0], s[1]), values);
                } else {
                    levelLinkProperties.put(new NodePair(s[1], s[0]), values);
                }
            }
        }
    }

    public List<String> getLevelsName() {
        return levelNames;
    }

    public List<String> getLevelNodeProperties(String level) {
        return levelNodeProperties.get(level);
    }

    public List<Long> getLevelLinkProperties(String l1, String l2) {
        if (l1.compareTo(l2) >= 0) {
            return levelLinkProperties.get(new NodePair(l1, l2));
        } else {
            return levelLinkProperties.get(new NodePair(l2, l1));
        }
    }


    private class NodePair {
        private String first;
        private String second;

        private NodePair(String first, String second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NodePair nodePair = (NodePair) o;

            if (first != null ? !first.equals(nodePair.first) : nodePair.first != null) return false;
            if (second != null ? !second.equals(nodePair.second) : nodePair.second != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = first != null ? first.hashCode() : 0;
            result = 31 * result + (second != null ? second.hashCode() : 0);
            return result;
        }
    }

}
