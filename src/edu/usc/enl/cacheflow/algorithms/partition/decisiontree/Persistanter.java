package edu.usc.enl.cacheflow.algorithms.partition.decisiontree;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 5/25/12
 * Time: 6:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class Persistanter {

    private Connection con;

    private LinkedBlockingQueue<Object[]> queue = new LinkedBlockingQueue<Object[]>();
    private List<SaveThread> threads = new ArrayList<SaveThread>();
    private PreparedStatement allLeavesRule;
    private PreparedStatement allLeavesRuleGroup2;
    public final String url = "jdbc:mysql://localhost:3306/treedb";
    public final String user = "cacheflow";
    public final String password = "cacheflow";
    public static String tableName = "treenode_4_1";
    public final String MATCH_CHILD_SRC_STRING = "SELECT rule,dipstart,dipend,protstart,protend,dportstart,dportend,id FROM " + tableName + " WHERE sipstart<=? AND sipend>=? " +
            " LIMIT 0,?;";
    public List<Connection> connections = new ArrayList<Connection>();

    public Persistanter(int connections) {
        try {
            con = DriverManager.getConnection(url, user, password);
            for (int i = 0; i < connections; i++) {
                threads.add(new SaveThread(DriverManager.getConnection(url, user, password)));
            }
            for (SaveThread thread : threads) {
                thread.start();
            }

            allLeavesRule = con.prepareStatement("SELECT id,rule FROM " + tableName + " where rule=? AND id>? ORDER BY id LIMIT 0,?;");
            allLeavesRuleGroup2 = con.prepareStatement("SELECT id,rule FROM " + tableName + " where rule>=? ORDER BY rule,id LIMIT 0,?;");

            /*deleteSt = con.prepareStatement("DELETE FROM treenode WHERE id=?;");
            updateSt = con.prepareStatement("UPDATE treenode SET pid=?,level=?,rstart=?,rend=? WHERE id=?;");*/
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public PreparedStatement getNewMatchChildSrc() {
        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            connections.add(connection);
            return connection.prepareStatement(MATCH_CHILD_SRC_STRING);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void createDB() {
        try {
            final Statement statement = con.createStatement();
            statement.execute("CREATE DATABASE IF NOT EXISTS treedb;");
            statement.execute("USE treedb;");
            statement.execute("DROP TABLE IF EXISTS " + tableName + " ;");
            statement.execute("CREATE TABLE IF NOT EXISTS  " + tableName + "  ( " +
                    "id int NOT NULL UNIQUE PRIMARY KEY," +
                    "sipstart int UNSIGNED NOT NULL," +
                    "sipend int UNSIGNED NOT NULL," +
                    "dipstart int UNSIGNED NOT NULL," +
                    "dipend int UNSIGNED NOT NULL," +
                    "dportstart int UNSIGNED NOT NULL," +
                    "dportend int UNSIGNED NOT NULL," +
                    "protstart int UNSIGNED NOT NULL," +
                    "protend int UNSIGNED NOT NULL," +
                    "rule int UNSIGNED NOT NULL" +
                    //",temp int UNSIGNED NOT NULL" +
                    ",INDEX ips (sipstart,sipend)" +
                    ",INDEX rule (rule)" +
                    ",INDEX ruleid (rule, id)" +
                    ") ;");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void save(int id, int minRule, List<RangeDimensionRange> ranges) {
        queue.offer(new Object[]{id,
                ranges.get(0).getStart(),//srcip
                ranges.get(0).getEnd(),
                ranges.get(1).getStart(),//dstip
                ranges.get(1).getEnd(),
                ranges.get(3).getStart(),//dstport
                ranges.get(3).getEnd(),
                ranges.get(2).getStart(),//protocol
                ranges.get(2).getEnd(),//4 is src port
                minRule
        });
    }

    public void finishSave() {

        for (SaveThread thread : threads) {
            queue.offer(new Object[]{});
        }
        try {
            for (SaveThread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            con.close();
            for (Connection connection : connections) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static class BlockEntry {
        int ruleId;
        RangeDimensionRange[] ranges;
        private int id;

        public BlockEntry(int ruleId, RangeDimensionRange[] ranges) {
            this.ruleId = ruleId;
            this.ranges = ranges;
        }

        public int getRuleID() {
            return ruleId;
        }

        public void setId(int id) {
            this.id = id;
        }

        public void setRule(int ruleId) {
            this.ruleId = ruleId;
        }

        public RangeDimensionRange[] getRanges() {
            return ranges;
        }

        public void setRanges(RangeDimensionRange[] ranges) {
            this.ranges = ranges;
        }

        public int getId() {
            return id;
        }

    }

    public static class FinishBlock extends BlockEntry {

        public FinishBlock(int ruleId, RangeDimensionRange[] ranges) {
            super(ruleId, ranges);
        }
    }

    public int getMatchChildSrc(long srcIp, List<BlockEntry> buffer, int loadSize, Object passedStatement) {
        //SELECT rule,r2start,r2end,r3start,r3end FROM treenode WHERE r0start<=? AND r0end>=? AND r1start<=? AND r1end>=?;
        int i = 0;
        PreparedStatement st;
        st = (PreparedStatement) passedStatement;
        try {
            st.setLong(1, srcIp);
            st.setLong(2, srcIp);
            st.setLong(3, loadSize);
            final ResultSet resultSet = st.executeQuery();
            while (resultSet.next()) {
                final BlockEntry blockEntry = buffer.get(i);
                blockEntry.setId(resultSet.getInt(8));
                blockEntry.setRule(resultSet.getInt(1));
                blockEntry.getRanges()[0].setStart(resultSet.getLong(2));
                blockEntry.getRanges()[0].setEnd(resultSet.getLong(3));
                blockEntry.getRanges()[1].setStart(resultSet.getLong(4));
                blockEntry.getRanges()[1].setEnd(resultSet.getLong(5));
                blockEntry.getRanges()[2].setStart(resultSet.getLong(6));
                blockEntry.getRanges()[2].setEnd(resultSet.getLong(7));
                i++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }



    public int getLeavesRule(List<Persistanter.BlockEntry> buffer, int rule, int loadSize) {
        int i = 0;
        try {
            allLeavesRuleGroup2.setInt(1, rule);
            allLeavesRuleGroup2.setInt(2, loadSize);
            final ResultSet resultSet = allLeavesRuleGroup2.executeQuery();
            while (resultSet.next()) {
                buffer.get(i).setId(resultSet.getInt(1));
                buffer.get(i).setRule(resultSet.getInt(2));
                i++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }

    public int getLeavesSpecificRule(List<Persistanter.BlockEntry> buffer, int rule, int id, int loadSize) {
        int i = 0;
        try {
            allLeavesRule.setInt(1, rule);
            allLeavesRule.setInt(2, id);
            allLeavesRule.setInt(3, loadSize);
            final ResultSet resultSet = allLeavesRule.executeQuery();
            while (resultSet.next()) {
                buffer.get(i).setId(resultSet.getInt(1));
                buffer.get(i).setRule(resultSet.getInt(2));
                i++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }


    public int load(List<Integer> ids, BlockingQueue<BlockEntry> q, int loadSize) {
        StringBuilder sb = new StringBuilder("SELECT * FROM  " + tableName + "  WHERE id IN (");
        for (Integer id : ids) {
            sb.append(id).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(") limit 0,").append(loadSize).append(";");
        int i = 0;
        try {
            final Statement statement = con.createStatement();
            final ResultSet resultSet = statement.executeQuery(sb.toString());
            i = 0;
            while (resultSet.next()) {
                BlockEntry blockEntry = new BlockEntry(resultSet.getInt(10), new RangeDimensionRange[]{
                        new RangeDimensionRange(resultSet.getLong(2), resultSet.getLong(3), Util.SRC_IP_INFO),
                        new RangeDimensionRange(resultSet.getLong(4), resultSet.getLong(5), Util.DST_IP_INFO),
                        new RangeDimensionRange(resultSet.getLong(8), resultSet.getLong(9), Util.PROTOCOL_INFO),
                        new RangeDimensionRange(resultSet.getLong(6), resultSet.getLong(7), Util.DST_PORT_INFO)});
                blockEntry.setId(resultSet.getInt(1));
                q.offer(blockEntry);
                i++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }

    private class SaveThread extends Thread {
        private int insertStBatch = 0;
        private Connection con;
        private PreparedStatement insertSt;

        private SaveThread(Connection con) {
            this.con = con;
            try {
                con.setAutoCommit(false);
                insertSt = con.prepareStatement("INSERT INTO  " + tableName + "  VALUES (?,?,?,?,?,?,?,?,?,?);");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }



        @Override
        public void run() {
            try {
                while (true) {
                    final Object[] node = queue.take();
                    if (node.length == 0) {
                        break;
                    }
                    insertSt.setInt(1, (Integer) node[0]);
                    insertSt.setLong(2, (Long) node[1]);
                    insertSt.setLong(3, (Long) node[2]);
                    insertSt.setLong(4, (Long) node[3]);
                    insertSt.setLong(5, (Long) node[4]);
                    insertSt.setLong(6, (Long) node[5]);
                    insertSt.setLong(7, (Long) node[6]);
                    insertSt.setLong(8, (Long) node[7]);
                    insertSt.setLong(9, (Long) node[8]);
                    insertSt.setInt(10, (Integer) node[9]);
                    //insertSt.setLong(11, 0);//(Integer) node[9] + MAX_ID + (Integer) node[0]
                    insertSt.addBatch();
                    //System.out.println(node[0]+" added");
                    insertStBatch++;
                    if ((insertStBatch + 1) % 1000 == 0) {
                        System.out.println("batch but in queue=" + queue.size());
                        insertSt.executeBatch();// Execute every 1000 items.
                        con.commit();
                    }
                }
                insertSt.executeBatch();
                con.commit();
                insertSt.close();
                con.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
