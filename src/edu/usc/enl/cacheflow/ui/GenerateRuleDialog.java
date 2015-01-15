package edu.usc.enl.cacheflow.ui;


import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;

import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import java.awt.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/29/11
 * Time: 1:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateRuleDialog extends JDialog {
    JDialog thisDialog;
    JTable configTable;

    public GenerateRuleDialog() {
        setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
        createGUI();

        pack();
    }

    private void createGUI() {
        configTable = new JTable(new ConfigTableModel());
        getContentPane().add(configTable);
        // initialize columns
        String[] dimensionTypeNames = new String[]{"SRCIP", "DSTIP", "PROTO", "SRCPORT", "DSTPORT", "Custom"};
        TableColumn dimensionType = configTable.getColumnModel().getColumn(0);
        dimensionType.setCellEditor(new MyComboBoxEditor(dimensionTypeNames));
        dimensionType.setCellRenderer(new MyComboBoxRenderer(dimensionTypeNames));
    }

    private class ConfigTableModel extends AbstractTableModel {
        Map<String, RangeDimensionRange> ranges = new HashMap<String, RangeDimensionRange>();
        Map<String,String> typeRangeName = new HashMap<String, String>();
        java.util.List<String> selectedRangeTypes = new LinkedList<String>();

        String[] columns = new String[]{"PreType", "Name", "Start", "End"};

        @Override
        public boolean isCellEditable(int rowIndex, int columnIndex) {
            return true;
        }

        public int getRowCount() {
            return ranges.size();
        }

        public int getColumnCount() {
            return columns.length;
        }

        public String getColumnName(int col) {
            return columns[col];
        }

        public Object getValueAt(int row, int column) {
            if (column==0){
                return selectedRangeTypes.get(row);
            }else if (column==1){
                return typeRangeName.get((String)getValueAt(row,0));
            }else if (column==2){
                return ( ranges.get((String) getValueAt(row, 1)).getStart());
            }else{
                return ( ranges.get((String) getValueAt(row, 1)).getEnd());
            }
        }

        public Class getColumnClass(int c) {
            return getValueAt(0, c).getClass();
        }

        public void setValueAt(Object value, int row, int col) {
                System.out.println("Setting value at " + row + "," + col
                                   + " to " + value
                                   + " (an instance of "
                                   + value.getClass() + ")");

            //data[row][col] = value;
            fireTableCellUpdated(row, col);

        }
    }

    public class MyComboBoxRenderer extends JComboBox implements TableCellRenderer {
        public MyComboBoxRenderer(String[] items) {
            super(items);
        }

        public Component getTableCellRendererComponent(JTable table, Object value,
                                                       boolean isSelected, boolean hasFocus, int row, int column) {
            if (isSelected) {
                setForeground(table.getSelectionForeground());
                super.setBackground(table.getSelectionBackground());
            } else {
                setForeground(table.getForeground());
                setBackground(table.getBackground());
            }

            // Select the current value
            setSelectedItem(value);
            return this;
        }
    }

    public class MyComboBoxEditor extends DefaultCellEditor {
        public MyComboBoxEditor(String[] items) {
            super(new JComboBox(items));
        }

        @Override
        protected void fireEditingStopped() {
            super.fireEditingStopped();
            System.out.println("ttttttttt");
        }
    }

    public static void main(String[] args) {
        GenerateRuleDialog dialog = new GenerateRuleDialog();
        dialog.setDefaultCloseOperation(JDialog.HIDE_ON_CLOSE);
        dialog.setVisible(true);
    }
}
