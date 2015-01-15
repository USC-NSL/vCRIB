package edu.usc.enl.cacheflow.ui;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/29/11
 * Time: 6:42 AM
 * To change this template use File | Settings | File Templates.
 */
/* class RuleWindow extends JFrame {
    private double zoomCoefficient = 1;
    private static final double ZOOM_STEP = 0.8;
    private Canvas canvas;
    private List<Rule> rules;
    private List<String> dimensions;
    private static final Dimension PREFERREDSIZE = new Dimension(300, 300);



    private RuleWindow thisFrame;
    private JFrame parent;

    public RuleWindow(JFrame parent, String title, List<Rule> rules, List<String> dimensions, Map<String, DimensionRange> nameRanges) throws HeadlessException {
        super(title);
        this.parent = parent;
        thisFrame = this;
        this.rules = new ArrayList<Rule>(rules);
        Collections.reverse(this.rules);
        this.dimensions = dimensions;
        createGUI();

        ///caclulate initial zoomCoefficient
        if (dimensions.size() > 2) {
            //need to select
            DimensionSelectionDialog selectionDialog = new DimensionSelectionDialog(parent, dimensions);
            selectionDialog.setVisible(true);
            if (selectionDialog.getReturnCode() == DimensionSelectionDialog.OK_BUTTON) {
                this.dimensions = Arrays.asList(selectionDialog.getDimension1(),selectionDialog.getDimension2());
            }
        }
        long range1 = ((RangeDimensionRange) nameRanges.get(getDimension1())).getSize();
        long range2 = ((RangeDimensionRange) nameRanges.get(getDimension2())).getSize();
        zoomCoefficient = Math.min(1.0 * PREFERREDSIZE.getWidth() / range1, 1.0 * PREFERREDSIZE.getWidth() / range2);


        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        pack();
    }

    private String getDimension1() {
        return dimensions.get(0);
    }

    private String getDimension2() {
        if (dimensions.size() == 1) {
            return dimensions.get(0);
        }
        return dimensions.get(1);
    }

    private void createGUI() {
        JPanel mainPanel = new JPanel(new BorderLayout());
        setContentPane(mainPanel);

        JToolBar toolBar = new JToolBar();
        mainPanel.add(toolBar, BorderLayout.PAGE_START);

        JButton zoomInBtn = new JButton("Zoom in");
        toolBar.add(zoomInBtn);

        JButton zoomOutBtn = new JButton("Zoom out");
        toolBar.add(zoomOutBtn);

        ///////////////////////// Canvas
        canvas = new Canvas();
        mainPanel.add(canvas, BorderLayout.CENTER);
        mainPanel.setPreferredSize(PREFERREDSIZE);

        //////////////////////// Actions
        zoomInBtn.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                zoomCoefficient /= ZOOM_STEP;
                repaint();
            }
        });

        zoomOutBtn.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                zoomCoefficient *= ZOOM_STEP;
                repaint();
            }
        });
    }



    private class Canvas extends JPanel {
        private Point dragStrat;
        private Point origin = new Point(0, 0);
        private Point dragOrigin = new Point(0, 0);

        private Canvas() {
            MouseAdapter ma = new HandleMouseAdapter();
            addMouseListener(ma);
            addMouseMotionListener(ma);
        }

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            Graphics2D g2 = (Graphics2D) g;
            //g2.scale(zoomCoefficient, zoomCoefficient);
            //g2.transform(new AffineTransform(1, 0, 0, -1, 0, 0));
            g2.translate(-origin.getX(), origin.getY());
            String d1 = getDimension1();
            String d2 = getDimension2();
            for (Rule rule : rules) {
                rule.paint(g2, d1, d2, zoomCoefficient);
            }
        }

        private class HandleMouseAdapter extends MouseAdapter {

            public void mouseDragged(MouseEvent e2) {
                super.mousePressed(e2);
                origin.setLocation((int) (-(e2.getX() - dragStrat.getX()) + dragOrigin.getX()),
                        (int) (e2.getY() - dragStrat.getY() + dragOrigin.getY()));
                repaint();
            }

            public void mousePressed(MouseEvent e2) {
                super.mousePressed(e2);
                dragStrat = e2.getPoint();
                dragOrigin.setLocation(origin.getX(), origin.getY());
            }


        }
    }

}*/
