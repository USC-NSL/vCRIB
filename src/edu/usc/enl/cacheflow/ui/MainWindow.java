package edu.usc.enl.cacheflow.ui;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.PartitionFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.RandomFlowGenerator;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.ClassBenchFlowGenerator;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.LocalizedDestinationSelector;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.UniformDestinationSelector;
import edu.usc.enl.cacheflow.processor.rule.aggregator.*;
import edu.usc.enl.cacheflow.processor.rule.aggregator.liu.ACLCompressionProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.liu.TCAMCompressionProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.patch.PatchMergeProcessor;
import edu.usc.enl.cacheflow.processor.rule.generator.ClassBenchRuleGenerator;
import edu.usc.enl.cacheflow.processor.statistics.PartitionStatisticsProcessor;
import edu.usc.enl.cacheflow.processor.topology.tree.FatTreeTopologyGenerator;
import edu.usc.enl.cacheflow.processor.topology.tree.TreeTemplate;
import edu.usc.enl.cacheflow.processor.topology.tree.TreeTopologyGenerator;
import edu.usc.enl.cacheflow.ui.action.CreatePartitionsAction;
import edu.usc.enl.cacheflow.ui.action.NeedTabAction;
import edu.usc.enl.cacheflow.ui.action.file.OpenAction;
import edu.usc.enl.cacheflow.ui.action.file.SaveAction;
import edu.usc.enl.cacheflow.ui.action.file.SaveAsAction;
import edu.usc.enl.cacheflow.ui.action.flow.ClassBenchFlowGenerationAction;
import edu.usc.enl.cacheflow.ui.action.flow.RandomFlowGenerationAction;
import edu.usc.enl.cacheflow.ui.action.placement.PlaceAction;
import edu.usc.enl.cacheflow.ui.action.rule.RandomRuleAction;
import edu.usc.enl.cacheflow.ui.action.statistics.PlacementStatAction;
import edu.usc.enl.cacheflow.ui.action.view.DrawPartitionsAction;
import edu.usc.enl.cacheflow.ui.action.view.DrawRulesAction;
import edu.usc.enl.cacheflow.ui.action.view.DrawRunningTopologyAction;
import edu.usc.enl.cacheflow.ui.action.view.DrawTopologyAction;
import edu.usc.enl.cacheflow.util.Util;

import javax.swing.*;
import javax.swing.filechooser.FileFilter;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.StringReader;
import java.util.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/28/11
 * Time: 11:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class MainWindow extends JFrame {
    private JTabbedPane tabbedPane;
    private MainWindow thisFrame;

    private List<JFrame> subWindows = new LinkedList<JFrame>();

    public MainWindow() throws HeadlessException {
        super("CacheFlow Simulator");
        thisFrame = this;
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        createMenu();

        tabbedPane = new JTabbedPane();
        this.getContentPane().add(tabbedPane);


        pack();
    }

    private void createMenu() {
        JMenuBar menuBar = new JMenuBar();
        this.setJMenuBar(menuBar);

        ////////////////// file menu
        createFileMenu(menuBar);

        /////////////////////////////// Rule menu

        createRuleMenu(menuBar);

        /////////////////////////////// Partition menu

        createPartitionMenu(menuBar);

        /////////////////////////////// Topology menu
        createTopologyMenu(menuBar);

        /////////////////////////////// Traffic menu
        createFlowMenu(menuBar);

        ////////////////////////////////// Placement menu
        createPlacementMenu(menuBar);

        ///////////////////////////////// Statistics menu
        createStatisticsMenu(menuBar);

        /////////////////////////////// View menu

        createViewMenu(menuBar);

        /////////////////////////////// Help menu

        createHelpMenu(menuBar);

    }

    private void createStatisticsMenu(JMenuBar menuBar) {
        JMenu statisticsMenu = new JMenu("Statistics");
        menuBar.add(statisticsMenu);
        statisticsMenu.setMnemonic(KeyEvent.VK_S);

        JMenuItem ruleSpaceStatMenu = new JMenuItem("Rule Space");
        statisticsMenu.add(ruleSpaceStatMenu);
        ruleSpaceStatMenu.setMnemonic(KeyEvent.VK_R);

        JMenuItem optimumTrafficStatMenu = new JMenuItem("Optimum Traffic");
        statisticsMenu.add(optimumTrafficStatMenu);
        optimumTrafficStatMenu.setMnemonic(KeyEvent.VK_O);

        JMenuItem placementStatMenu = new JMenuItem("Placement");
        statisticsMenu.add(placementStatMenu);
        placementStatMenu.setMnemonic(KeyEvent.VK_P);

        ruleSpaceStatMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                final List<Partition> partitions = new PartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>()).
                        create(new BufferedReader(new StringReader(selectedTab.getData())), new HashMap<String, Object>(), new LinkedList<Partition>());
                mainWindow.createTab(null, new PartitionStatisticsProcessor(partitions, new HashMap<String, Object>()).run().toString());
            }
        });
        placementStatMenu.addActionListener(new PlacementStatAction(this));
    }

    private void createPlacementMenu(JMenuBar menuBar) {
        JMenu placementMenu = new JMenu("Placement");
        menuBar.add(placementMenu);
        placementMenu.setMnemonic(KeyEvent.VK_L);

        JMenuItem placeMenu = new JMenuItem("Place");
        placementMenu.add(placeMenu);
        placeMenu.setMnemonic(KeyEvent.VK_P);

        JMenuItem shortestPathPlacementMenu = new JMenuItem("Shortest Path Placement");
        placementMenu.add(shortestPathPlacementMenu);
        shortestPathPlacementMenu.setMnemonic(KeyEvent.VK_S);

        placeMenu.addActionListener(new PlaceAction(this));

        //shortestPathPlacementMenu.addActionListener(new ShortestPathPlacementAction(this);
    }

    private void createFlowMenu(JMenuBar menuBar) {
        JMenu trafficMenu = new JMenu("Flow");
        menuBar.add(trafficMenu);
        trafficMenu.setMnemonic(KeyEvent.VK_F);

        JMenuItem uniformFlowGenerator = new JMenuItem("Generate Uniform Flow");
        trafficMenu.add(uniformFlowGenerator);
        uniformFlowGenerator.setMnemonic(KeyEvent.VK_U);

        JMenuItem uniformEdgeOnlyFlowGenerator = new JMenuItem("Generate Edge Only Flow");
        trafficMenu.add(uniformEdgeOnlyFlowGenerator);
        uniformEdgeOnlyFlowGenerator.setMnemonic(KeyEvent.VK_E);

        JMenuItem localizedFlowGenerator = new JMenuItem("Generate Localized Flow");
        trafficMenu.add(localizedFlowGenerator);
        localizedFlowGenerator.setMnemonic(KeyEvent.VK_L);

        JMenuItem localizedEdgeFlowGenerator = new JMenuItem("Generate Localized Edge Only Flow");
        trafficMenu.add(localizedEdgeFlowGenerator);
        localizedEdgeFlowGenerator.setMnemonic(KeyEvent.VK_O);

        trafficMenu.add(new JSeparator(JSeparator.HORIZONTAL));

        JMenuItem classbenchUniformFlowGenerator = new JMenuItem("Generate Uniform Classbench Flow");
        trafficMenu.add(classbenchUniformFlowGenerator);

        JMenuItem classbenchUniformEdgeOnlyFlowGenerator = new JMenuItem("Generate Edge Only Classbench Flow");
        trafficMenu.add(classbenchUniformEdgeOnlyFlowGenerator);

        JMenuItem classbenchLocalizedFlowGenerator = new JMenuItem("Generate Localized Classbench Flow");
        trafficMenu.add(classbenchLocalizedFlowGenerator);

        JMenuItem classbenchLocalizedEdgeFlowGenerator = new JMenuItem("Generate Localized Edge Only Classbench Flow");
        trafficMenu.add(classbenchLocalizedEdgeFlowGenerator);

        uniformFlowGenerator.addActionListener(new RandomFlowGenerationAction(this) {
            @Override
            protected void doAction(ActionEvent e) throws Exception {
                Map<String, Object> requirements = getFlowGenerationRequirements();
                if (requirements != null) {
                    final Topology topology = (Topology) requirements.get("Topology");
                    parameters2.put("flow.local",false);
                    parameters2.put("flow.edge",false);
                    List<Flow> flows = new RandomFlowGenerator().generate(Util.random, topology.getSwitches(), topology.getSwitches(),
                            DimensionInfo.getTotalRanges((List<DimensionInfo>) requirements.get("RuleSet")),
                            new UniformDestinationSelector(topology), (CustomRandomFlowDistribution) requirements.get("Flow Distribution"));
                    mainWindow.createTab(null, WriterSerializableUtil.getString(flows, parameters2));
                }
            }
        });

        uniformEdgeOnlyFlowGenerator.addActionListener(new RandomFlowGenerationAction(this) {
            @Override
            protected void doAction(ActionEvent e) throws Exception {
                Map<String, Object> requirements = getFlowGenerationRequirements();
                if (requirements != null) {
                    final Topology topology = (Topology) requirements.get("Topology");
                    final List<Switch> switches = topology.findEdges();
                    parameters2.put("flow.local",false);
                    parameters2.put("flow.edge",true);
                    List<Flow> flows = new RandomFlowGenerator().generate(Util.random, switches, switches,
                            DimensionInfo.getTotalRanges((List<DimensionInfo>) requirements.get("RuleSet")),
                            new UniformDestinationSelector(topology), (CustomRandomFlowDistribution) requirements.get("Flow Distribution"));
                    mainWindow.createTab(null, WriterSerializableUtil.getString(flows, parameters2));
                }
            }
        });

        localizedFlowGenerator.addActionListener(new RandomFlowGenerationAction(this) {
            @Override
            protected void doAction(ActionEvent e) throws Exception {
                Map<String, Object> requirements = getFlowGenerationRequirements();
                if (requirements != null) {
                    final Topology topology = (Topology) requirements.get("Topology");
                    final CustomRandomFlowDistribution flowDistribution = (CustomRandomFlowDistribution) requirements.get("Flow Distribution");
                    parameters2.put("flow.local",true);
                    parameters2.put("flow.edge",false);
                    List<Flow> flows = new RandomFlowGenerator().generate(Util.random, topology.getSwitches(), topology.getSwitches(),
                            DimensionInfo.getTotalRanges((List<DimensionInfo>) requirements.get("RuleSet")),
                            new LocalizedDestinationSelector(topology, flowDistribution.getLocalizedFlowDistribution()), flowDistribution);

                    mainWindow.createTab(null, WriterSerializableUtil.getString(flows, parameters2));
                }
            }
        });

        localizedEdgeFlowGenerator.addActionListener(new RandomFlowGenerationAction(this) {
            @Override
            protected void doAction(ActionEvent e) throws Exception {
                Map<String, Object> requirements = getFlowGenerationRequirements();
                if (requirements != null) {
                    final Topology topology = (Topology) requirements.get("Topology");
                    final List<Switch> switches = topology.findEdges();
                    final CustomRandomFlowDistribution flowDistribution = (CustomRandomFlowDistribution) requirements.get("Flow Distribution");
                    parameters2.put("flow.local",true);
                    parameters2.put("flow.edge",true);
                    List<Flow> flows = new RandomFlowGenerator().generate(Util.random, switches, switches,
                            DimensionInfo.getTotalRanges((List<DimensionInfo>) requirements.get("RuleSet")),
                            new LocalizedDestinationSelector(topology, flowDistribution.getLocalizedFlowDistribution()), flowDistribution);
                    mainWindow.createTab(null, WriterSerializableUtil.getString(flows, parameters2));
                }
            }
        });

        /////////////////////////////////////////////////////////////////////////////////////////////////

        classbenchUniformFlowGenerator.addActionListener(new ClassBenchFlowGenerationAction(this) {
            @Override
            protected void doAction(ActionEvent e) throws Exception {
                Map<String, Object> requirements = getFlowGenerationRequiremetns();
                if (requirements != null) {
                    final Topology topology = (Topology) requirements.get("Topology");
                    final List<Switch> switches = topology.getSwitches();
                    final CustomRandomFlowDistribution flowDistribution = (CustomRandomFlowDistribution) requirements.get("Flow Distribution");
                    parameters2.put("flow.local",false);
                    parameters2.put("flow.edge",false);
                    List<Flow> flows = new edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.ClassBenchFlowGenerator().generate(Util.random, switches, switches,
                            (List<String>) requirements.get("Classbench Flows"),
                            new UniformDestinationSelector(topology), flowDistribution);
                    mainWindow.createTab(null, WriterSerializableUtil.getString(flows, parameters2));
                }
            }
        });

        classbenchUniformEdgeOnlyFlowGenerator.addActionListener(new ClassBenchFlowGenerationAction(this) {
            @Override
            protected void doAction(ActionEvent e) throws Exception {
                Map<String, Object> requirements = getFlowGenerationRequiremetns();
                if (requirements != null) {
                    final Topology topology = (Topology) requirements.get("Topology");
                    final List<Switch> switches = topology.findEdges();
                    final CustomRandomFlowDistribution flowDistribution = (CustomRandomFlowDistribution) requirements.get("Flow Distribution");
                    parameters2.put("flow.local",false);
                    parameters2.put("flow.edge",true);
                    List<Flow> flows = new edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.ClassBenchFlowGenerator().generate(Util.random, switches, switches,
                            (List<String>) requirements.get("Classbench Flows"),
                            new UniformDestinationSelector(topology), flowDistribution);
                    mainWindow.createTab(null, WriterSerializableUtil.getString(flows, null));
                }
            }
        });

        classbenchLocalizedFlowGenerator.addActionListener(new ClassBenchFlowGenerationAction(this) {
            @Override
            protected void doAction(ActionEvent e) throws Exception {
                Map<String, Object> requirements = getFlowGenerationRequiremetns();
                if (requirements != null) {
                    final Topology topology = (Topology) requirements.get("Topology");
                    final List<Switch> switches = topology.getSwitches();
                    final CustomRandomFlowDistribution flowDistribution = (CustomRandomFlowDistribution) requirements.get("Flow Distribution");
                    parameters2.put("flow.local",true);
                    parameters2.put("flow.edge",false);
                    List<Flow> flows = new ClassBenchFlowGenerator().generate(Util.random, switches, switches,
                            (List<String>) requirements.get("Classbench Flows"),
                            new LocalizedDestinationSelector(topology, flowDistribution.getLocalizedFlowDistribution()), flowDistribution);
                    mainWindow.createTab(null, WriterSerializableUtil.getString(flows, null));
                }
            }
        });

        classbenchLocalizedEdgeFlowGenerator.addActionListener(new ClassBenchFlowGenerationAction(this) {
            @Override
            protected void doAction(ActionEvent e) throws Exception {
                Map<String, Object> requirements = getFlowGenerationRequiremetns();
                if (requirements != null) {
                    final Topology topology = (Topology) requirements.get("Topology");
                    final List<Switch> switches = topology.findEdges();
                    final CustomRandomFlowDistribution flowDistribution = (CustomRandomFlowDistribution) requirements.get("Flow Distribution");
                    parameters2.put("flow.local",true);
                    parameters2.put("flow.edge",true);
                    List<Flow> flows = new edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.ClassBenchFlowGenerator().generate(Util.random, switches, switches,
                            (List<String>) requirements.get("Classbench Flows"),
                            new LocalizedDestinationSelector(topology, flowDistribution.getLocalizedFlowDistribution()), flowDistribution);
                    mainWindow.createTab(null, WriterSerializableUtil.getString(flows, null));
                }
            }
        });
    }

    private void createTopologyMenu(JMenuBar menuBar) {
        JMenu topologyMenu = new JMenu("Topology");
        menuBar.add(topologyMenu);
        topologyMenu.setMnemonic(KeyEvent.VK_T);

        JMenuItem createTreeMenu = new JMenuItem("Create Tree");
        topologyMenu.add(createTreeMenu);
        createTreeMenu.setMnemonic(KeyEvent.VK_T);
        createTreeMenu.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_T, ActionEvent.CTRL_MASK));

        JMenuItem createFatTreeMenu = new JMenuItem("Create Fat Tree");
        topologyMenu.add(createFatTreeMenu);
        createFatTreeMenu.setMnemonic(KeyEvent.VK_F);


        ///////////////////////////////////////////////////////// Topology
        createTreeMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                TreeTemplate template = new TreeTemplate(selectedTab.getData());
                TreeTopologyGenerator treeTopologyGenerator = new TreeTopologyGenerator(template);
                mainWindow.createTab(null, WriterSerializableUtil.getString(Collections.singletonList(
                        treeTopologyGenerator.generate(Util.DEFAULT_AGGREGATOR,new HashSet<Rule>())), null));
            }
        });

        createFatTreeMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                TreeTemplate template = new TreeTemplate(selectedTab.getData());
                FatTreeTopologyGenerator treeTopologyGenerator = new FatTreeTopologyGenerator(template, 4);
                mainWindow.createTab(null, WriterSerializableUtil.getString(Collections.singletonList(
                        treeTopologyGenerator.generate(Util.DEFAULT_AGGREGATOR,new HashSet<Rule>())), null));
            }
        });
    }

    private void createHelpMenu(JMenuBar menuBar) {
        JMenu helpMenu = new JMenu("Help");
        menuBar.add(helpMenu);
        helpMenu.setMnemonic(KeyEvent.VK_H);

        JMenuItem aboutMenu = new JMenuItem("About");
        helpMenu.add(aboutMenu);
        aboutMenu.setMnemonic(KeyEvent.VK_A);

        JMenuItem helpTopics = new JMenuItem("Help Topics");
        helpMenu.add(helpTopics);
        helpTopics.setMnemonic(KeyEvent.VK_T);
    }

    private void createViewMenu(JMenuBar menuBar) {
        JMenu viewMenu = new JMenu("View");
        menuBar.add(viewMenu);
        viewMenu.setMnemonic(KeyEvent.VK_V);

        JMenuItem drawRulesMenu = new JMenuItem("Draw Rules");
        viewMenu.add(drawRulesMenu);
        drawRulesMenu.setMnemonic(KeyEvent.VK_R);
        drawRulesMenu.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_D, ActionEvent.CTRL_MASK));

        JMenuItem drawPartitionsMenu = new JMenuItem("Draw Partitions");
        viewMenu.add(drawPartitionsMenu);
        drawPartitionsMenu.setMnemonic(KeyEvent.VK_P);
        drawPartitionsMenu.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_F, ActionEvent.CTRL_MASK));

        JMenuItem drawTopologyMenu = new JMenuItem("Draw Topology");
        viewMenu.add(drawTopologyMenu);
        drawTopologyMenu.setMnemonic(KeyEvent.VK_T);

        JMenuItem drawRunningTopologyMenu = new JMenuItem("Draw Running Topology");
        viewMenu.add(drawRunningTopologyMenu);
        drawRunningTopologyMenu.setMnemonic(KeyEvent.VK_L);

        JMenuItem closeAllMenu = new JMenuItem("Close All");
        viewMenu.add(closeAllMenu);
        closeAllMenu.setMnemonic(KeyEvent.VK_C);


        drawRulesMenu.addActionListener(new DrawRulesAction(this));

        drawPartitionsMenu.addActionListener(new DrawPartitionsAction(this));

        drawTopologyMenu.addActionListener(new DrawTopologyAction(this));

        drawRunningTopologyMenu.addActionListener(new DrawRunningTopologyAction(this));

        closeAllMenu.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                for (JFrame subWindow : subWindows) {
                    subWindow.dispose();
                }
            }
        });

    }

    private void createPartitionMenu(JMenuBar menuBar) {
        JMenu partitionMenu = new JMenu("Partition");
        menuBar.add(partitionMenu);
        partitionMenu.setMnemonic(KeyEvent.VK_P);

        JMenuItem calculatePartitionsMenu = new JMenuItem("Calculate Partitions");
        partitionMenu.add(calculatePartitionsMenu);
        calculatePartitionsMenu.setMnemonic(KeyEvent.VK_C);
        calculatePartitionsMenu.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_P, ActionEvent.CTRL_MASK));

        calculatePartitionsMenu.addActionListener(new CreatePartitionsAction(this));
    }

    private void createRuleMenu(JMenuBar menuBar) {
        JMenu ruleMenu = new JMenu("Rule");
        menuBar.add(ruleMenu);
        ruleMenu.setMnemonic(KeyEvent.VK_R);

        JMenuItem randomRuleMenu = new JMenuItem("Generate Random Rules");
        ruleMenu.add(randomRuleMenu);
        randomRuleMenu.setMnemonic(KeyEvent.VK_R);
        randomRuleMenu.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_R, ActionEvent.CTRL_MASK));

        JMenuItem classbenchConvertMenu = new JMenuItem("Convert Classbench format");
        ruleMenu.add(classbenchConvertMenu);
        classbenchConvertMenu.setMnemonic(KeyEvent.VK_C);

        ruleMenu.add(new JSeparator(JSeparator.HORIZONTAL));

        JMenuItem gridMenu = new JMenuItem("Grid");
        ruleMenu.add(gridMenu);
        gridMenu.setMnemonic(KeyEvent.VK_G);

        JMenuItem gridandMergeMenu = new JMenuItem("Grid and Merge");
        ruleMenu.add(gridandMergeMenu);

        JMenuItem semiGridMenu = new JMenuItem("Semi-Grid");
        ruleMenu.add(semiGridMenu);
        semiGridMenu.setMnemonic(KeyEvent.VK_S);


        JMenuItem semiGridAndMergeMenu = new JMenuItem("Semi-Grid and Merge");
        ruleMenu.add(semiGridAndMergeMenu);

        JMenuItem semiGridAndMergeTogetherMenu = new JMenuItem("Semi-Grid and Merge Together");
        ruleMenu.add(semiGridAndMergeTogetherMenu);

        JMenuItem mergeByPatchingMenu = new JMenuItem("Merge by Patching");
        ruleMenu.add(mergeByPatchingMenu);
        mergeByPatchingMenu.setMnemonic(KeyEvent.VK_P);

        JMenuItem aclCompressionMenu = new JMenuItem("ACL Compression");
        ruleMenu.add(aclCompressionMenu);
        aclCompressionMenu.setMnemonic(KeyEvent.VK_A);

        JMenuItem TCAMCompressionMenu = new JMenuItem("TCAM Compression");
        ruleMenu.add(TCAMCompressionMenu);
        TCAMCompressionMenu.setMnemonic(KeyEvent.VK_T);

        randomRuleMenu.addActionListener(new RandomRuleAction(this));

        classbenchConvertMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                mainWindow.createTab(null, WriterSerializableUtil.getString(ClassBenchRuleGenerator.generate(selectedTab.getData3(), Util.random), null));
            }
        });

        gridMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                HashMap<String, Object> parameters = new HashMap<String, Object>();
                final Collection<Rule> rules = new RuleFactory(new FileFactory.EndOfFileCondition()).create(
                        new BufferedReader(new StringReader(selectedTab.getData())), parameters, new LinkedList<Rule>());
                mainWindow.createTab(null, WriterSerializableUtil.getString(new GridProcessor(rules).run(), parameters));
            }
        });


        gridandMergeMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                HashMap<String, Object> parameters = new HashMap<String, Object>();
                final Collection<Rule> rules = new RuleFactory(new FileFactory.EndOfFileCondition()).create(new BufferedReader(
                        new StringReader(selectedTab.getData())), parameters, new LinkedList<Rule>());
                mainWindow.createTab(null, WriterSerializableUtil.getString(new GridAndMergeProcessor(rules).run(), parameters));
            }
        });

        semiGridMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                HashMap<String, Object> parameters = new HashMap<String, Object>();
                final Collection<Rule> rules = new RuleFactory(new FileFactory.EndOfFileCondition()).create(
                        new BufferedReader(new StringReader(selectedTab.getData())), parameters, new LinkedList<Rule>());
                mainWindow.createTab(null, WriterSerializableUtil.getString(new SemiGridProcessor(rules).run(), parameters));
            }
        });

        semiGridAndMergeMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                HashMap<String, Object> parameters = new HashMap<String, Object>();
                final Collection<Rule> rules = new RuleFactory(new FileFactory.EndOfFileCondition()).create(
                        new BufferedReader(new StringReader(selectedTab.getData())), parameters, new LinkedList<Rule>());
                mainWindow.createTab(null, WriterSerializableUtil.getString(new SemiGridAndMergeProcessor(rules).run(), parameters));
            }
        });

        semiGridAndMergeTogetherMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                HashMap<String, Object> parameters = new HashMap<String, Object>();
                final Collection<Rule> rules = new RuleFactory(new FileFactory.EndOfFileCondition()).create(
                        new BufferedReader(new StringReader(selectedTab.getData())), parameters, new LinkedList<Rule>());
                mainWindow.createTab(null, WriterSerializableUtil.getString(new IntegratedSemiGridAndMergeProcessor(rules).run(), parameters));
            }
        });

        mergeByPatchingMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                HashMap<String, Object> parameters = new HashMap<String, Object>();
                final Collection<Rule> rules = new RuleFactory(new FileFactory.EndOfFileCondition()).create(
                        new BufferedReader(new StringReader(selectedTab.getData())), parameters, new LinkedList<Rule>());
                mainWindow.createTab(null, WriterSerializableUtil.getString(new PatchMergeProcessor(rules).run(), parameters));
            }
        });

        aclCompressionMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                HashMap<String, Object> parameters = new HashMap<String, Object>();
                final Collection<Rule> rules = new RuleFactory(new FileFactory.EndOfFileCondition()).create(
                        new BufferedReader(new StringReader(selectedTab.getData())), parameters, new LinkedList<Rule>());
                mainWindow.createTab(null, WriterSerializableUtil.getString(new ACLCompressionProcessor(rules).run(), parameters));
            }
        });
        TCAMCompressionMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                HashMap<String, Object> parameters = new HashMap<String, Object>();
                final Collection<Rule> rules = new RuleFactory(new FileFactory.EndOfFileCondition()).create(
                        new BufferedReader(new StringReader(selectedTab.getData())), parameters, new LinkedList<Rule>());
                mainWindow.createTab(null, WriterSerializableUtil.getString(new TCAMCompressionProcessor(rules).run(), parameters));
            }
        });
    }

    private void createFileMenu(JMenuBar menuBar) {
        JMenu fileMenu = new JMenu("File");
        menuBar.add(fileMenu);
        fileMenu.setMnemonic(KeyEvent.VK_F);

        JMenuItem newMenu = new JMenuItem("New");
        fileMenu.add(newMenu);
        newMenu.setMnemonic(KeyEvent.VK_N);
        newMenu.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_N, ActionEvent.CTRL_MASK));

        JMenuItem openMenu = new JMenuItem("Open");
        fileMenu.add(openMenu);
        openMenu.setMnemonic(KeyEvent.VK_O);
        openMenu.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.CTRL_MASK));

        JMenuItem saveMenu = new JMenuItem("Save");
        fileMenu.add(saveMenu);
        saveMenu.setMnemonic(KeyEvent.VK_S);
        saveMenu.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S, ActionEvent.CTRL_MASK));

        JMenuItem saveAsMenu = new JMenuItem("SaveAs");
        fileMenu.add(saveAsMenu);
        saveAsMenu.setMnemonic(KeyEvent.VK_A);

        JMenuItem closeMenu = new JMenuItem("Close");
        fileMenu.add(closeMenu);
        closeMenu.setMnemonic(KeyEvent.VK_C);

        JMenuItem closeAllMenu = new JMenuItem("CloseAll");
        fileMenu.add(closeAllMenu);
        closeAllMenu.setMnemonic(KeyEvent.VK_C);

        fileMenu.add(new JSeparator());

        JMenuItem exitMenu = new JMenuItem("Exit");
        fileMenu.add(exitMenu);
        exitMenu.setMnemonic(KeyEvent.VK_X);
        exitMenu.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_X, ActionEvent.ALT_MASK));


        newMenu.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                createTab(null, null);

            }
        });

        saveMenu.addActionListener(new SaveAction(this));

        saveAsMenu.addActionListener(new SaveAsAction(this));

        openMenu.addActionListener(new OpenAction(this));

        closeMenu.addActionListener(new NeedTabAction(this) {
            @Override
            protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
                closeSelectedTab();
            }
        });

        closeAllMenu.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                tabbedPane.removeAll();
            }
        });

        exitMenu.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                System.exit(0);
            }
        });
    }

    public void closeSelectedTab() {
        tabbedPane.removeTabAt(tabbedPane.getSelectedIndex());

        //update index
        for (int i = 0; i < tabbedPane.getComponentCount(); i++) {
            ((WorkingPanel) tabbedPane.getComponentAt(i)).setIndex(i);
        }
    }


    public void addSubWindow(JFrame frame) {
        subWindows.add(frame);
    }

    public List<WorkingPanel> getTabList() {
        List<WorkingPanel> openTabs = new LinkedList<WorkingPanel>();
        for (int i = 0; i < tabbedPane.getTabCount(); i++) {
            openTabs.add((WorkingPanel) tabbedPane.getComponentAt(i));
        }
        return openTabs;
    }

    public WorkingPanel getSelectedTab2() {
        WorkingPanel selectedTab = getSelectedTab();
        if (selectedTab != null) {
            return selectedTab;
        } else {
            JOptionPane.showMessageDialog(thisFrame, "No input data", "Error", JOptionPane.ERROR_MESSAGE);
            return null;
        }
    }

    public static File openAFile(JFrame parent) {
        JFileChooser fileChooser = new JFileChooser(Util.getCurrentDirectory());
        FileFilter filter = new FileNameExtensionFilter("Text File (*.txt)", "txt");
        fileChooser.setFileFilter(filter);
        int returnVal = fileChooser.showOpenDialog(parent);
        if (returnVal == JFileChooser.APPROVE_OPTION) {
            return fileChooser.getSelectedFile();
        } else {
            return null;
        }
    }


    public WorkingPanel createTab(String title, String text) {
        if (title == null) {
            title = "Unttitled";
        }
        if (text == null) {
            text = "";
        }
        WorkingPanel newTab = new WorkingPanel(tabbedPane, tabbedPane.getComponentCount());
        newTab.setText(text);
        tabbedPane.add(title, newTab);
        tabbedPane.setSelectedIndex(tabbedPane.getComponentCount() - 1);
        pack();
        return newTab;
    }


    private WorkingPanel getSelectedTab() {
        Component selectedComponent = tabbedPane.getSelectedComponent();
        if (selectedComponent == null) {
            return null;
        }
        return (WorkingPanel) selectedComponent;
    }

    public static void main(String[] args) {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception e) {
            e.printStackTrace();
        }
        MainWindow window = new MainWindow();
        window.setVisible(true);
    }


}
