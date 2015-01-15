package edu.usc.enl.cacheflow.util;

import java.io.File;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DefaultLogger;
import org.apache.tools.ant.DemuxOutputStream;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.Echo;
import org.apache.tools.ant.taskdefs.Java;
import org.apache.tools.ant.types.Commandline;
import org.apache.tools.ant.types.Path;

public class LaunchAJVM {

    public boolean run(Class className, String vmParams, String params) {

        Project project = new Project();
        project.setBaseDir(new File(System.getProperty("user.dir")));
        project.init();
        DefaultLogger logger = new DefaultLogger();
        project.addBuildListener(logger);
        PrintStream oldOut = System.out;
        PrintStream oldErr = System.err;
        logger.setOutputPrintStream(System.out);
        logger.setErrorPrintStream(System.err);
        logger.setMessageOutputLevel(Project.MSG_INFO);
        System.setOut(new PrintStream(new DemuxOutputStream(project, false)));
        System.setErr(new PrintStream(new DemuxOutputStream(project, true)));
        project.fireBuildStarted();

        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader) cl).getURLs();

        System.out.println("running");
        Throwable caught = null;
        int ret=1;
        try {
            Echo echo = new Echo();
            echo.setTaskName("Echo");
            echo.setProject(project);
            echo.init();
            echo.setMessage("Launching a new task");
            echo.execute();

            Java javaTask = new Java();
            javaTask.setTaskName("runjava");
            javaTask.setProject(project);
            javaTask.setFork(true);
            javaTask.setFailonerror(true);
            javaTask.setClassname(className.getName());
            Path path = new Path(project);
            for (URL url : urls) {
                path.add(new Path(project, url.getPath()));
            }
            javaTask.setClasspath(path);

            // add some vm args
            Commandline.Argument jvmArgs = javaTask.createJvmarg();
            jvmArgs.setLine(vmParams);

            // added some args for to class to launch
            Commandline.Argument taskArgs = javaTask.createArg();
            taskArgs.setLine(params);


            javaTask.init();
            ret = javaTask.executeJava();
            System.out.println("java task return code: " + ret);

        } catch (BuildException e) {
            caught = e;
            return false;
        } finally {
            project.log("finished");
            project.fireBuildFinished(caught);
            System.setOut(oldOut);
            System.setErr(oldErr);
        }
        return ret==0;
    }

    private String toString(String[] params) {
        StringBuilder sb = new StringBuilder();
        for (String vmParam : params) {
            sb.append(vmParam).append(" ");
        }
        return sb.toString();
    }
}
