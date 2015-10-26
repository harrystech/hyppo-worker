package com.harrys.hyppo.executor.cli;

import java.io.*;

/**
 * Created by jpetty on 10/26/15.
 */
public final class TaskSpecificLogging {

    private int taskCounter = 0;

    //  Initial log file does not exist
    private File currentLogFile = null;

    public TaskSpecificLogging(){

    }

    public final File getCurrentLogFile(){
        return this.currentLogFile;
    }

    public final void flushLogStream(){
        System.out.flush();
    }

    public final synchronized void rotateTaskLogFile() throws IOException {
        this.taskCounter++;
        this.currentLogFile = new File(String.format("task-%5d", taskCounter));
        System.out.println("Rotating to new log file: " + this.currentLogFile.getPath());
        System.out.close();
        System.setOut(new PrintStream(new FileOutputStream(this.currentLogFile)));
        System.out.println("Started new task log file: " + currentLogFile.getPath());
    }
}
