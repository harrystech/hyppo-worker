package com.harrys.hyppo.executor.cli;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

/**
 * Created by jpetty on 10/26/15.
 */
public final class TaskSpecificLogging {

    private int taskCounter = 0;

    //  Initial log file does not exist
    private File currentLogFile = null;

    private final Path logPath;

    public TaskSpecificLogging(){
        this.logPath = new File("").getAbsoluteFile().getParentFile().toPath().resolve("log");
    }

    public final File getCurrentLogFile(){
        return this.currentLogFile;
    }

    public final void flushLogStream(){
        System.out.flush();
    }

    public final synchronized void rotateTaskLogFile() throws IOException {
        this.taskCounter++;
        if (!this.logPath.toFile().isDirectory() && !this.logPath.toFile().mkdirs()){
            throw new IllegalStateException("Couldn't create log directory: " + this.logPath.toAbsolutePath().toString());
        }
        this.currentLogFile = logPath.resolve(String.format("task-%05d.out", taskCounter)).toFile();
        if (!this.currentLogFile.createNewFile()){
            throw new IllegalStateException("Failed to create new log output file: " + this.currentLogFile.getAbsolutePath());
        }
        System.out.println("Rotating to new log file: " + this.currentLogFile.getPath());
        System.out.close();
        System.setOut(new PrintStream(new FileOutputStream(this.currentLogFile)));
        System.out.println("Started new task log file: " + currentLogFile.getPath());
    }
}
