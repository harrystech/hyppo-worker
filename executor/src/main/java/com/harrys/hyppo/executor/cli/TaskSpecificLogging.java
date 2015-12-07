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

    private final LogStrategy logStrategy;

    private int taskCounter = 0;

    private final Path logPath;

    public TaskSpecificLogging(final LogStrategy logStrategy){
        this.logStrategy = logStrategy;
        if (logStrategy == LogStrategy.File){
            this.logPath  = new File("").getAbsoluteFile().getParentFile().toPath().resolve("log");
        } else {
            this.logPath  = null;
        }
    }

    public final void flushLogStream(){
        System.out.flush();
    }

    public final synchronized void rotateTaskLogFile() throws IOException {
        this.taskCounter++;
        if (this.logStrategy == LogStrategy.File && this.logPath != null){
            if (!this.logPath.toFile().isDirectory() && !this.logPath.toFile().mkdirs()){
                throw new IllegalStateException("Couldn't create log directory: " + this.logPath.toAbsolutePath().toString());
            }
            final File currentLogFile = logPath.resolve(String.format("task-%05d.out", taskCounter)).toFile();
            if (!currentLogFile.createNewFile()){
                throw new IllegalStateException("Failed to create new log output file: " + currentLogFile.getAbsolutePath());
            }
            System.out.println("Rotating to new log file: " + currentLogFile.getPath());
            System.out.close();
            System.setOut(new PrintStream(new FileOutputStream(currentLogFile)));
            System.out.println("Started new task log file: " + currentLogFile.getPath());
        }
    }
}
