package com.harrys.hyppo.executor.cli;

/**
 * Created by jpetty on 7/21/15.
 */
public final class ExecutorMain {

    public static void main(String[] args){
        //  Option values
        Integer commanderPort   = null;
        String  integrationName = null;

        //  Parse the command line options
        try {
            final String portVal = System.getProperty("executor.workerPort");
            try {
                commanderPort = Integer.parseInt(portVal);
            } catch (NumberFormatException nfe){
                throw new IllegalArgumentException("Invalid port number value: " + portVal, nfe);
            }
            integrationName = System.getProperty("executor.integrationClass");
        } catch (Exception e){
            System.err.println("Failed to parse executor options:\n" + e.getMessage());
            System.exit(1);
        }

        final ExecutorCommandLoop looper = new ExecutorCommandLoop(commanderPort, integrationName);

        try {
            looper.initializeIntegration();
        } catch (Exception e){
            System.err.println("Failure initializing integration instance: " + e.toString());
            e.printStackTrace(System.err);
            System.err.flush();
            System.exit(1);
        }

        try {
            looper.runUntilExitCommand();
            //  This is put here to prevent non-daemon background threads from keeping the executor
            //  alive unnecessarily.
            System.exit(0);
        } catch (Exception e){
            System.err.println("Failure inside executor loop: " + e.toString());
            e.printStackTrace(System.err);
            System.err.flush();
            System.exit(1);
        }
    }
}
