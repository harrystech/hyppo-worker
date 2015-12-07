package com.harrys.hyppo.executor.cli;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Created by jpetty on 12/7/15.
 */
public enum LogStrategy {
    Pipe("PIPE"),
    File("FILE"),
    Null("NULL");

    private final String configName;

    LogStrategy(final String configName){
        this.configName = configName;
    }

    public final String getConfigName(){
        return this.configName;
    }

    public static final LogStrategy fromConfigName(final String value){
        for (final LogStrategy ls : values()){
            if (ls.getConfigName().equalsIgnoreCase(value)){
                return ls;
            }
        }
        final String allowedValues = Arrays.asList(values()).stream().map(LogStrategy::getConfigName).collect(Collectors.joining(", "));
        throw new IllegalArgumentException("Invalid log strategy name: " + value + ". Must be one of: " + allowedValues);
    }
}
