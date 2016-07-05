package com.frameworks.storm.state.custom;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Created by christiangao on 6/15/16.
 */

public class FruitStateFactory implements StateFactory {

    String init;
    public FruitStateFactory(String init){
        this.init = init;
    }

    public State makeState(Map conf, IMetricsContext metricsContext,int partitionIndex, int numPartitions) {
        return new FruitState();
    }
}