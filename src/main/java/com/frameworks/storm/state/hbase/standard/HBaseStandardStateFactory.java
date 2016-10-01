package com.frameworks.storm.state.hbase.standard;

import backtype.storm.task.IMetricsContext;
import java.util.Map;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

public class HBaseStandardStateFactory implements StateFactory {
    private HBaseStandardState.Options options;

    public HBaseStandardStateFactory(HBaseStandardState.Options options) {
        this.options = options;
    }

    public State makeState(Map map, IMetricsContext iMetricsContext, int partitionIndex, int numPartitions) {
        HBaseStandardState state = new HBaseStandardState(map, partitionIndex, numPartitions, this.options);
        state.prepare();
        return state;
    }
}