package com.frameworks.storm.state.hbase.standard;

import java.util.List;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class HBaseStandardStateUpdater extends BaseStateUpdater<HBaseStandardState> {
    public HBaseStandardStateUpdater() {
    }

    public void updateState(HBaseStandardState hBaseState, List<TridentTuple> tuples, TridentCollector collector) {
        hBaseState.updateState(tuples, collector);
    }
}