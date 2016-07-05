package com.frameworks.storm.state.kafka;

import storm.kafka.trident.TridentKafkaState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Created by christiantest on 4/20/16.
 */
public class PermeableKafkaUpdater extends BaseStateUpdater<TridentKafkaState> {
    public PermeableKafkaUpdater() {
    }

    public void updateState(TridentKafkaState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.updateState(tuples, collector);
        for (TridentTuple tuple : tuples) {
            collector.emit(tuple);
        }
    }
}