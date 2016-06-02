package com.frameworks.storm.state.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.hbase.trident.state.HBaseState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

@Slf4j
public class PermeableHBaseUpdater extends BaseStateUpdater<HBaseState> {

  @Override
  public void updateState(HBaseState hBaseState, List<TridentTuple> tuples, TridentCollector collector) {
    hBaseState.updateState(tuples, collector);

    for (TridentTuple tuple : tuples) {
          collector.emit(tuple);
    }
  }
}
