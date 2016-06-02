package com.frameworks.storm.operation;

import backtype.storm.tuple.Values;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.*;

@Slf4j
public class Unique implements Aggregator<Set<String>> {
  @Override
  public void prepare(Map conf, TridentOperationContext context) {

  }

  @Override
  public void cleanup() {

  }

  @Override
  public Set<String> init(Object batchId, TridentCollector collector) {
    return new HashSet<String>();
  }

  @Override
  public void aggregate(Set<String> val, TridentTuple tuple, TridentCollector collector) {
    val.add(tuple.getString(0));
  }

  @Override
  public void complete(Set<String> val, TridentCollector collector) {
    for (String key : val) {
      collector.emit(new Values(key));
    }

  }

}
