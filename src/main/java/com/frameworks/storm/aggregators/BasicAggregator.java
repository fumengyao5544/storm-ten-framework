package com.frameworks.storm.aggregators;

import backtype.storm.tuple.Values;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

//import java.util.HashMap;
//import java.util.Map;
//import java.util.PriorityQueue;

@Slf4j
@RequiredArgsConstructor
public class BasicAggregator extends BaseAggregator<Integer> {

  Integer fruitCount;

  @Override
  public void prepare(Map conf, TridentOperationContext context) {

  }

  @Override
  public void cleanup() {

  }

  @Override
  public Integer init(Object fruitCount, TridentCollector tridentCollector) {
    return (int) fruitCount;

  }

  @Override
  public void complete(Integer eb, TridentCollector tridentCollector) {
    tridentCollector.emit(new Values("string"));
  }

  @Override
  public void aggregate(Integer eb, TridentTuple tridentTuple, TridentCollector tridentCollector) {

  }

}
