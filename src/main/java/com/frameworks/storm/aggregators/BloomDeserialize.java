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
public class BloomDeserialize extends BaseAggregator<String> {
  public final String eventIdField;
  private final String qualifierField;
  private final String valueField;
  private final int bfSize = 10;
  private final double bfProbability = .01;

  @Override
  public void prepare(Map conf, TridentOperationContext context) {

  }

  @Override
  public void cleanup() {

  }


  @Override
  public String init(Object o, TridentCollector tridentCollector) {
    return "string";

  }

  @Override
  public void complete(String eb, TridentCollector tridentCollector) {
    tridentCollector.emit(new Values("string"));
  }

  @Override
  public void aggregate(String eb, TridentTuple tridentTuple, TridentCollector tridentCollector) {

  }

}
