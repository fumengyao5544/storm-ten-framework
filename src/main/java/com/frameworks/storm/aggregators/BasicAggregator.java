package com.frameworks.storm.aggregators;

import backtype.storm.tuple.Values;
import com.frameworks.storm.objects.FruitCount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

//import java.util.HashMap;
//import java.util.Map;
//import java.util.PriorityQueue;

@Slf4j
@RequiredArgsConstructor
public class BasicAggregator extends BaseAggregator<FruitCount> {

  @Override
  public void prepare(Map conf, TridentOperationContext context) {

  }

  @Override
  public void cleanup() {

  }

  @Override
  public FruitCount init(Object fruitCount, TridentCollector tridentCollector) {
    return new FruitCount();

  }

  @Override
  public void aggregate(FruitCount fruitCount, TridentTuple tridentTuple, TridentCollector tridentCollector) {

    if(fruitCount.isFirstTuple()) fruitCount.setFruit(tridentTuple.getStringByField("fruit")); // Only set name on frist tuple
    fruitCount.setFirstTuple(false);

    fruitCount.setCount(fruitCount.getCount()+1); //aggregate step: how many apples are in this batch?
  }

  @Override
  public void complete(FruitCount fruitCount, TridentCollector tridentCollector) {
    int count = fruitCount.getCount();
    String fruitType = fruitCount.getFruit();
    tridentCollector.emit(new Values(fruitType,count)); //return fruit
  }

}
