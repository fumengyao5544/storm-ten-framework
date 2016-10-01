package com.frameworks.storm.operation;

import backtype.storm.tuple.Values;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

@Slf4j
public class KafkaFieldGenerator extends BaseFunction{

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        //System.out.println("TEST: "+rowKey);
        //log.info();
        collector.emit(new Values("key",tuple.getStringByField("str")));

    }
}
