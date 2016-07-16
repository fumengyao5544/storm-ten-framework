package com.frameworks.storm.state.custom;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christiangao on 6/14/16.
 */
public class FruitStateQuery extends BaseQueryFunction<FruitState, Integer> {

    String fruitName;
    public FruitStateQuery(String fruitName) {
        this.fruitName = fruitName;
    }

    public List<Integer> batchRetrieve(FruitState state, List<TridentTuple> inputs) {
        List<String> fruitCountList = new ArrayList<String>();
        for(TridentTuple input: inputs) {
            fruitCountList.add(input.getStringByField(fruitName));
        }
        return state.batchQuery(fruitCountList);
    }

    public void execute(TridentTuple tuple, Integer prediction, TridentCollector collector) {
        collector.emit(new Values(prediction));
    }
}
