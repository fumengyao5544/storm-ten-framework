package com.frameworks.storm.state.custom;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christiangao on 6/14/16.
 */
@Slf4j
public class FruitStateUpdater extends BaseStateUpdater<FruitState> {
    String fruitName;
    String fruitCountName;

    public FruitStateUpdater(String fruitName, String fruitCountName){
        this.fruitName = fruitName;
        this.fruitCountName = fruitCountName;
    }

    public void updateState(FruitState state, List<TridentTuple> tuples, TridentCollector collector) {
        for(TridentTuple t: tuples) {
            state.addFruitsToBasket(t.getStringByField(fruitName),t.getIntegerByField(fruitCountName));
        }
        for (TridentTuple tuple : tuples) { //emit tuples for reuse
            collector.emit(tuple);
        }
    }
}
