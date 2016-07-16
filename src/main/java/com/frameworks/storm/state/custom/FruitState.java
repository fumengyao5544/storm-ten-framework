package com.frameworks.storm.state.custom;

import storm.trident.state.State;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christiangao on 6/14/16.
 */
public class FruitState implements State {

    Map <String,Integer> fruitbasket = new HashMap<String,Integer>();

    public void beginCommit(Long txid) {
    }

    public void commit(Long txid) {
    }

    public void addFruitsToBasket(String fruit,int fruitNum) {
        int basketcount = fruitbasket.containsKey(fruit) ? fruitbasket.get(fruit) : 0; //This is just a simple if else statement
        fruitbasket.put(fruit, basketcount + fruitNum);
    }

    public void batchAddToBasket(List<String> Fruits) {
        for(String fruit : Fruits){
            int basketcount = fruitbasket.containsKey(fruit) ? fruitbasket.get(fruit) : 0;
            fruitbasket.put(fruit, basketcount + 1);
        }
    }

    public int query(String fruit) {
        return fruitbasket.containsKey(fruit) ? fruitbasket.get(fruit) : 0;
    }

    public List<Integer> batchQuery(List<String> Fruits) {

        List<Integer> fruitCounts = new ArrayList<Integer>();
        for(String fruit : Fruits){
            int basketcount = fruitbasket.containsKey(fruit) ? fruitbasket.get(fruit) : 0;
            fruitCounts.add(basketcount);
        }
        return fruitCounts;

    }

}
