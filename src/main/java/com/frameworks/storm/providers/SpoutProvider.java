package com.frameworks.storm.providers;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
@Setter
public class SpoutProvider {
  public static final Fields FIELDS = new Fields("strFruit");
  public IBatchSpout createSpout() {
    return new Spout();
  }

  protected static class Spout implements IBatchSpout {
    private static final long serialVersionUID = -3587144552523719158L;

    private String[] strArray = {

            "{\"fruitId\":\"orange1\",\"fruit\":\"orange\",\"color\":\"light orange\",\"weight\": 11}\n",
            "{\"fruitId\":\"orange2\",\"fruit\":\"orange\",\"color\":\"dark orange\",\"weight\": 12}\n",
            "{\"fruitId\":\"orange3\",\"fruit\":\"orange\",\"color\":\"light orange\",\"weight\": 13}\n",
            "{\"fruitId\":\"orange4\",\"fruit\":\"orange\",\"color\":\"dark orange\",\"weight\": 14}\n",
            "{\"fruitId\":\"orange5\",\"fruit\":\"orange\",\"color\":\"orange\",\"weight\": 15}\n",
            "{\"fruitId\":\"apple1\",\"fruit\":\"apple\",\"color\":\"light green\",\"weight\": 1}\n",
            "{\"fruitId\":\"apple2\",\"fruit\":\"apple\",\"color\":\"dark green\",\"weight\": 2}\n",
            "{\"fruitId\":\"apple3\",\"fruit\":\"apple\",\"color\":\"light green\",\"weight\": 3}\n",
            "{\"fruitId\":\"apple4\",\"fruit\":\"apple\",\"color\":\"dark green\",\"weight\": 4}\n",
            "{\"fruitId\":\"apple5\",\"fruit\":\"apple\",\"color\":\"green\",\"weight\": 5}\n",
            "{\"fruitId\":\"pomegranate1\",\"fruit\":\"pomegranate\",\"color\":\"dark red\",\"weight\": 6}\n",
            "{\"fruitId\":\"pomegranate2\",\"fruit\":\"pomegranate\",\"color\":\"light red\",\"weight\": 7}\n",
            "{\"fruitId\":\"pomegranate3\",\"fruit\":\"pomegranate\",\"color\":\"dark red\",\"weight\": 8}\n",
            "{\"fruitId\":\"pomegranate4\",\"fruit\":\"pomegranate\",\"color\":\"light red\",\"weight\": 9}\n",
            "{\"fruitId\":\"pomegranate5\",\"fruit\":\"pomegranate\",\"color\":\"red\",\"weight\": 10}"

    };

    //private final long delay;
    private final int batchSize=14;
    private final int tickerPause =0;
    private boolean emitted = false;

    private int i;

    @Override
    public void open(Map conf, TopologyContext context) {
      i = 0;
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
      String fruitStr = "error";
      if(emitted){return;}

      for (int i = 0; i<= (batchSize-1);i++) {
        fruitStr = strArray[i];
        try{Thread.sleep(tickerPause);}catch(InterruptedException ex){Thread.currentThread().interrupt();}
        collector.emit(new Values(fruitStr));
      }
      emitted=true;
      return;
    }

    @Override
    public void ack(long batchId) {

    }

    @Override
    public void close() {
      i = 0;
    }

    @Override
    public Map getComponentConfiguration() {
      Config conf = new Config();
      conf.setMaxTaskParallelism(1);
      return conf;
    }

    @Override
    public Fields getOutputFields() {
      return FIELDS;
    }
  }
}