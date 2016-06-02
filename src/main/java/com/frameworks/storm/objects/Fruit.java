package com.frameworks.storm.objects;

/**
 * Created by christian on 5/18/16.
 */
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class Fruit implements Serializable {

    @Getter @Setter
    private String fruitId;

    @Getter @Setter
    private String fruit;

    @Getter @Setter
    private String color;

    @Getter @Setter
    private Integer weight;

}