package com.frameworks.storm.objects;

/**
 * Created by christian on 5/18/16.
 */
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class FruitCount implements Serializable {

    @Getter @Setter
    private String fruit;

    @Getter @Setter
    private Integer count = 0;

    @Getter @Setter
    private boolean firstTuple = true;

}