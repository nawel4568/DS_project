package org.example;

import java.util.HashMap;
import java.util.Map;

public class History {
    private final Map<TimeId, Integer> histMap;

    public History(){
        this.histMap = new HashMap<TimeId, Integer>();
    }

    public Integer getUpdate(TimeId key){
        return histMap.getOrDefault(key, null);
    }

    public Integer setUpdate(TimeId id, Integer val){
        histMap.put(id, val);
        return val;
    }
}
