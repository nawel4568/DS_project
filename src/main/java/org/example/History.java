package org.example;

import akka.util.DoubleLinkedList;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class History {
    private class HistoryNode {
        private TimeId timeId;
        private int v;
        public HistoryNode(TimeId timeId, int v) {
            this.timeId = timeId;
            this.v = v;
        }
    }
    private final LinkedList<HistoryNode> hist;


    public History(){
        this.hist = new LinkedList<HistoryNode>();
    }

    /** public Integer getUpdate(TimeId key){
        return histMap.getOrDefault(key, null);
    }

    public Integer setUpdate(TimeId id, Integer val){
        histMap.put(id, val);
        return val;
    } **/
}
