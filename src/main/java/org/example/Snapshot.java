package org.example;

import akka.util.DoubleLinkedList;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class Snapshot {
    private final TimeId timeId;
    private final int v;
    public Snapshot(TimeId timeId, int v) {
        this.timeId = timeId;
        this.v = v;
    }
    public TimeId getTimeId() {
        return timeId;
    }

}
