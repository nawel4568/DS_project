package org.example;

import akka.util.DoubleLinkedList;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

public class Snapshot {
    private final TimeId timeId;
    private final int v;
    private boolean stable;
    public Snapshot(TimeId timeId, int v, boolean stable) {
        this.timeId = timeId;
        this.v = v;
        this.stable = stable;
    }
    public TimeId getTimeId() {
        return timeId;
    }
    public int getV(){ return v; }
    public boolean isStable() { return stable; }
    public void setStable(boolean isStable) {
        this.stable=isStable;
    }
    public boolean getStable(){
        return stable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeId, v);
    }
}
