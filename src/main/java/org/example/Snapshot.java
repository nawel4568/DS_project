package org.example;

import akka.util.DoubleLinkedList;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

public class Snapshot {
    private final TimeId timeId;
    private final Integer v;
    private boolean stable;

    public static Snapshot defaultSnapshot(){
        return new Snapshot(new TimeId(-1,-1), null, true);
    }

    public Snapshot(TimeId timeId, Integer v, boolean stable) {
        this.timeId = timeId;
        this.v = v;
        this.stable = stable;
    }

    public TimeId getTimeId() {
        return timeId;
    }

    public int getV(){ return v; }
    public void setStable(boolean isStable) {
        this.stable=isStable;
    }
    public boolean getStable(){ return stable; }

    @Override
    public int hashCode() {
        return Objects.hash(timeId, v);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Snapshot other = (Snapshot) obj;
        return v == other.v && stable == other.stable && Objects.equals(timeId, other.timeId);
    }

}
