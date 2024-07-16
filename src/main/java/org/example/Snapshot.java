package org.example;

import java.util.Objects;

public class Snapshot {
    private final TimeId timeId;
    private final Integer v;
    private boolean stable;

    public static Snapshot defaultSnapshot(){
        return new Snapshot(new TimeId(Integer.MIN_VALUE,Integer.MIN_VALUE), null, true);
    }

    public Snapshot(TimeId timeId, Integer v, boolean stable) {
        this.timeId = timeId;
        this.v = v;
        this.stable = stable;
    }

    public TimeId getTimeId() {
        return this.timeId;
    }

    public int getV(){ return this.v; }
    public void setStable(boolean stable) {
        this.stable = stable;
    }
    public boolean isStable(){ return this.stable; }

    @Override
    public int hashCode() {
        return Objects.hash(timeId, v);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        Snapshot other = (Snapshot) obj;
        return Objects.equals(this.v, other.v) && this.stable == other.stable && Objects.equals(this.timeId, other.timeId);
    }

    @Override
    public String toString() {
        return "Snapshot{" +
                "timeId=" + timeId +
                ", v=" + v +
                ", stable=" + stable +
                '}';
    }


}
