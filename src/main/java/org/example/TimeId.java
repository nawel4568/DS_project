package org.example;

import java.util.Objects;

public class TimeId implements Comparable<TimeId> {
    public final int epoch;
    public int seqNum;

    public TimeId(int aEpoch, int aSeqNum){
        this.epoch = aEpoch;
        this.seqNum = aSeqNum;
    }

    @Override
    public boolean equals(Object obj){
        if (this == obj) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        TimeId timeObj = (TimeId) obj;
        return this.epoch == timeObj.epoch && this.seqNum == timeObj.seqNum;
    }

    @Override
    public int compareTo(TimeId other) {
        if (this.epoch != other.epoch) {
            return Integer.compare(this.epoch, other.epoch);
        } else {
            return Integer.compare(this.seqNum, other.seqNum);
        }
    }

    @Override
    public int hashCode(){
        return Objects.hash(epoch, seqNum);
    }
}
