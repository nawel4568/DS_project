package org.example;

import java.util.Objects;

public class TimeId {
    public final int epoch;
    public final int seqNum;

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
    public int hashCode(){
        return Objects.hash(epoch, seqNum);
    }
}
