package refactor.Messages;

import java.util.Objects;

public class Timestamp implements Comparable<Timestamp> {
    private int epoch;
    private int seqNum;

    public static Timestamp defaultTimestamp(){
        return new Timestamp(Integer.MIN_VALUE, Integer.MIN_VALUE);
    }

    public Timestamp(int aEpoch, int aSeqNum){
        this.epoch = aEpoch;
        this.seqNum = aSeqNum;
    }
    public Timestamp(Timestamp other){
        this.epoch = other.epoch;
        this.seqNum = other.seqNum;
    }

    public int getEpoch(){return this.epoch;}
    public int getSeqNum(){return this.seqNum;}
    public Timestamp incrementEpoch(){
        return new Timestamp(this.epoch+1, 0);
    }
    public Timestamp incrementSeqNum(){
        return new Timestamp(this.epoch, this.seqNum+1);
    }

    @Override
    public boolean equals(Object obj){
        if (this == obj) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        Timestamp timeObj = (Timestamp) obj;
        return this.epoch == timeObj.epoch && this.seqNum == timeObj.seqNum;
    }

    @Override
    public int compareTo(Timestamp other) { //return 1 if this > other, -1 otherwise
        if (this.epoch != other.epoch) return Integer.compare(this.epoch, other.epoch);
        else return Integer.compare(this.seqNum, other.seqNum);
    }

    @Override
    public int hashCode(){
        return Objects.hash(epoch, seqNum);
    }

    @Override
    public String toString() {
        return "TimeId{" +
                "epoch=" + epoch +
                ", seqNum=" + seqNum +
                '}';
    }

}

