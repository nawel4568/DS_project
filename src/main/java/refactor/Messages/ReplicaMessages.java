package refactor.Messages;

import akka.actor.ActorRef;
import refactor.TimeoutType;

import java.io.Serializable;
import java.util.*;

public abstract class ReplicaMessages implements Serializable {

    public static class StartMsg extends ReplicaMessages {
        public final List<ActorRef> group;
        public StartMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }


    public static class UpdateAckMsg extends ReplicaMessages {
        public final Timestamp timestamp;
        public UpdateAckMsg(Timestamp timestamp) {
                        this.timestamp = new Timestamp(timestamp);
        }
    }


    public static class ElectionMsg extends ReplicaMessages {
        //This class is a pair replicaID-Update and define an order
        // of these object based on the timestamp. The Sorted set used to store objects of this class
        //keeps as last element the highest element, which is, accordingly to the custom Comparator,
        //the most updated replica: this way, the last element of the sorted set is always the candidate coordinator.
        public static class LocalState implements Comparable<LocalState>{
            public final int replicaID;
            public final Timestamp lastUpdate;
            public final Timestamp lastStable;

            public LocalState(int replicaID, Timestamp lastUpdate, Timestamp lastStable){
                this.replicaID = replicaID;
                this.lastUpdate = new Timestamp(lastUpdate);
                this.lastStable = new Timestamp(lastStable);
            }
            @Override
            public int compareTo(LocalState other){
                int compare = this.lastUpdate.compareTo(other.lastUpdate);
                if(compare != 0)
                    return compare;
                //otherwise compare by last stable
                compare = this.lastStable.compareTo(other.lastStable);
                if(compare != 0)
                    return compare;
                //otherwise compare by replica ID
                return Integer.compare(this.replicaID, other.replicaID);
            }

            @Override
            public String toString(){
                return "LocalState{" +
                        "replicaID=" + replicaID +
                        ", lastUpdate=" + lastUpdate +
                        ", lastStable=" + lastStable +
                        '}';
            }
        }

        public final SortedSet<LocalState> localStates;

        public ElectionMsg(SortedSet<LocalState> localStates){
            this.localStates = Collections.unmodifiableSortedSet(new TreeSet<LocalState>(localStates));
        }

        public static class InconsistentTokenException extends Exception{
            public InconsistentTokenException(SortedSet<LocalState> localStates){
                super("Inconsistent token: First LocalState is: " + localStates.first().toString()
                        + ". Last LocalState is: " + localStates.last().toString());
            }
        }
    }


    public static class ElectionAckMsg extends ReplicaMessages {}

    public static class TimeoutMsg extends ReplicaMessages {
        public final TimeoutType type;
        public TimeoutMsg(TimeoutType type){
                        this.type = type;
        }
    }

}
