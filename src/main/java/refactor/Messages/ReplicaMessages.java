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
        public final Timestamp updateID;
        public UpdateAckMsg(Timestamp uid) {
                        this.updateID = uid;
        }
    }


    public static class ElectionMsg extends ReplicaMessages {
        //This class is a pair replcaID-Update and define an order
        // of these object based on the timestamp. The Sorted set used to store objects of this class
        //keeps as last element the highest element, which is, accordingly to the custom Comparator,
        //the most updated replica: this way, the last element of the sorted set is always the candidate coordinator.
        public static class ReplicaUpdate implements Comparable<ReplicaUpdate>{
            public final int replicaID;
            public final Timestamp lastUpdate;
            public final Timestamp lastStable;

            public ReplicaUpdate(int replicaID, Timestamp lastUpdate, Timestamp lastStable){
                this.replicaID = replicaID;
                this.lastUpdate = lastUpdate;
                this.lastStable = lastStable;
            }
            @Override
            public int compareTo(ReplicaUpdate other){
                return this.lastUpdate.compareTo(other.lastUpdate);
            }
            @Override
            public String toString(){
                return "ReplicaUpdate{" +
                        "replicaID=" + replicaID +
                        ", lastUpdate=" + lastUpdate +
                        '}';
            }
        }

        public final SortedSet<ReplicaUpdate> replicaUpdates;

        public ElectionMsg(SortedSet<ReplicaUpdate> ReplicaUpdates){
            this.replicaUpdates = new TreeSet<ReplicaUpdate>(ReplicaUpdates);
        }
    }
    /*public static class ElectionMsg extends ReplicaMessages {

        public static final class ActorData{ //class for exchanging data about the replicas
            public final ActorRef replicaRef;
            public final Snapshot lastUpdate;


            public ActorData(ActorRef replicaRef, Snapshot lastUpdate) {
                this.replicaRef = replicaRef;
                this.lastUpdate = lastUpdate;
            }
        }
        public final Map<Integer, ActorData> actorDatas;
        public final Integer candidateCoordinatorID;
        public final Snapshot lastKnownUpdate;

        public ElectionMsg(Integer candidateCoordinatorID, Snapshot lastKnownUpdate, Map<Integer, ActorData> actorUpdates) {
            //this.id = id;
            this.candidateCoordinatorID = candidateCoordinatorID;
            this.lastKnownUpdate = lastKnownUpdate;
            this.actorDatas = Collections.unmodifiableMap(new HashMap<Integer, ActorData>(actorUpdates));
        }


    } */

    public static class ElectionAckMsg extends ReplicaMessages {}

    public static class TimeoutMsg extends ReplicaMessages {
        public final TimeoutType type;
        public TimeoutMsg(TimeoutType type){
                        this.type = type;
        }
    }

}
