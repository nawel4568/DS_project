package refactor.Messages;

import akka.actor.ActorRef;
import refactor.TimeoutType;

import java.io.Serializable;
import java.util.*;

public abstract class ReplicaMessages implements Serializable {

    public static class StartReplicaMsg extends ReplicaMessages {
        private final List<ActorRef> group;
        public StartReplicaMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
        public List<ActorRef> getGroup() { return group; }
    }


    public static class UpdateAckMsg extends ReplicaMessages {
        public final Timestamp updateID;
        public UpdateAckMsg(Timestamp uid) {
                        this.updateID = uid;
        }
    }


    public static class ElectionMsg extends ReplicaMessages {}
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
