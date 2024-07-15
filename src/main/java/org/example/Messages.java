package org.example;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.*;

public abstract class Messages implements Serializable {
    
    public static class StartMessage extends Messages{
        public final List<ActorRef> group;
        public StartMessage(List<ActorRef> group) {
                        this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }

        public List<ActorRef> getGroup() {
            return group;
        }
    }

    public static class ReadReqMsg extends Messages {
        public ReadReqMsg() {

                    }
    }

    public static class WriteReqMsg extends Messages {
        private int v;
        public WriteReqMsg(int v) {
                        this.v = v;
        }

        public int getV() {
            return v;
        }
    }

    public static class UpdateMsg extends Messages {
        public final Snapshot snap;
        public UpdateMsg(Snapshot asnap) {
                        this.snap = asnap;
        }
    }

    public static class UpdateAckMsg extends Messages {
        public final TimeId uid;
        public UpdateAckMsg(TimeId uid) {
                        this.uid = uid;
        }
    }

    public static class WriteOKMsg extends Messages {
        public final TimeId uid;
        public WriteOKMsg(TimeId aid) {
                        this.uid = aid;
        }
    }

    public static class CrashMsg extends Messages {
        public CrashMsg() {
                    }
    }

    public static class HeartbeatMsg extends Messages {
        public HeartbeatMsg() {
                    }
    }

    public static class ElectionMsg extends Messages {
        public static class ElectionID{ //class for distinguishing different instances of the protocol within an epoch
            public final int initiatorID; //ref of the initiator
            public final int locCounter; //local counter (of the initiator) of the number of election instances initiated

            public static ElectionID defaultElectionID(){ //ID that will always be preempted by other IDs in priority comparison
                return new ElectionID(Integer.MAX_VALUE,  Integer.MIN_VALUE);
            }
            public ElectionID(int init, int counter){
                this.initiatorID = init;
                this.locCounter = counter;
            }

            public int comparePriority(ElectionID other) {
                // If this initiatorID is less than the other's initiatorID, this has higher priority
                if (this.initiatorID < other.initiatorID) return 1;
                // If this initiatorID is greater than the other's initiatorID, other has higher priority
                else if (this.initiatorID > other.initiatorID) return -1;
                // If initiatorIDs are equal, compare locCounter
                else
                    // If this locCounter is greater than the other's locCounter, this has higher priority
                    if (this.locCounter > other.locCounter) return 1;
                    // If this locCounter is less than the other's locCounter, other has higher priority
                    else if (this.locCounter < other.locCounter) return -1;
                    // If both initiatorID and locCounter are equal, they have equal priority (not possible)
                    else return 0;

            }
        }
        public static final class ActorData{ //class for exchanging data about the replicas
            public final ActorRef replicaRef;
            public final int actorId;
            public final Snapshot lastUpdate;


            public ActorData(ActorRef replicaRef, int actorId, Snapshot lastUpdate) {
                this.replicaRef = replicaRef;
                this.actorId = actorId;
                this.lastUpdate = lastUpdate;
            }
        }
        public final List<ActorData> actorDatas;
        public final ElectionID instanceID;

        public ElectionMsg(ElectionID instanceID, List<ActorData> actorUpdates) {
            //this.id = id;
            this.instanceID = instanceID;
            this.actorDatas = Collections.unmodifiableList(new ArrayList<ActorData>(actorUpdates));
        }


    }

    public static class ElectionAckMsg extends Messages {
        public ElectionAckMsg() {
                    }
    }


    public static class SyncMsg extends Messages {
        public final Queue<Snapshot> sync;
        public SyncMsg(Queue<Snapshot> syncHistory) {
                        this.sync = syncHistory;
        }
    }

    public static class TimeoutMsg extends Messages{
        public final Replica.TimeoutType type;
        public TimeoutMsg(Replica.TimeoutType type){
                        this.type = type;
        }
    }


    public static class MessageDebugging extends Messages{

    }




}
