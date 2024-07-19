package org.example;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.*;

public abstract class Messages implements Serializable {

    public static class PrintHistoryMsg extends Messages{}
    
    public static class StartMessage extends Messages{
        public final List<ActorRef> group;
        public StartMessage(List<ActorRef> group) {
                        this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
        public List<ActorRef> getGroup() { return group; }
    }

    public static class ReadReqMsg extends Messages {
        public ReadReqMsg() {}
    }

    public static class WriteReqMsg extends Messages {
        private final int v;
        public WriteReqMsg(int v) {
                        this.v = v;
        }
        public int getV() {
            return v;
        }
    }

    public static class UpdateMsg extends Messages {
        public final Snapshot snap;
        public UpdateMsg(Snapshot aSnap) {
                        this.snap = aSnap;
        }
    }

    public static class UpdateAckMsg extends Messages {
        public final TimeId updateID;
        public UpdateAckMsg(TimeId uid) {
                        this.updateID = uid;
        }
    }

    public static class WriteOKMsg extends Messages {
        public final TimeId updateID;
        public WriteOKMsg(TimeId aid) {
                        this.updateID = aid;
        }
    }

    //public static class CrashMsg extends Messages {
    //    public CrashMsg() {}
    //}

    public static class HeartbeatMsg extends Messages {
        public HeartbeatMsg() {}
    }

    /*public static class ElectionMsg extends Messages {
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
                //compare the Priority of 2 messages.
                // if THIS has more priority return 1, if OTHER has more priority return -1.

                if (this.initiatorID != other.initiatorID) {
                    // Return 1 if this initiatorID is less (higher priority), otherwise return -1
                    return Integer.compare(other.initiatorID, this.initiatorID);
                }
                // If initiatorIDs are equal, compare locCounter
                return Integer.compare(this.locCounter, other.locCounter);

                /*
                // If this initiatorID is less than the other's initiatorID, this has higher priority
                if (this.initiatorID < other.initiatorID) return 1;
                // If this initiatorID is greater than the other's initiatorID, other has higher priority
                else // If this locCounter is less than the other's locCounter, other has higher priority
                    // If both initiatorID and locCounter are equal, they have equal priority (not possible)
                    if (this.initiatorID > other.initiatorID) return -1;
                // If initiatorIDs are equal, compare locCounter
                else
                    // If this locCounter is greater than the other's locCounter, this has higher priority
                        return Integer.compare(this.locCounter, other.locCounter); // if this.locCounter > other.locCounter then return 1
                                                                                    // if this.locCounter < other.locCoutner then return -1
                                                                                    // if this.locCounter == other.locCoutner then return 0

            }
        } */

    public static class ElectionMsg extends Messages {

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


    }

    public static class ElectionAckMsg extends Messages {
        public ElectionAckMsg() {}
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

}
