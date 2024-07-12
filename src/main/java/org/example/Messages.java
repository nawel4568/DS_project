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
        public static final class ActorData{
            public final ActorRef replicaRef;
            public final int actorId;
            public final Snapshot lastUpdate;


            public ActorData(ActorRef replicaRef, int actorId, Snapshot lastUpdate) {
                this.replicaRef = replicaRef;
                this.actorId = actorId;
                this.lastUpdate = lastUpdate;
            }
        }
        public final int epochNumber;
        public final List<ActorData> actorDatas;

        public ElectionMsg(int epochNum, List<ActorData> actorUpdates) {
            this.epochNumber = epochNum;
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
