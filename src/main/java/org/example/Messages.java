package org.example;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.*;

public class Messages implements Serializable {
    public final ActorRef sender;

    public Messages(ActorRef sender){
        this.sender = sender;
    }

    public static class StartMessage implements Serializable{
        public final List<ActorRef> group;
        public StartMessage(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    public static class ReadReqMsg extends Messages {
        public ReadReqMsg(ActorRef sender) {
            super(sender);
        }
    }

    public static class WriteReqMsg extends Messages {
        private int v;
        public WriteReqMsg(ActorRef sender, int v) {
            super(sender);
            this.v = v;
        }

        public int getV() {
            return v;
        }
    }

    public static class UpdateMsg extends Messages {
        public final Snapshot snap;
        public UpdateMsg(ActorRef sender, Snapshot asnap) {
            super(sender);
            this.snap = asnap;
        }
    }

    public static class WriteAckMsg extends Messages {
        public final TimeId uid;
        public WriteAckMsg(ActorRef sender, TimeId uid) {
            super(sender);
            this.uid = uid;
        }
    }

    public static class WriteOKMsg extends Messages {
        public final TimeId uid;
        public WriteOKMsg(ActorRef sender, TimeId aid) {
            super(sender);
            this.uid = aid;
        }
    }

    public static class CrashMsg extends Messages {
        public CrashMsg(ActorRef sender) {
            super(sender);
        }
    }

    public static class HeartbeatMsg extends Messages {
        public HeartbeatMsg(ActorRef sender) {
            super(sender);
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
        public final List<ActorData> actorDatas;
        public ElectionMsg(ActorRef sender, List<ActorData> actorUpdates) {
            super(sender);
            this.actorDatas = Collections.unmodifiableList(new ArrayList<ActorData>(actorUpdates));
        }
    }

    public static class ElectionAckMsg extends Messages {
        public ElectionAckMsg(ActorRef sender) {
            super(sender);
        }
    }


    public static class SyncMsg extends Messages {
        public final List<Snapshot> sync;
        public SyncMsg(ActorRef sender, List<Snapshot> syncHistory) {
            super(sender);
            this.sync = syncHistory;
        }
    }







}
