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
        public ReadReqMsg(ActorRef client) {
            super(client);
        }
    }

    public static class WriteReqMsg extends Messages {
        private int v;
        public WriteReqMsg(ActorRef client, int v) {
            super(client);
            this.v = v;
        }

        public int getV() {
            return v;
        }
    }

    public static class UpdateMsg extends Messages {
        public final Snapshot HistoryNode;
        public UpdateMsg(ActorRef client, Snapshot HistoryNode) {
            super(client);
            this.HistoryNode = HistoryNode;
        }
    }

    public static class WriteAckMsg extends Messages {
        public final TimeId uid;
        public WriteAckMsg(ActorRef client, TimeId uid) {
            super(client);
            this.uid = uid;
        }
    }

    public static class ElectionAckMsg extends Messages {
        public final TimeId uid;
        public ElectionAckMsg(ActorRef client, TimeId aid) {
            super(client);
            this.uid = aid;
        }
    }

    public static class WriteOKMsg extends Messages {
        public final TimeId uid;
        public WriteOKMsg(ActorRef client, TimeId aid) {
            super(client);
            this.uid = aid;
        }
    }

    public static class CrashMsg extends Messages {
        public CrashMsg(ActorRef client) {
            super(client);
        }
    }

    public static class HeartbeatMsg extends Messages {
        public HeartbeatMsg(ActorRef client) {
            super(client);
        }
    }

    public static class ElectionMsg extends Messages {
        public final Map<ActorRef, LinkedList<Snapshot>> knownHistories;
        public ElectionMsg(ActorRef client, HashMap<ActorRef, LinkedList<Snapshot>> prevHists) {
            super(client);
            this.knownHistories = Collections.unmodifiableMap(new HashMap<ActorRef, LinkedList<Snapshot>>(prevHists));
        }
    }

    public static class SyncMsg extends Messages {
        public final LinkedList<Snapshot> sync;
        public SyncMsg(ActorRef client, LinkedList<Snapshot> syncHistory) {
            super(client);
            this.sync = syncHistory;
        }
    }







}
