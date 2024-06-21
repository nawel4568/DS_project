package org.example;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Messages implements Serializable {
    public final ActorRef client;

    public Messages(ActorRef client){
        this.client = client;
    }

    public static class ReadReqMsg extends Messages {
        public ReadReqMsg(ActorRef client) {
            super(client);
        }
    }

    public static class WriteReqMsg extends Messages {
        public WriteReqMsg(ActorRef client) {
            super(client);
        }
    }

    public static class UpdateMsg extends Messages {
        public final int newVal;
        public final TimeId uid;
        public UpdateMsg(ActorRef client, int aNewVal, TimeId aid) {
            super(client);
            this.newVal = aNewVal;
            this.uid = aid;
        }
    }

    public static class WriteAckMsg extends Messages {
        public final TimeId uid;
        public WriteAckMsg(ActorRef client, TimeId aid) {
            super(client);
            this.uid = aid;
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
        public final int newVal;
        public WriteOKMsg(ActorRef client, int aNewVal) {
            super(client);
            this.newVal = aNewVal;
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
        public final Map<ActorRef, History> knownHistories;
        public ElectionMsg(ActorRef client, HashMap<ActorRef, History> prevHists) {
            super(client);
            this.knownHistories = Collections.unmodifiableMap(new HashMap<ActorRef, History>(prevHists));
        }
    }

    public static class SyncMsg extends Messages {
        public final History sync;
        public SyncMsg(ActorRef client, History syncHistory) {
            super(client);
            this.sync = syncHistory;
        }
    }







}
