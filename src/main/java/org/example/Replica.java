package org.example;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.io.Serializable;


public class Replica extends AbstractActor {
    private final int replicaId;
    private Integer value;
    private TimeId timestamp;

    public static Props props(int replicaId) {
        return Props.create(Replica.class, () -> new Replica(replicaId));
    }

    public Replica(int replicaId) {
        this.replicaId = replicaId;
        this.timestamp = new TimeId(0,0);
        this.value = null;
    }

    @Override
    public Receive createReceive() {
        return replicaBehavior();
    }

    public Receive replicaBehavior() {
        return receiveBuilder()
                .match(Messages.ReadReqMsg.class, this::onReadReqMsg)
                .match(Messages.WriteReqMsg.class, this::onWriteReqMsg)
                .match(Messages.UpdateMsg.class, this::onUpdateMsg)
                .match(Messages.WriteOKMsg.class, this::onWriteOKMsg)
                .match(Messages.ElectionMsg.class, this::onElectionMsg)
                .match(Messages.HeartbeatMsg.class, this::onHeartbeatMsg)
                .build();
    }

    public Receive replicaDuringElectionBehavior() {
        // Define behavior for election state
        return receiveBuilder()
                .match(Messages.ElectionMsg.class, this::onElectionMsg)
                .match(Messages.ReadReqMsg.class, this::onReadReqMsg)
                .match(Messages.WriteReqMsg.class, this::onWriteDuringElectionMsg)
                .match(Messages.SyncMsg.class, this::onSyncMsg)
                .build();
    }

    public Receive coordinatorBehavior() {
        // Define behavior for coordinator state
        return receiveBuilder()
                //.match(Messages.CoordinatorMsg.class, this::onCoordinatorMsg)
                .build();
    }

    public Receive crashedBehavior() {
        return receiveBuilder()
                .matchAny(msg -> {}) // Ignore all messages when crashed
                .build();
    }

    public void crash(int interval) {
        getContext().become(crashedBehavior());
        // Optionally handle the duration for which the actor should remain in the crashed state
    }

    public void onReadReqMsg(Messages.ReadReqMsg req) {
        // Handle ReadReqMsg
    }

    public void onWriteReqMsg(Messages.WriteReqMsg req) {
        // Handle WriteReqMsg
    }

    public void onWriteDuringElectionMsg(Messages.WriteReqMsg req) {
        //Handle WriteReqMsg during election
    }

    public void onUpdateMsg(Messages.UpdateMsg msg) {
        // Handle UpdateMsg
    }

    public void onWriteOKMsg(Messages.WriteOKMsg msg) {
        // Handle WriteOKMsg
    }

    public void onElectionMsg(Messages.ElectionMsg msg) {
        // Handle ElectionMsg
    }

    public void onHeartbeatMsg(Messages.HeartbeatMsg msg) {
        // Handle HeartbeatMsg
    }

    public void onSyncMsg(Messages.SyncMsg msg) {
        // Handle SyncMsg
    }

}
