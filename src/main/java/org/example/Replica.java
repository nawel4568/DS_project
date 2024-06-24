package org.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;


public class Replica extends AbstractActor {
    private final int replicaId;
    private TimeId timeStamp;
    private List<ActorRef> groupOfReplicas;
    private ActorRef coordinator;
    private History localHistory;
    private Queue<Integer> writeQueue;
    private static int q = 1;

    public static Props props(int replicaId) {
        return Props.create(Replica.class, () -> new Replica(replicaId));
    }

    public Replica(int replicaId) {
        this.replicaId = replicaId;
        this.timeStamp = new TimeId(0,0);
        this.localHistory = new History();
    }


    @Override
    public Receive createReceive() {
        return replicaBehavior();
    }

    public Receive replicaBehavior() {
        return receiveBuilder()
                .match(Messages.StartMessage.class, this::onStartMessage)
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
                .match(Messages.ReadReqMsg.class, this::onReadReqMsg)
                .match(Messages.WriteReqMsg.class, this::onWriteDuringElectionMsg)
                .match(Messages.WriteAckMsg.class, this::onWriteAckMsg)
                .build();
    }

    public Receive crashedBehavior() {
        return receiveBuilder()
                .matchAny(msg -> {}) // Ignore all messages when crashed
                .build();
    }

    private void setGroup(Messages.StartMessage m){
        this.groupOfReplicas = new ArrayList<ActorRef>();
        for(ActorRef repl: m.group){
            if(!repl.equals(getSelf())){
                this.groupOfReplicas.add(repl);
            }
        }
    }

    private void setCoordinator(ActorRef coordinator){
        this.coordinator = coordinator;
    }



    public void onStartMessage(Messages.StartMessage msg) {
        setGroup(msg);
    }

    public void crash(int interval) {
        getContext().become(crashedBehavior());
        // Optionally handle the duration for which the actor should remain in the crashed state
    }

    public void onReadReqMsg(Messages.ReadReqMsg req) {
        // Handle ReadReqMsg
        System.out.println(getSender().path().name()+" read req to "+getSelf().path().name()); // getSender().path().name : returns the name of the sender actor
        req.sender.tell(localHistory, getSelf());
    }

    public void onWriteReqMsg(Messages.WriteReqMsg req) {
        // Handle WriteReqMsg
        Messages.UpdateMsg updateMsg = new Messages.UpdateMsg(req.sender, req.getV(),timeStamp);
        if(getSelf().equals(coordinator)){
            for (ActorRef replica: groupOfReplicas){
                replica.tell(updateMsg, getSelf());
            }
        }
        coordinator.tell(req, getSelf());

    }

    public void onWriteDuringElectionMsg(Messages.WriteReqMsg req) {
        //Handle WriteReqMsg during election
    }

    public void onUpdateMsg(Messages.UpdateMsg msg) {
        // Handle UpdateMsg

        coordinator.tell(new Messages.WriteAckMsg(msg.sender,timeStamp), getSelf());
    }

    public void onWriteOKMsg(Messages.WriteOKMsg msg) {
        // Handle WriteOKMsg
        q++;
        if(q == (groupOfReplicas.size()/2+1)){

            Messages.WriteOKMsg writeOk= new Messages.WriteOKMsg(msg.sender,value)
            for (ActorRef replica: groupOfReplicas){
                replica.tell(writeOk, getSelf());
            }
        }

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

    public void onWriteAckMsg(Messages.WriteAckMsg msg){

    }

}
