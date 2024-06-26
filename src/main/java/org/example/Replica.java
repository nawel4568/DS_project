package org.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;


public class Replica extends AbstractActor {
    private final int replicaId;
    private TimeId timeStamp;
    private List<ActorRef> groupOfReplicas;
    private ActorRef coordinator;
    private LinkedList<Snapshot> localHistory;
    private LinkedList<Snapshot> writeAckHistory; // this is the history for saving the values with their timeStamp during the update phase
    private HashMap<TimeId, Integer> quorum;

    public static Props props(int replicaId) {
        return Props.create(Replica.class, () -> new Replica(replicaId));
    }

    public Replica(int replicaId) {
        this.replicaId = replicaId;
        this.timeStamp = new TimeId(0,0);
        this.localHistory = new LinkedList<Snapshot>();
        this.writeAckHistory = new LinkedList<Snapshot>();
        quorum = new HashMap<TimeId, Integer>();
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
                .match(Messages.WriteReqMsg.class, this::onWriteReqMsg)
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
        if(getSelf().equals(coordinator)){
            // if the replica is the coordinator
            timeStamp.seqNum++;
            Snapshot historyNode = new Snapshot(timeStamp,req.getV());
            this.writeAckHistory.add(historyNode);

            Messages.UpdateMsg updateMsg = new Messages.UpdateMsg(req.sender, historyNode);
            for (ActorRef replica: groupOfReplicas){
                replica.tell(updateMsg, ActorRef.noSender());
            }
        }else{ // if the Replica is not the coordinator
            coordinator.tell(req, ActorRef.noSender());
        }


    }

    public void onWriteDuringElectionMsg(Messages.WriteReqMsg req) {
        //Handle WriteReqMsg during election
    }

    public void onUpdateMsg(Messages.UpdateMsg msg) {
        // Handle UpdateMsg
        writeAckHistory.add(msg.HistoryNode);
        coordinator.tell(new Messages.WriteAckMsg(msg.sender, msg.HistoryNode.getTimeId()), ActorRef.noSender());
    }

    public void onWriteOKMsg(Messages.WriteOKMsg msg) {
        // Handle WriteOKMsg
        localHistory.add(writeAckHistory.remove());
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

    public void onWriteAckMsg(Messages.WriteAckMsg msg){ // **** work in this to stay alive and timeout while it doesn't receive the ACK cuz this is called each tile it receives an ACK
        quorum.putIfAbsent(msg.uid, 1);
        quorum.put(msg.uid, quorum.get(msg.uid) + 1);
        if(quorum.get(msg.uid) == (groupOfReplicas.size()/2+1)){

            localHistory.add(writeAckHistory.remove());
            Messages.WriteOKMsg writeOk= new Messages.WriteOKMsg(msg.sender, msg.uid);
            for (ActorRef replica: groupOfReplicas){
                replica.tell(writeOk, getSelf());
            }
            quorum.remove(msg.uid);
        }

    }

}
