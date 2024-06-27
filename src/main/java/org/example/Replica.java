package org.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;


public class Replica extends AbstractActor {
    private final int replicaId;
    private ActorRef successor;
    private final TimeId timeStamp;
    private List<ActorRef> groupOfReplicas;
    private ActorRef coordinator;
    private final LinkedList<Snapshot> localHistory; // this is the linked list of the local history: last node is the last update. For each update, we have the timestamp associated (with the Snapshot datatype)
    private final HashMap<TimeId, Snapshot> writeAckHistory; // this is the history for saving the values with their timeStamp during the update phase.
    private final HashMap<TimeId, Integer> quorum; //this is the quorum hashmap, because we assume that it can be possible that two quorums are asked concurrently (quorum for message m1 can start before
                                                    // the quorum request for message m0 is reached)
    private final Queue<Messages.WriteReqMsg> writeReqMsgQueue; // this is the queue of the write requests for when a replica receives write reqs from the clients during the lection: we have to enqueue the
                                                                //requests and serve them later

    public static Props props(int replicaId) {
        return Props.create(Replica.class, () -> new Replica(replicaId));
    }

    public Replica(int replicaId) {
        this.replicaId = replicaId;
        this.timeStamp = new TimeId(0,0);
        this.localHistory = new LinkedList<Snapshot>();
        this.writeAckHistory = new HashMap<TimeId, Snapshot>();
        this.quorum = new HashMap<TimeId, Integer>();
        this.writeReqMsgQueue = new LinkedList<Messages.WriteReqMsg>();
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
        req.sender.tell(localHistory.getLast().getV(), getSelf());
    }

    public void onWriteReqMsg(Messages.WriteReqMsg req) {
        // Handle WriteReqMsg
        if(getSelf().equals(coordinator)){
            // if the replica is the coordinator
            timeStamp.seqNum++;
            Snapshot snap = new Snapshot(timeStamp, req.getV());
            this.writeAckHistory.put(timeStamp, snap);

            Messages.UpdateMsg updateMsg = new Messages.UpdateMsg(req.sender, snap);
            for (ActorRef replica: groupOfReplicas){
                replica.tell(updateMsg, ActorRef.noSender());
            }
        }else{ // if the Replica is not the coordinator
            coordinator.tell(req, ActorRef.noSender());
        }


    }

    public void onWriteDuringElectionMsg(Messages.WriteReqMsg req) {
        //Handle WriteReqMsg during election
        this.writeReqMsgQueue.add(req);
    }

    public void onUpdateMsg(Messages.UpdateMsg msg) {
        // Handle UpdateMsg
        writeAckHistory.put(msg.snap.getTimeId(), msg.snap);
        coordinator.tell(new Messages.WriteAckMsg(msg.sender, msg.snap.getTimeId()), ActorRef.noSender());
    }

    public void onWriteOKMsg(Messages.WriteOKMsg msg) {
        // Handle WriteOKMsg
        localHistory.add(writeAckHistory.remove(msg.uid));
    }

    public void onElectionMsg(Messages.ElectionMsg msg) {
        // Handle ElectionMsg

        List<Messages.ElectionMsg.ActorData> actorData = msg.actorDatas;
        List<Integer> actorIDs = new ArrayList<Integer>();

        for(Messages.ElectionMsg.ActorData actorDatum : actorData)
            actorIDs.add(actorDatum.actorId);

        if(actorIDs.contains(this.replicaId)){ //if the message contains already this Actor
            int comp;
            for(Messages.ElectionMsg.ActorData actorDatum : actorData) { //check if you are the most updated one and if you are the highest-ID one among the most updated
                comp = actorDatum.lastUpdate.getTimeId().compareTo(localHistory.getLast().getTimeId());
                if (comp > 0 || ( comp == 0 && actorDatum.actorId > this.replicaId )) { //if not, then forward and return
                    this.getSender().tell(new Messages.ElectionAckMsg(this.getSelf()), this.getSelf());
                    this.successor.tell(msg, this.getSelf());
                    return;
                }
            }

            //Otherwise congrats, you are the new coordinator
            for(Messages.ElectionMsg.ActorData actorDatum : actorData){// tell this to everyone and update all the other nodes
                Iterator<Snapshot> it = this.localHistory.descendingIterator();
                LinkedList<Snapshot> partialHistory = new LinkedList<Snapshot>(); //this will be the missing history of this Actor
                while(it.hasNext()){
                    Snapshot snap = it.next();
                    if(snap.equals(actorDatum.lastUpdate)) //this is the last update of the actor, everything from now on has to be sent to him
                        break;
                    partialHistory.add(snap);
                }
                Messages.SyncMsg sync = new Messages.SyncMsg(this.getSelf(), partialHistory);
                this.getSender().tell(new Messages.ElectionAckMsg(this.getSelf()), this.getSelf());
                actorDatum.replicaRef.tell(sync, this.getSelf());
            }
        }
        else { //put yourself in the message and forward
            Messages.ElectionMsg.ActorData data = new Messages.ElectionMsg.ActorData(this.getSelf(), this.replicaId, localHistory.getLast());
            List<Messages.ElectionMsg.ActorData> newActorData = new ArrayList<Messages.ElectionMsg.ActorData>(actorData);
            newActorData.add(data);
            this.getSender().tell(new Messages.ElectionAckMsg(this.getSelf()), this.getSelf());
            this.successor.tell(new Messages.ElectionMsg(this.getSelf(), newActorData), this.getSelf());
        }


    }

    public void onHeartbeatMsg(Messages.HeartbeatMsg msg) {
        // Handle HeartbeatMsg
    }

    public void onSyncMsg(Messages.SyncMsg msg) {
        // Handle SyncMsg
        this.coordinator = this.getSender();
        this.localHistory.addAll(msg.sync);
        while(!writeReqMsgQueue.isEmpty()){ //send all the wirte requests enqueued during the election phase
            this.coordinator.tell(writeReqMsgQueue.remove(), this.getSelf());
        }
    }

    public void onWriteAckMsg(Messages.WriteAckMsg msg){ // **** work in this to stay alive and timeout while it doesn't receive the ACK cuz this is called each tile it receives an ACK
        quorum.putIfAbsent(msg.uid, 1);
        quorum.put(msg.uid, quorum.get(msg.uid) + 1);
        if(quorum.get(msg.uid) == (groupOfReplicas.size()/2+1)){
            localHistory.add(writeAckHistory.remove(msg.uid));
            Messages.WriteOKMsg writeOk= new Messages.WriteOKMsg(msg.sender, msg.uid);

            for (ActorRef replica: groupOfReplicas){
                replica.tell(writeOk, getSelf());
            }
            quorum.remove(msg.uid);
        }

    }

}
