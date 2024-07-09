package org.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;


public class Replica extends AbstractActor {
    //private final static int HEARTBEAT_TIMEOUT = 50;
    //private final static int UPDATEREQ_TIMEOUT = 20;
    //private final static int WRITEOK_TIMEOUT = 20;
    //private final static int ELECTION_ACK_TIMEOUT = 10;
    //private final static int ELECTION_PROTOCOL_TIMEOUT = 1000;
    private HashMap<TimeoutType, Cancellable> timeoutSchedule;

    private final int replicaId;
    private ActorRef successor;
    private TimeId timeStamp; //if coordinator, this TimeId contains the current epoch and the sequence number of the last update
                              //if replica, this timeId contains the current apoch and the seqNum counter is kept to 0 (useless)
    private List<ActorRef> groupOfReplicas;
    private ActorRef coordinator;
    private final List<Snapshot> localHistory; // this is the linked list of the local history: last node is the last update. For each update, we have the timestamp associated (with the Snapshot datatype)

    // private final HashMap<TimeId, Snapshot> writeAckHistory; // this is the history for saving the values with their timeStamp during the update phase.
    private final HashMap<TimeId, Integer> quorum; //this is the quorum hashmap, because we assume that it can be possible that two quorums are asked concurrently (quorum for message m1 can start before
                                                    // the quorum request for message m0 is reached)
    //private final Queue<Messages.WriteReqMsg> writeReqMsgQueue; // this is the queue of the write requests for when a replica receives write reqs from the clients during the election: we have to enqueue the
                                                                //requests and serve them later
    private boolean isInElectionBehavior;

    public enum TimeoutType{
        UPDATE_REQ(20),
        WRITEOK(20),
        ELECTION_ACK(10),
        ELECTION_PROTOCOL(1000),
        HEARTBEAT(100);

        private final int millis;
        TimeoutType(int millis){
            this.millis=millis;
        }

        public int getMillis(){ return this.millis; }
    }


    public static Props props(int replicaId) {
        return Props.create(Replica.class, () -> new Replica(replicaId));
    }

    public Replica(int replicaId) {
        this.replicaId = replicaId;
        this.timeStamp = new TimeId(0,0);
        this.localHistory = new ArrayList<Snapshot>();
        //this.writeAckHistory = new HashMap<TimeId, Snapshot>();
        this.quorum = new HashMap<TimeId, Integer>();
        this.writeReqMsgQueue = new LinkedList<Messages.WriteReqMsg>();
        this.isInElectionBehavior = false;
        this.timeoutSchedule = new HashMap<TimeoutType, Cancellable>();
    }

    private void setTimeout(TimeoutType type){
        Cancellable newTimeout = getContext().system().scheduler().scheduleOnce(
                Duration.create(type.getMillis(), TimeUnit.MILLISECONDS),
                getSelf(),
                new Messages.TimeoutMsg(type),
                getContext().system().dispatcher(),
                getSelf()
        );
        timeoutSchedule.put(type, newTimeout);
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
               // .match(Messages.WriteReqMsg.class, this::onWriteDuringElectionMsg) // COMMENTED: just ignore the write requests during election
                .match(Messages.SyncMsg.class, this::onSyncMsg)
                .build();
    }

    public Receive coordinatorBehavior() {
        // Define behavior for coordinator state
        return receiveBuilder()
                .match(Messages.ReadReqMsg.class, this::onReadReqMsg)
                .match(Messages.WriteReqMsg.class, this::onWriteReqMsg)
                .match(Messages.UpdateAckMsg.class, this::onUpdateAckMsg)
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
        setTimeout(TimeoutType.HEARTBEAT);
    }

    public void crash(int interval) {
        getContext().become(crashedBehavior());
        // Optionally handle the duration for which the actor should remain in the crashed state
    }

    public void onReadReqMsg(Messages.ReadReqMsg req) {
        // Handle ReadReqMsg
        System.out.println(getSender().path().name()+" read req to "+getSelf().path().name()); // getSender().path().name : returns the name of the sender actor
        this.getSender().tell(localHistory.get(localHistory.size()-1).getV(), getSelf());
    }match(Messages.WriteReqMsg.class, writeReqMsg -> {})

    public void onWriteReqMsg(Messages.WriteReqMsg req) {
        // Handle WriteReqMsg
        if(getSelf().equals(coordinator)){
            // if the replica is the coordinator
            timeStamp = new TimeId(timeStamp.epoch, timeStamp.seqNum+1);
            Snapshot snap = new Snapshot(timeStamp, req.getV(),false);
            this.localHistory.add(snap); // add the Msg to the local history of the coordinator with a specification that it's unstable

            Messages.UpdateMsg updateMsg = new Messages.UpdateMsg(snap); // create the update message with our Snapshot
            for (ActorRef replica: groupOfReplicas){ // Broadcast the Update message to all the Replicas
                replica.tell(updateMsg, ActorRef.noSender());
            }
        }else{ // if the Replica is not the coordinator
            coordinator.tell(req, ActorRef.noSender());
        }


    }

/*    public void onWriteDuringElectionMsg(Messages.WriteReqMsg req) {
        //Handle WriteReqMsg during election
        this.writeReqMsgQueue.add(req);
    } */

    public void onUpdateMsg(Messages.UpdateMsg msg) {
        // Handle UpdateMsg
        localHistory.add(msg.snap); // add the unstable message to the replicas localHistory
        coordinator.tell(new Messages.UpdateAckMsg(msg.snap.getTimeId()), ActorRef.noSender()); //send the Ack of this specific message with associating the timeId of the message
    }

    public void onWriteOKMsg(Messages.WriteOKMsg msg) {
        // Handle WriteOKMsg
        //we need to keep the whole history across the epochs, so I do some stuff to get the index
        int lastSeqNum = this.localHistory.get(this.localHistory.size()-1).getTimeId().seqNum; //eqNum of the last msg in the array
        int seqNumDiff = msg.uid.seqNum - lastSeqNum; //difference between the last msg and the msg that I want (channels are FIFO, msgs are always ordered)
        this.localHistory.get((this.localHistory.size()-1) - seqNumDiff).setStable(true); //set stable the message
    }

    /** public void onElectionMsg(Messages.ElectionMsg msg) {
        // Handle ElectionMsg
        if (!isInElectionBehavior) {
            this.getContext().become(replicaDuringElectionBehavior());
            isInElectionBehavior = true;
        }

        List<Messages.ElectionMsg.ActorData> actorData = msg.actorDatas;
        List<Integer> actorIDs = new ArrayList<Integer>();

        for(Messages.ElectionMsg.ActorData actorDatum : actorData)
            actorIDs.add(actorDatum.actorId);

        if(actorIDs.contains(this.replicaId)){ //if the message contains already this Actor
            int comp;
            for(Messages.ElectionMsg.ActorData actorDatum : actorData) { //check if you are the most updated one and if you are the highest-ID one among the most updated
                comp = actorDatum.lastUpdate.getTimeId().compareTo(localHistory.getLast().getTimeId());
                if (comp > 0 || ( comp == 0 && actorDatum.actorId > this.replicaId )) { //if not, then forward and return
                    this.successor.tell(msg, this.getSelf());
                    this.getSender().tell(new Messages.ElectionAckMsg(this.getSelf()), this.getSelf());
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
                    partialHistory.addFirst(snap);
                }
                this.getSender().tell(new Messages.ElectionAckMsg(this.getSelf()), this.getSelf());

                this.isInElectionBehavior = false;
                this.getContext().unbecome();
                this.getContext().become(coordinatorBehavior());
                this.setCoordinator(this.getSelf());
                Messages.SyncMsg sync = new Messages.SyncMsg(this.getSelf(), partialHistory);
                actorDatum.replicaRef.tell(sync, this.getSelf());
                this.ventWriteQueue(); //empty the write requests queue
                this.timeStamp = new TimeId(this.timeStamp.epoch+1, 0);
                return;
            }
        }
        else { //put yourself in the message and forward
            Messages.ElectionMsg.ActorData data = new Messages.ElectionMsg.ActorData(this.getSelf(), this.replicaId, localHistory.getLast());
            List<Messages.ElectionMsg.ActorData> newActorData = new ArrayList<Messages.ElectionMsg.ActorData>(actorData);
            newActorData.add(data);
     this.successor.tell(new Messages.ElectionMsg(this.getSelf(), newActorData), this.getSelf());
     this.getSender().tell(new Messages.ElectionAckMsg(this.getSelf()), this.getSelf());
        }


    } **/

    public void onElectionMsg(Messages.ElectionMsg msg) {
        if (!isInElectionBehavior) {
            this.getContext().become(replicaDuringElectionBehavior());
            isInElectionBehavior = true;
        }
        //check if the election message contains this replica already
        if(msg.actorDatas.stream().anyMatch(actorData -> actorData.replicaRef == this.getSelf())){
            // if yes, check if you are the new coordinator
            TimeId lastUpdateTimestamp = this.localHistory.get(localHistory.size()-1).getTimeId();
            for(Messages.ElectionMsg.ActorData actorDatum : msg.actorDatas){
                int comp = actorDatum.lastUpdate.getTimeId().compareTo(lastUpdateTimestamp);
                if(0 < comp || ( comp == 0 && actorDatum.actorId > this.replicaId )){
                    //if someone else has more recent update or has the same update but highest ID than you then forward and return
                    this.successor.tell(msg, this.getSelf());
                    this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());
                    return;
                }
            }
            //if no one has more recent update than you, then you are the new coordinator
            this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());
            this.isInElectionBehavior = false;
            this.getContext().unbecome();
            this.getContext().become(coordinatorBehavior());
            this.setCoordinator(this.getSelf());
            this.updateReplicas(msg.actorDatas); //update the other replicas
        }
        else{ //if you are not alreadu in the token
            Messages.ElectionMsg.ActorData newData = new Messages.ElectionMsg.ActorData(
                    this.getSelf(),
                    this.replicaId,
                    this.localHistory.get(this.localHistory.size()-1)
            );
            List<Messages.ElectionMsg.ActorData> newActorData = new ArrayList<Messages.ElectionMsg.ActorData>(msg.actorDatas);
            newActorData.add(newData);
            this.successor.tell(new Messages.ElectionMsg(this.timeStamp.epoch, newActorData), this.getSelf());
            this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());


        }
    }

    private void updateReplicas(List<Messages.ElectionMsg.ActorData> actorDataList){
        //tell that you are the new coordinator and send the missing partial history to each node
        for(Messages.ElectionMsg.ActorData actorDatum : actorDataList){
            //build the missing partial history for this node. Iterate backward through the localHistory
            for(int i=this.localHistory.size(); i>=0)
        }



        /*for(Messages.ElectionMsg.ActorData actorDatum : actorData){// tell this to everyone and update all the other nodes
            Iterator<Snapshot> it = this.localHistory.descendingIterator();
            LinkedList<Snapshot> partialHistory = new LinkedList<Snapshot>(); //this will be the missing history of this Actor
            while(it.hasNext()){
                Snapshot snap = it.next();
                if(snap.equals(actorDatum.lastUpdate)) //this is the last update of the actor, everything from now on has to be sent to him
                    break;
                partialHistory.addFirst(snap);
            }
            this.getSender().tell(new Messages.ElectionAckMsg(this.getSelf()), this.getSelf());

            this.isInElectionBehavior = false;
            this.getContext().unbecome();
            this.getContext().become(coordinatorBehavior());
            this.setCoordinator(this.getSelf());
            Messages.SyncMsg sync = new Messages.SyncMsg(this.getSelf(), partialHistory);
            actorDatum.replicaRef.tell(sync, this.getSelf());
            this.ventWriteQueue(); //empty the write requests queue
            this.timeStamp = new TimeId(this.timeStamp.epoch+1, 0);
            return; */



    }


    public void onHeartbeatMsg(Messages.HeartbeatMsg msg) {
        // Handle HeartbeatMsg
        Cancellable heartbeatTimeout = this.timeoutSchedule.get(TimeoutType.HEARTBEAT);
        heartbeatTimeout.cancel();
        this.setTimeout(TimeoutType.HEARTBEAT);
    }

    public void onSyncMsg(Messages.SyncMsg msg) {
        // Handle SyncMsg
        this.isInElectionBehavior = false;
        this.getContext().unbecome();
        this.setCoordinator(this.getSender());
        this.localHistory.addAll(msg.sync);
        //this.ventWriteQueue();
        if(!this.writeAckHistory.isEmpty()){

        }
    }

    public void onUpdateAckMsg(Messages.UpdateAckMsg msg){// **** work in this to stay alive and timeout while it doesn't receive the ACK cuz this is called each tile it receives an ACK
        if(!localHistory.get(msg.uid.seqNum-1).getStable()){
            if(!quorum.containsKey(msg.uid)){
                quorum.put(msg.uid, 2); // the quorum == 2 because the coordinator ACK + the replicas ACK
            }else{
                quorum.put(msg.uid, quorum.get(msg.uid) + 1);
                if(quorum.get(msg.uid) >= (groupOfReplicas.size()/2+1)){
                    localHistory.get(msg.uid.seqNum-1).setStable(true);
                    Messages.WriteOKMsg writeOk= new Messages.WriteOKMsg(msg.uid);
                    for (ActorRef replica: groupOfReplicas){
                        replica.tell(writeOk, getSelf());
                    }
                    quorum.remove(msg.uid);
                }
            }
        }



    }

/*    private void ventWriteQueue(){
        while(!writeReqMsgQueue.isEmpty()) //send all the wirte requests enqueued during the election phase
            this.coordinator.tell(writeReqMsgQueue.remove(), this.getSelf());

    } */



}
