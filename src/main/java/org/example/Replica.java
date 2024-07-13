package org.example;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;


public class Replica extends AbstractActor {
    Utils.FileAdd file = new Utils.FileAdd("output.txt");

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

    private Random rand = new Random();
    private boolean isInElectionBehavior;

    public enum TimeoutType{
        UPDATE_REQ(20),
        WRITEOK(20),
        ELECTION_ACK(10),
        ELECTION_PROTOCOL(1000),
        RECEIVE_HEARTBEAT(100),
        SEND_HEARTBEAT(30);

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
        this.isInElectionBehavior = false;
        this.timeoutSchedule = new HashMap<TimeoutType, Cancellable>();
    }

    void delay(int d) {
        try {Thread.sleep(d);} catch (Exception ignored) {}
    }




    private void setTimeout(TimeoutType type){
        if(type.equals(TimeoutType.SEND_HEARTBEAT)){
            for (ActorRef replica: groupOfReplicas){
                if(!replica.equals(coordinator)){
                    getContext().getSystem().scheduler().scheduleAtFixedRate(
                            Duration.Zero(),
                            Duration.create(type.getMillis(), TimeUnit.MILLISECONDS),
                            replica,
                            new Messages.HeartbeatMsg(),
                            getContext().system().dispatcher(),
                            getSelf()
                    );
                }
            }
        }else{
            Cancellable newTimeout = getContext().system().scheduler().scheduleOnce(
                    Duration.create(type.getMillis(), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new Messages.TimeoutMsg(type),
                    getContext().system().dispatcher(),
                    getSelf()
            );
            timeoutSchedule.put(type, newTimeout);
        }



    }


    @Override
    //public Receive createReceive() {
      //  return replicaBehavior();
    //}

    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.StartMessage.class, this::onStartMessage)
                .match(Messages.ReadReqMsg.class, this::onReadReqMsg)
                .match(Messages.WriteReqMsg.class, this::onWriteReqMsg)
                .match(Messages.UpdateMsg.class, this::onUpdateMsg)
                .match(Messages.WriteOKMsg.class, this::onWriteOKMsg)
                .match(Messages.ElectionMsg.class, this::onElectionMsg)
                .match(Messages.HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(Messages.TimeoutMsg.class, this::onTimeoutMsg)
                .build();
    }


    public Receive replicaDuringElectionBehavior() {
        // Define behavior for election state
        return receiveBuilder()
                .match(Messages.ElectionMsg.class, this::onElectionMsg)
                .match(Messages.ReadReqMsg.class, this::onReadReqMsg)
                .match(Messages.ElectionAckMsg.class, this::onElectionAckMsg)
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
                .match(Messages.MessageDebugging.class, this::onDebuggingMsg)
                .build();
    }

    public void onDebuggingMsg(Messages.MessageDebugging msg){
        System.out.println("onDebuggingMsg");
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
        System.out.println("Replica "+getSelf().path().name() + " received onStartMsg from "+getSender().path().name());
        System.out.flush();

        setGroup(msg);
//        if(getSelf().equals(msg.group.get(0))){
//            System.out.println("Replica " + replicaId + " started as a coordinator");
//            System.out.flush();
//            getContext().become(coordinatorBehavior());
//            getSelf().tell(new Messages.MessageDebugging(), ActorRef.noSender());
//        }
        System.out.println("Replica " + replicaId + " started");
        System.out.flush();
        groupOfReplicas = msg.getGroup();
        //this.coordinator = groupOfReplicas.get(0);



        this.setTimeout(TimeoutType.RECEIVE_HEARTBEAT);


    }

    public void crash(int interval) {
        getContext().become(crashedBehavior());
        // Optionally handle the duration for which the actor should remain in the crashed state
    }

    public void onTimeoutMsg(Messages.TimeoutMsg msg){
        switch (msg.type){
            case WRITEOK, RECEIVE_HEARTBEAT, UPDATE_REQ:
                ArrayList<Messages.ElectionMsg.ActorData> actorData = new ArrayList<Messages.ElectionMsg.ActorData>();
                if(!localHistory.isEmpty())
                    actorData.add(new Messages.ElectionMsg.ActorData(getSelf(),this.replicaId,this.localHistory.get(this.localHistory.size()-1)));
                else
                    actorData.add(new Messages.ElectionMsg.ActorData(getSelf(),this.replicaId,null));
                getSelf().tell(new Messages.ElectionMsg(this.timeStamp.epoch, actorData),ActorRef.noSender());
                break;
        }
    }


    public void onReadReqMsg(Messages.ReadReqMsg req) {
        System.out.println("Replica "+getSelf().path().name() + " received ReadRequest from "+getSender().path().name());
        System.out.flush();

        // Handle ReadReqMsg
        file.appendToFile(getSender().path().name()+" read req to "+getSelf().path().name());
        System.out.println(getSender().path().name()+" read req to "+getSelf().path().name()); // getSender().path().name : returns the name of the sender actor
        int i;
        for(i=this.localHistory.size()-1; i>=0 && !this.localHistory.get(i).getStable(); i--);


        if(!localHistory.isEmpty())
            this.getSender().tell(localHistory.get(i).getV(), getSelf());
        else{
            this.getSender().tell(0, getSelf());
        }
    }

    public void onWriteReqMsg(Messages.WriteReqMsg req) {
        // Handle WriteReqMsg
        if(getSelf().equals(coordinator)){
            System.out.println("Coordinator "+getSelf().path().name() + " received writereq from "+getSender().path().name()+" with value "+ req.getV());
            System.out.flush();
            // if the replica is the coordinator
            timeStamp = new TimeId(this.timeStamp.epoch, this.timeStamp.seqNum+1);
            Snapshot snap = new Snapshot(timeStamp, req.getV(),false);
            this.localHistory.add(snap); // add the Msg to the local history of the coordinator with a specification that it's unstable

            Messages.UpdateMsg updateMsg = new Messages.UpdateMsg(snap); // create the update message with our Snapshot
            for (ActorRef replica: this.groupOfReplicas){ // Broadcast the Update message to all the Replicas
                if(!replica.equals(this.coordinator)){
                    System.out.println("Coordinator "+getSelf().path().name() + " is sending the update msg to replica "+replica.path().name());
                    System.out.flush();
                    replica.tell(updateMsg, this.getSelf());
                }
            }
        }else{ // if the Replica is not the coordinator
            System.out.println("Replica "+getSelf().path().name() + " received writereq from "+getSender().path().name() + " with value "+req.getV()+" and is forwarding to coordinator "+coordinator.path().name());
            System.out.flush();
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
        System.out.println("Replica "+getSelf().path().name() + " received updateMsg from "+getSender().path().name());
        System.out.flush();
        Messages.UpdateAckMsg updateAckMsg = new Messages.UpdateAckMsg(msg.snap.getTimeId());

        coordinator.tell(updateAckMsg, this.getSelf()); //send the Ack of this specific message with associating the timeId of the message
    }

    public void onWriteOKMsg(Messages.WriteOKMsg msg) {
        // Handle WriteOKMsg
        System.out.println("replica "+getSelf().path().name() + " received writeOKmsg of the msg (" + msg.uid.epoch+"/"+msg.uid.seqNum+") from "+getSender().path().name());
        System.out.flush();
        //we need to keep the whole history across the epochs, so I do some stuff to get the index
        int lastSeqNum = this.localHistory.get(this.localHistory.size()-1).getTimeId().seqNum; //eqNum of the last msg in the array
        int seqNumDiff =  lastSeqNum - msg.uid.seqNum; //difference between the last msg and the msg that I want (channels are FIFO, msgs are always ordered)
        this.localHistory.get((this.localHistory.size()-1) - seqNumDiff).setStable(true); //set stable the message
        System.out.println(getSelf().path().name()+" update stabilized "+msg.uid.epoch+" : "+msg.uid.seqNum+" "+this.localHistory.get((this.localHistory.size()-1) - seqNumDiff).getV());
        System.out.flush();
        file.appendToFile(getSelf().path().name()+" update stabilized "+msg.uid.epoch+" : "+msg.uid.seqNum+" "+this.localHistory.get((this.localHistory.size()-1) - seqNumDiff).getV());

    }

    public void onElectionAckMsg(Messages.ElectionAckMsg msg){
        this.setTimeout(TimeoutType.ELECTION_ACK);
        System.out.println("Replica "+getSelf().path().name() + " received ElectionACKMsg from "+ getSender().path().name());
        System.out.flush();
    }

    public void onElectionMsg(Messages.ElectionMsg msg) {
        if (!isInElectionBehavior) {
            this.getContext().become(replicaDuringElectionBehavior());
            isInElectionBehavior = true;
            System.out.println("Replica "+getSelf().path().name() + " received ElectionMsg from "+ getSender().path().name()+ "and is turning in election behavior");
            System.out.flush();
        }

        //check if the election message contains this replica already
        if(msg.actorDatas.stream().anyMatch(actorData -> actorData.replicaRef == this.getSelf())){
            // if yes, check if you are the new coordinator
            System.out.println("Replica "+getSelf().path().name() + " received ElectionMsg for the second round from "+ getSender().path().name()+ "and is checking if it is the new coordinator");
            System.out.flush();
            TimeId lastUpdateTimestamp = this.localHistory.get(localHistory.size()-1).getTimeId();
            for(Messages.ElectionMsg.ActorData actorDatum : msg.actorDatas){
                int comp = actorDatum.lastUpdate.getTimeId().compareTo(lastUpdateTimestamp);
                if(0 < comp || ( comp == 0 && actorDatum.actorId > this.replicaId )){
                    //if someone else has more recent update or has the same update but highest ID than you then forward and return
                    this.successor.tell(msg, this.getSelf());
                    this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());
                    System.out.println("Replica "+getSelf().path().name() + " loses the election and forwards to "+ this.successor);
                    System.out.flush();
                    return;
                }
            }
            //if no one has more recent update than you, then you are the new coordinator
            System.out.println("Replica "+getSelf().path().name() + " wins the election and becomes the new coordinator");
            System.out.flush();
            this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());
            this.isInElectionBehavior = false;
            this.getContext().unbecome();
            this.getContext().become(coordinatorBehavior());
            this.setCoordinator(this.getSelf());
            this.updateReplicas(msg.actorDatas); //update the other replicas
            setTimeout(TimeoutType.SEND_HEARTBEAT);
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
            ActorRef currentReplica = actorDatum.replicaRef;
            Queue<Snapshot> partialHistory = new LinkedList<Snapshot>(); //queue of the missing updates to be sent to the current replica

            for(int i=this.localHistory.size()-1; i>=0; i--) {//add to the queue the missing updates to send to the current replica
                if (this.localHistory.get(i).equals(actorDatum.lastUpdate)) break;
                else partialHistory.add(this.localHistory.get(i));
            }
            //send updates
            Messages.SyncMsg syncMsg = new Messages.SyncMsg(partialHistory);
            currentReplica.tell(syncMsg, this.getSelf());
        }
    }


    public void onHeartbeatMsg(Messages.HeartbeatMsg msg) {
        // Handle HeartbeatMsg
        Cancellable heartbeatTimeout = this.timeoutSchedule.get(TimeoutType.RECEIVE_HEARTBEAT);
        heartbeatTimeout.cancel();
        this.setTimeout(TimeoutType.RECEIVE_HEARTBEAT);
    }

    public void onSyncMsg(Messages.SyncMsg msg) {
        // Handle SyncMsg
        System.out.println("Replica "+getSelf().path().name() + " received SyncMsg from the new coordinator "+ getSender().path().name());
        System.out.flush();
        this.setCoordinator(this.getSender());
        this.isInElectionBehavior = false;
        this.localHistory.addAll(msg.sync);
        this.getContext().unbecome(); //return to normal behavior
        setTimeout(TimeoutType.RECEIVE_HEARTBEAT);
    }

    public void onUpdateAckMsg(Messages.UpdateAckMsg msg){// **** work in this to stay alive and timeout while it doesn't receive the ACK cuz this is called each tile it receives an ACK
        System.out.println(getSelf().path().name() + " received updateACK from "+getSender().path().name());
        System.out.flush();
        System.out.println("onUpdateAckMsg");
        System.out.flush();
        if(!localHistory.get(msg.uid.seqNum-1).getStable()){
            if(!quorum.containsKey(msg.uid)){
                quorum.put(msg.uid, 2); // the quorum == 2 because the coordinator ACK + the replicas ACK
                System.out.println("Quorum for update (" + msg.uid.epoch + "/" +msg.uid.seqNum + ") is " + quorum.get(msg.uid));
                System.out.flush();
            }else{
                quorum.put(msg.uid, quorum.get(msg.uid) + 1);
                System.out.println("Quorum for update (" + msg.uid.epoch + "/" +msg.uid.seqNum + ") is " + quorum.get(msg.uid));
                System.out.flush();
                if(quorum.get(msg.uid) >= (groupOfReplicas.size()/2+1)){
                    localHistory.get(msg.uid.seqNum-1).setStable(true);
                    file.appendToFile(getSelf().path().name()+" update "+msg.uid.epoch+" : "+msg.uid.seqNum+" "+this.localHistory.get(this.localHistory.size()-1).getV());
                    Messages.WriteOKMsg writeOk= new Messages.WriteOKMsg(msg.uid);

                    for (ActorRef replica: groupOfReplicas){
                        if(!getSelf().equals(replica))
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
