package org.example;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.example.Utils.DEBUG;


public class Replica extends AbstractActor {

    private static int a=0;
    public static boolean crashed = false;




    Utils.FileAdd file = new Utils.FileAdd("output.txt");

    private final HashMap<TimeoutType, Cancellable> timeoutSchedule;
    private final List<Cancellable> heartbeatScheduler;


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
    private Messages.ElectionMsg cachedMsg; //this is the election message that, during election, remain cached till the election ack is received
    private Messages.ElectionMsg.ElectionID electionInstance; //identifier of the election protocol instance that has the priority in this epoch
    private int localElectionCounter; //counter for the lection protocol instances initiated by this replica in the current epoch


    public enum TimeoutType{
        UPDATE(200),
        WRITEOK(200),
        ELECTION_ACK(200),
        ELECTION_PROTOCOL(10000),
        RECEIVE_HEARTBEAT(2000),
        SEND_HEARTBEAT(300);



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
        this.quorum = new HashMap<TimeId, Integer>();
        this.isInElectionBehavior = false;
        this.timeoutSchedule = new HashMap<TimeoutType, Cancellable>();
        this.localElectionCounter = 0;
        this.electionInstance = Messages.ElectionMsg.ElectionID.defaultElectionID(); //Instantiate and ID that will always be preempted by any other ID, just for not having null
        this.cachedMsg = null;
        this.heartbeatScheduler = new ArrayList<Cancellable>();
    }

    void delay(int d) {
        try {Thread.sleep(d);} catch (Exception ignored) {}
    }


    private void setTimeout(TimeoutType type){
        if(type.equals(TimeoutType.SEND_HEARTBEAT)){
            if(DEBUG){
                System.out.println("setTimeout: The new coordinator ** " + this.getSelf().path().name() + " ** is scheduling the heartbeat");
                System.out.flush();
            }
            for (ActorRef replica: this.groupOfReplicas){
                if(!replica.equals(this.coordinator)){
                    Cancellable newTimeout = getContext().getSystem().scheduler().scheduleAtFixedRate(
                            Duration.Zero(),
                            Duration.create(type.getMillis(), TimeUnit.MILLISECONDS),
                            replica,
                            new Messages.HeartbeatMsg(),
                            getContext().system().dispatcher(),
                            getSelf()
                    );
                    this.heartbeatScheduler.add(newTimeout);
                }

            }
        }else{
            if(DEBUG){
                System.out.println("setTimeout: ** "+this.getSelf().path().name() + " ** is scheduling the timeout: " + type.name());
                System.out.flush();
            }
            Cancellable newTimeout = getContext().system().scheduler().scheduleOnce(
                    Duration.create(type.getMillis(), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new Messages.TimeoutMsg(type),
                    getContext().system().dispatcher(),
                    getSelf()
            );
            this.timeoutSchedule.put(type, newTimeout);
        }



    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()

                .match(Messages.StartMessage.class, this::onStartMessage)

                .match(Messages.ReadReqMsg.class, this::onReadReqMsg)
                .match(Messages.WriteReqMsg.class, this::onWriteReqMsg)

                .match(Messages.UpdateMsg.class, this::onUpdateMsg)
                .match(Messages.UpdateAckMsg.class, this::onUpdateAckMsg)
                .match(Messages.WriteOKMsg.class, this::onWriteOKMsg)

                .match(Messages.HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(Messages.TimeoutMsg.class, this::onTimeoutMsg)

                .matchAny(msg -> {})

                .build();
    }


    public Receive replicaDuringElectionBehavior() {
        // Define behavior for election state
        return receiveBuilder()

                //.match(Messages.StartMessage.class, msg -> {})

                .match(Messages.ReadReqMsg.class, this::onReadReqMsg)
                // .match(Messages.WriteReqMsg.class, this::onWriteDuringElectionMsg) // COMMENTED: just ignore the write requests during election
                //.match(Messages.WriteReqMsg.class, msg -> {})

                //.match(Messages.UpdateMsg.class, msg -> {})
                //.match(Messages.UpdateAckMsg.class, msg -> {})
                //.match(Messages.WriteOKMsg.class, msg -> {})

                //.match(Messages.ElectionMsg.class, this::onElectionMsg)
                .match(Messages.ElectionAckMsg.class, this::onElectionAckMsg)
                .match(Messages.SyncMsg.class, this::onSyncMsg)

                //.match(Messages.HeartbeatMsg.class, msg -> {})
                .match(Messages.TimeoutMsg.class, this::onTimeoutMsg)
                .matchAny(msg -> {})

                .build();
    }

    public Receive coordinatorBehavior() {
        // Define behavior for coordinator state
        return receiveBuilder()

                //.match(Messages.StartMessage.class, msg -> {})

                .match(Messages.ReadReqMsg.class, this::onReadReqMsg)
                .match(Messages.WriteReqMsg.class, this::onWriteReqMsg)

                .match(Messages.UpdateMsg.class, this::onUpdateMsg)
                .match(Messages.UpdateAckMsg.class, this::onUpdateAckMsg)
                //.match(Messages.WriteOKMsg.class, msg -> {})

                //.match(Messages.ElectionMsg.class, this::onElectionMsg)
                //.match(Messages.ElectionAckMsg.class, msg -> {})
                //.match(Messages.SyncMsg.class, msg -> {})

                .match(Messages.HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(Messages.TimeoutMsg.class, this::onTimeoutMsg)

                .matchAny(msg -> {})

                .build();


                //.match(Messages.ReadReqMsg.class, this::onReadReqMsg)
                //.match(Messages.WriteReqMsg.class, this::onWriteReqMsg)
                //.match(Messages.UpdateAckMsg.class, this::onUpdateAckMsg)
                //.match(Messages.MessageDebugging.class, this::onDebuggingMsg)
                //.build();
    }

    //public void onDebuggingMsg(Messages.MessageDebugging msg){
    //    System.out.println("onDebuggingMsg");
    //}

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
        if(DEBUG){
            System.out.println("onStartMessageReplica: ** " + getSelf().path().name() + " ** received onStartMsg from ** " + getSender().path().name()+" **");
            System.out.flush();
        }

        setGroup(msg);
        if(getSelf().equals(msg.group.get(0))){
            if(DEBUG){
                System.out.println("Replica ** " + replicaId + " ** started as a coordinator");
                System.out.flush();
            }
            groupOfReplicas = msg.getGroup();
            this.successor = this.groupOfReplicas.get((this.groupOfReplicas.indexOf(this.getSelf()) + 1) % this.groupOfReplicas.size());
            this.coordinator = groupOfReplicas.get(0);
            getContext().become(coordinatorBehavior());
            getSelf().tell(new Messages.MessageDebugging(), ActorRef.noSender());

        }else {
            if (DEBUG) {
                System.out.println("Replica " + replicaId + " started");
                System.out.flush();
            }
            groupOfReplicas = msg.getGroup();
            this.successor = this.groupOfReplicas.get((this.groupOfReplicas.indexOf(this.getSelf()) + 1) % this.groupOfReplicas.size());
            this.coordinator = groupOfReplicas.get(0);
            this.setTimeout(TimeoutType.RECEIVE_HEARTBEAT);
        }


    }

    public void crash() {

        System.out.println(getSelf().path().name());
        System.out.flush();
        if(this.coordinator == this.getSelf()) {
            for(Cancellable heartbeat : heartbeatScheduler)
                heartbeat.cancel();
            heartbeatScheduler.clear();
        }
        crashed = true;
        getContext().become(crashedBehavior());

    }

    public void onTimeoutMsg(Messages.TimeoutMsg msg){
        if(DEBUG){
            System.out.println(this.getSelf().path().name() + " receives ----- onTimeoutMsg -----" + msg.type.name() + "  from " + getSender().path().name());
            System.out.flush();
        }
        ArrayList<Messages.ElectionMsg.ActorData> actorData;
        switch (msg.type){
            case WRITEOK, RECEIVE_HEARTBEAT, UPDATE:
                actorData = new ArrayList<Messages.ElectionMsg.ActorData>();
                if(!localHistory.isEmpty())
                    actorData.add(new Messages.ElectionMsg.ActorData(getSelf(),this.replicaId,this.localHistory.get(this.localHistory.size()-1)));
                else
                    actorData.add(new Messages.ElectionMsg.ActorData(getSelf(),this.replicaId, Snapshot.defaultSnapshot()));

                this.cachedMsg = new Messages.ElectionMsg(new Messages.ElectionMsg.ElectionID(this.replicaId, this.localElectionCounter), actorData);
                getSelf().tell(this.cachedMsg, this.getSelf()); //send election message to yourself

                this.localElectionCounter++; //increment the local counter for the next election instance created by this replica in thi epoch (if this initiator timeouts)

                break;

            case ELECTION_PROTOCOL:
                if(isInElectionBehavior) {
                    //restart the protocol
                    this.getContext().unbecome();
                    this.cachedMsg = null;
                    this.isInElectionBehavior = false;
                    actorData = new ArrayList<Messages.ElectionMsg.ActorData>();
                    if (!localHistory.isEmpty())
                        actorData.add(new Messages.ElectionMsg.ActorData(getSelf(), this.replicaId, this.localHistory.get(this.localHistory.size() - 1)));
                    else
                        actorData.add(new Messages.ElectionMsg.ActorData(getSelf(), this.replicaId, Snapshot.defaultSnapshot()));

                    this.cachedMsg = new Messages.ElectionMsg(new Messages.ElectionMsg.ElectionID(this.replicaId, this.localElectionCounter), actorData);
                    getSelf().tell(this.cachedMsg, this.getSelf()); //send election message to yourself

                    this.localElectionCounter++; //increment the local counter for the next election instance created by this replica in thi epoch (if this initiator timeouts)

                }
                break;

            case ELECTION_ACK:
                if(isInElectionBehavior) {

                    //set the new successor and send the cached message
                    ActorRef oldSuccessor;
                    if(DEBUG)
                        oldSuccessor = this.successor;


                    this.successor = this.groupOfReplicas.get((this.groupOfReplicas.indexOf(this.successor) + 1) % this.groupOfReplicas.size());
                    if(DEBUG){
                        System.out.println(getSelf().path().name() + " has timed out for election ack of the old successor " + this.successor.path().name() + " and is changing the successor to the new one: " + this.successor.path().name());
                        System.out.flush();
                    }
                    this.successor.tell(this.cachedMsg, this.getSelf());



                    this.setTimeout(TimeoutType.ELECTION_ACK);
                }
                break;


            //this.successor = this.groupOfReplicas.get(this.replicaId+2);



        }
    }


    public void onReadReqMsg(Messages.ReadReqMsg req) {
        // Handle ReadReqMsg
        file.appendToFile(this.getSender().path().name()+" read req to "+this.getSelf().path().name());
        int i;
        for(i=this.localHistory.size()-1; i>=0 && !this.localHistory.get(i).getStable(); i--);
        if(!localHistory.isEmpty())
            this.getSender().tell(this.localHistory.get(i).getV(), this.getSelf());
        else{
            this.getSender().tell(Snapshot.defaultSnapshot().getV(), this.getSelf());
        }
    }

    public void onWriteReqMsg(Messages.WriteReqMsg req) {
        if(getSelf().equals(coordinator)){
            // if the replica is the coordinator
            timeStamp = new TimeId(this.timeStamp.epoch, this.timeStamp.seqNum+1);
            Snapshot snap = new Snapshot(this.timeStamp, req.getV(),false);
            this.localHistory.add(snap); // add the Msg to the local history of the coordinator with a specification that it's unstable

            Messages.UpdateMsg updateMsg = new Messages.UpdateMsg(snap); // create the update message with our Snapshot
            for (ActorRef replica: this.groupOfReplicas){ // Broadcast the Update message to all the Replicas
                if(!replica.equals(this.coordinator)){
                    if(DEBUG){
                        System.out.println("onWriteReqMsg: Coordinator ** " + this.getSelf().path().name() + " ** is sending the update msg to replica ** " + replica.path().name()+" **");
                    }
                    replica.tell(updateMsg, this.getSelf());
                }
            }
        }else{ // if the Replica is not the coordinator
            if(DEBUG){
                System.out.println("onWriteReqMsg: The ** " + this.getSelf().path().name() + " ** is not the coordinator so it forward the message to the Coordinator ** " + this.coordinator.path().name()+" **");
                System.out.flush();
            }
            setTimeout(TimeoutType.UPDATE);
            this.coordinator.tell(req, this.getSelf());

        }
    }


    public void onUpdateMsg(Messages.UpdateMsg msg) {
        // Handle UpdateMsg
        this.timeoutSchedule.remove(TimeoutType.UPDATE).cancel(); // canceling the Update timeout of the replica
        this.localHistory.add(msg.snap); // add the unstable message to the replicas localHistory
        if(DEBUG){
            System.out.println("onUpdateMsg: ** " + getSelf().path().name() + " ** received update message from ** " + getSender().path().name()+" **");
            System.out.flush();
        }
        Messages.UpdateAckMsg updateAckMsg = new Messages.UpdateAckMsg(msg.snap.getTimeId());
        setTimeout(TimeoutType.WRITEOK);
        this.coordinator.tell(updateAckMsg, this.getSelf()); //send the Ack of this specific message with associating the timeId of the message
    }

    public void onUpdateAckMsg(Messages.UpdateAckMsg msg){// **** work in this to stay alive and timeout while it doesn't receive the ACK cuz this is called each tile it receives an ACK
        if(DEBUG){
            System.out.println("onUpdateAckMsg: ** "+this.getSelf().path().name() + " received updateACK from ** " + this.getSender().path().name()+" **");
        }
        if(!this.localHistory.get(msg.uid.seqNum-1).getStable()){
            if(!this.quorum.containsKey(msg.uid)){
                this.quorum.put(msg.uid, 2); // the quorum == 2 because the coordinator ACK + the replicas ACK
                if(DEBUG){
                    System.out.println(" ----- ADD NEW Quorum for update (" + msg.uid.epoch + "/" + msg.uid.seqNum + ") is " + this.quorum.get(msg.uid)+" -----");
                }
            }else{
                this.quorum.put(msg.uid, quorum.get(msg.uid) + 1);
                if(DEBUG){
                    System.out.println(" ----- Quorum for update (" + msg.uid.epoch + "/" + msg.uid.seqNum + ") is " + this.quorum.get(msg.uid)+" -----");
                }
                if(this.quorum.get(msg.uid) >= (this.groupOfReplicas.size()/2+1)){
                    if(DEBUG){
                        System.out.println(" ----- Set the message" + msg.uid.epoch + "/" + msg.uid.seqNum + ") to Stable -----");
                        System.out.flush();
                    }
                    this.localHistory.get(msg.uid.seqNum-1).setStable(true);
                    Messages.WriteOKMsg writeOk= new Messages.WriteOKMsg(msg.uid);
                    for (ActorRef replica: this.groupOfReplicas){
                        if(!this.getSelf().equals(replica))
                            replica.tell(writeOk, this.getSelf());
                    }
                    this.quorum.remove(msg.uid);
                }
            }
        }
    }

    public void onWriteOKMsg(Messages.WriteOKMsg msg) {
        // Handle WriteOKMsg
        this.timeoutSchedule.remove(TimeoutType.WRITEOK).cancel(); // Canceling the WriteOk timeout of the replica
        if(DEBUG){
            System.out.println("onWriteOKMsg: ** " + this.getSelf().path().name() + "** received writeOK message of the message (" + msg.uid.epoch + "/" + msg.uid.seqNum + ") from ** " + this.getSender().path().name()+" **");
        }
        //we need to keep the whole history across the epochs, so I do some stuff to get the index
        int lastSeqNum = this.localHistory.get(this.localHistory.size()-1).getTimeId().seqNum; //eqNum of the last msg in the array
        int seqNumDiff =  lastSeqNum - msg.uid.seqNum; //difference between the last msg and the msg that I want (channels are FIFO, msgs are always ordered)
        this.localHistory.get((this.localHistory.size()-1) - seqNumDiff).setStable(true); //set stable the message
        if(DEBUG){
            System.out.println(" ----- ** "+this.getSelf().path().name() + "** update " + msg.uid.epoch + " : " + msg.uid.seqNum + " " + this.localHistory.get((this.localHistory.size() - 1) - seqNumDiff).getV()+" -----");
            System.out.flush();
        }
        file.appendToFile(this.getSelf().path().name()+" update "+msg.uid.epoch+" : "+msg.uid.seqNum+" "+this.localHistory.get((this.localHistory.size()-1) - seqNumDiff).getV());
    }

    public void onElectionAckMsg(Messages.ElectionAckMsg msg){
        Cancellable electionACKTimeout = this.timeoutSchedule.get(TimeoutType.ELECTION_ACK);
        electionACKTimeout.cancel();
        //this.timeoutSchedule.remove(TimeoutType.ELECTION_ACK);
        this.cachedMsg = null;
        if(DEBUG){
            System.out.println("Replica " + getSelf().path().name() + " received ElectionACKMsg from " + getSender().path().name() + ", so it has canceled the ElectionACK timout");
            System.out.flush();
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
        this.timeoutSchedule.remove(TimeoutType.RECEIVE_HEARTBEAT);
        this.setTimeout(TimeoutType.RECEIVE_HEARTBEAT);
    }

    public void onSyncMsg(Messages.SyncMsg msg) {
        // Handle SyncMsg
        if(DEBUG){
            System.out.println("-------- Replica " + getSelf().path().name() + " received SyncMsg from the new coordinator " + getSender().path().name());
            System.out.flush();
        }
        //System.exit(0);
        this.setCoordinator(this.getSender());
        this.isInElectionBehavior = false;
        this.localHistory.addAll(msg.sync);
        this.getContext().unbecome(); //return to normal behavior
        Cancellable electionProtocolTimeout = this.timeoutSchedule.get(TimeoutType.ELECTION_PROTOCOL);
        electionProtocolTimeout.cancel();
        this.timeoutSchedule.remove(TimeoutType.ELECTION_PROTOCOL);

        Cancellable electionAckTimeout = this.timeoutSchedule.get(TimeoutType.ELECTION_ACK);
        electionAckTimeout.cancel();
        this.timeoutSchedule.remove(TimeoutType.ELECTION_ACK);
        this.localElectionCounter = 0; //reset counter for the new epoch
        this.electionInstance = Messages.ElectionMsg.ElectionID.defaultElectionID();
        setTimeout(TimeoutType.RECEIVE_HEARTBEAT);
    }



}
