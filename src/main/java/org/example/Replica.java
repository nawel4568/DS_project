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

    private Random rand = new Random();

    public enum TimeoutType{
        UPDATE_REQ(200),
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
        //this.localHistory.add(new Snapshot(new TimeId(-1,1), Integer.MIN_VALUE, true));

        //this.writeAckHistory = new HashMap<TimeId, Snapshot>();
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
                System.out.println("The new coordinator " + getSelf().path().name() + " is scheduling the heartbeat in the setTimeout method");
                System.out.flush();
            }
            for (ActorRef replica: groupOfReplicas){
                if(!replica.equals(coordinator)){
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
                System.out.println(getSelf().path().name() + " is scheduling the timeout of type: " + type.name());
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

                .match(Messages.ElectionMsg.class, this::onElectionMsg)
                //.match(Messages.ElectionAckMsg.class, msg -> {})
                //.match(Messages.SyncMsg.class, msg -> {})

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

                .match(Messages.ElectionMsg.class, this::onElectionMsg)
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

                .match(Messages.ElectionMsg.class, this::onElectionMsg)
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
        System.out.println("Replica "+getSelf().path().name() + " received onStartMsg from "+getSender().path().name());
        System.out.flush();

        setGroup(msg);
//        if(getSelf().equals(msg.group.get(0))){
//            System.out.println("Replica " + replicaId + " started as a coordinator");
//            System.out.flush();
//            getContext().become(coordinatorBehavior());
//            getSelf().tell(new Messages.MessageDebugging(), ActorRef.noSender());
//        }
        if(DEBUG){
            System.out.println("Replica " + replicaId + " started");
            System.out.flush();
        }
        groupOfReplicas = msg.getGroup();
        this.successor = this.groupOfReplicas.get((this.groupOfReplicas.indexOf(this.getSelf())+1) % this.groupOfReplicas.size());

        //this.coordinator = groupOfReplicas.get(0);

        this.setTimeout(TimeoutType.RECEIVE_HEARTBEAT);


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
            case WRITEOK, RECEIVE_HEARTBEAT, UPDATE_REQ:
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
        if(DEBUG){
            System.out.println("Replica " + getSelf().path().name() + " received ReadRequest from " + getSender().path().name());
            System.out.flush();
        }

        // Handle ReadReqMsg
        file.appendToFile(getSender().path().name()+" read req to "+getSelf().path().name());
        if(DEBUG){
            System.out.println(getSender().path().name() + " read req to " + getSelf().path().name()); // getSender().path().name : returns the name of the sender actor
        }
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
            if(DEBUG){
                System.out.println("Coordinator " + getSelf().path().name() + " received writereq from " + getSender().path().name() + " with value " + req.getV());
                System.out.flush();
            }
            // if the replica is the coordinator
            timeStamp = new TimeId(this.timeStamp.epoch, this.timeStamp.seqNum+1);
            Snapshot snap = new Snapshot(timeStamp, req.getV(),false);
            this.localHistory.add(snap); // add the Msg to the local history of the coordinator with a specification that it's unstable

            Messages.UpdateMsg updateMsg = new Messages.UpdateMsg(snap); // create the update message with our Snapshot
            for (ActorRef replica: this.groupOfReplicas){ // Broadcast the Update message to all the Replicas
                if(!replica.equals(this.coordinator)){
                    if(DEBUG){
                        System.out.println("Coordinator " + getSelf().path().name() + " is sending the update msg to replica " + replica.path().name());
                        System.out.flush();
                    }
                    replica.tell(updateMsg, this.getSelf());
                    /***  crashed  ***/
                    a++;
                    if(a==13){
                        file.appendToFile("the coordinator Crashed");
                        if(DEBUG){
                            System.out.println("------ " + getSelf().path().name() + " is CRASHED ------");
                            System.out.flush();
                        }

                       this.crash();
                    }

                }
            }
        }else{ // if the Replica is not the coordinator
            if(DEBUG){
                System.out.println("Replica " + getSelf().path().name() + " received writereq from " + getSender().path().name() + " with value " + req.getV() + " and is forwarding to coordinator " + coordinator.path().name());
                System.out.flush();
            }
            coordinator.tell(req, ActorRef.noSender());
        }


    }

/*    public void onWriteDuringElectionMsg(Messages.WriteReqMsg req) {
        //Handle WriteReqMsg during election
        this.writeReqMsgQueue.add(req);
    } */    //public Receive createReceive() {
      //  return replicaBehavior();
    //}


    public void onUpdateMsg(Messages.UpdateMsg msg) {
        // Handle UpdateMsg
        localHistory.add(msg.snap); // add the unstable message to the replicas localHistory
        if(DEBUG){
            System.out.println("Replica " + getSelf().path().name() + " received updateMsg from " + getSender().path().name());
            System.out.flush();
        }
        Messages.UpdateAckMsg updateAckMsg = new Messages.UpdateAckMsg(msg.snap.getTimeId());

        coordinator.tell(updateAckMsg, this.getSelf()); //send the Ack of this specific message with associating the timeId of the message
    }

    public void onWriteOKMsg(Messages.WriteOKMsg msg) {
        // Handle WriteOKMsg
        if(DEBUG){
            System.out.println("replica " + getSelf().path().name() + " received writeOKmsg of the msg (" + msg.uid.epoch + "/" + msg.uid.seqNum + ") from " + getSender().path().name());
            System.out.flush();
        }
        //we need to keep the whole history across the epochs, so I do some stuff to get the index
        int lastSeqNum = this.localHistory.get(this.localHistory.size()-1).getTimeId().seqNum; //eqNum of the last msg in the array
        int seqNumDiff =  lastSeqNum - msg.uid.seqNum; //difference between the last msg and the msg that I want (channels are FIFO, msgs are always ordered)
        this.localHistory.get((this.localHistory.size()-1) - seqNumDiff).setStable(true); //set stable the message
        if(DEBUG){
            System.out.println(getSelf().path().name() + " update " + msg.uid.epoch + " : " + msg.uid.seqNum + " " + this.localHistory.get((this.localHistory.size() - 1) - seqNumDiff).getV());
            System.out.flush();
        }
        file.appendToFile(getSelf().path().name()+" update "+msg.uid.epoch+" : "+msg.uid.seqNum+" "+this.localHistory.get((this.localHistory.size()-1) - seqNumDiff).getV());

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

    public void onElectionMsg(Messages.ElectionMsg msg) {
        System.out.println("before Debug");
        System.out.flush();
        if(this.electionInstance.comparePriority(msg.instanceID) > 0) { // if current instance has higher priority then discard this election
            if(DEBUG){
                System.out.println(
                        this.getSelf().path().name() + " received an ElectionMsg from " + this.getSender().path().name() +
                                " with lower priority (current instance priority -> initID: " + this.electionInstance.initiatorID +
                                ", counter: " + this.electionInstance.locCounter +
                                "; message instance priority -> initID: " + msg.instanceID.initiatorID +
                                ", counter: " + msg.instanceID.locCounter + ")"
                );
                System.out.flush();
            }
            System.out.println("**********   "+this.getSelf().path().name()+" Drops "+this.getSender().path().name()+" *********");
            System.out.flush();
            return;
        }
        this.electionInstance = msg.instanceID;

        if (!isInElectionBehavior) {
            if(DEBUG){
                if(this.timeoutSchedule.get(TimeoutType.RECEIVE_HEARTBEAT) == null){

                    System.out.println(getSelf().path().name() + " received ElectionMsg from " + getSender().path().name() + " and is finding the RECEIVE_HEARTBEAT timeout in the hashmap as null. This leads to the error");
                    System.out.flush();
                }
            }
            this.getContext().become(replicaDuringElectionBehavior());
            //this.timeoutSchedule.remove(TimeoutType.ELECTION_ACK);
            isInElectionBehavior = true;
            // !!! maybe I have to check if the heartbeat timeout is null
            this.timeoutSchedule.get(TimeoutType.RECEIVE_HEARTBEAT).cancel(); //remove and cancel heartbeat timeout from the schedule
            if(DEBUG){
                System.out.println("Replica " + getSelf().path().name() + " received ElectionMsg from " + getSender().path().name() + " and is turning in election behavior");
            }
            this.setTimeout(TimeoutType.ELECTION_PROTOCOL); //set timeout for election convergence
            //System.out.flush();
            if(this.getSender() == this.getSelf()){
                //if the electionMsg received is sent by yourself, then you don't need to update the data inside it, you just have to forward
                //it to your successor and set the timout for the ACK
                if(DEBUG){
                    System.out.println("this replica (" + getSelf().path().name() + ") initiated the election, so forwarding to successor " + this.successor.path().name());
                    System.out.flush();
                }
                this.cachedMsg = msg;
                this.successor.tell(this.cachedMsg, this.getSelf());
                //if(this.timeoutSchedule.get(TimeoutType.ELECTION_ACK) != null)
                //    this.timeoutSchedule.get(TimeoutType.ELECTION_ACK).cancel();
                this.setTimeout(TimeoutType.ELECTION_ACK);
                return;
            }
        }

        //check if the election message contains this replica already
        if(msg.actorDatas.stream().anyMatch(actorData -> actorData.replicaRef == this.getSelf())){
            // if yes, check if you are the new coordinator
            if(DEBUG){
                System.out.println("Replica " + getSelf().path().name() + " received ElectionMsg for the second round from " + getSender().path().name() + " and is checking if it is the new coordinator");
                System.out.flush();
            }
            TimeId lastUpdateTimestamp;// = this.localHistory.get(localHistory.size()-1).getTimeId();
            if(!this.localHistory.isEmpty()) lastUpdateTimestamp = this.localHistory.get(localHistory.size()-1).getTimeId();
            else lastUpdateTimestamp = Snapshot.defaultSnapshot().getTimeId();
            for(Messages.ElectionMsg.ActorData actorDatum : msg.actorDatas){
                int comp = actorDatum.lastUpdate.getTimeId().compareTo(lastUpdateTimestamp);
                if(0 < comp || ( comp == 0 && actorDatum.actorId > this.replicaId )){
                    //if someone else has more recent update or has the same update but highest ID than you then forward and return
                    this.cachedMsg = msg; //cache the message in case the successor is crashed
                    this.successor.tell(msg, this.getSelf());
                    //if(this.timeoutSchedule.get(TimeoutType.ELECTION_ACK) != null)
                    //    this.timeoutSchedule.get(TimeoutType.ELECTION_ACK).cancel();
                    this.setTimeout(TimeoutType.ELECTION_ACK);
                    this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());
                    if(DEBUG){
                        System.out.println("Replica " + getSelf().path().name() + " loses the election and forwards to " + this.successor.path().name());
                        System.out.flush();
                    }
                    return;
                }
            }
            //if no one has more recent update than you, then you are the new coordinator
            if(DEBUG){
                System.out.println("Replica " + getSelf().path().name() + " wins the election and becomes the new coordinator");
                System.out.flush();
            }
            //this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());
            this.isInElectionBehavior = false;
            this.getContext().unbecome();
            this.getContext().become(coordinatorBehavior());
            this.setCoordinator(this.getSelf());
            this.localElectionCounter = 0; //reset counter for the new epoch
            this.updateReplicas(msg.actorDatas); //update the other replicas
            this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());
            setTimeout(TimeoutType.SEND_HEARTBEAT);
            //if(this.timeoutSchedule.get(TimeoutType.ELECTION_ACK) != null)
            //    this.timeoutSchedule.get(TimeoutType.ELECTION_ACK).cancel();
            //System.exit(0);

        }

        else{ //if you are not alreadu in the token
            Snapshot lastUpdate;// = this.localHistory.get(this.localHistory.size()-1);
            if(!this.localHistory.isEmpty()) lastUpdate = this.localHistory.get(this.localHistory.size()-1);
            else lastUpdate = Snapshot.defaultSnapshot();
            Messages.ElectionMsg.ActorData newData = new Messages.ElectionMsg.ActorData(
                    this.getSelf(),
                    this.replicaId,
                    lastUpdate
            );
            List<Messages.ElectionMsg.ActorData> newActorData = new ArrayList<Messages.ElectionMsg.ActorData>(msg.actorDatas);
            newActorData.add(newData);
            this.cachedMsg = new Messages.ElectionMsg(msg.instanceID, newActorData);
            this.successor.tell(this.cachedMsg, this.getSelf());
            if(DEBUG){
                System.out.println("Replica " + getSelf().path().name() + " is not in the msg: "+ msg.instanceID.initiatorID +" so it put itself into it and forwards to the successor " + this.successor.path().name());
                System.out.flush();
            }
            //if(this.timeoutSchedule.get(TimeoutType.ELECTION_ACK) != null)
            //    this.timeoutSchedule.get(TimeoutType.ELECTION_ACK).cancel();
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

    public void onUpdateAckMsg(Messages.UpdateAckMsg msg){// **** work in this to stay alive and timeout while it doesn't receive the ACK cuz this is called each tile it receives an ACK
        if(DEBUG){
            System.out.println(getSelf().path().name() + " received updateACK from " + getSender().path().name());
            System.out.flush();
            System.out.println("onUpdateAckMsg");
            System.out.flush();
        }
        if(!localHistory.get(msg.uid.seqNum-1).getStable()){
            if(!quorum.containsKey(msg.uid)){
                quorum.put(msg.uid, 2); // the quorum == 2 because the coordinator ACK + the replicas ACK
                if(DEBUG){
                    System.out.println("Quorum for update (" + msg.uid.epoch + "/" + msg.uid.seqNum + ") is " + quorum.get(msg.uid));
                    System.out.flush();
                }
            }else{
                quorum.put(msg.uid, quorum.get(msg.uid) + 1);
                if(DEBUG){
                    System.out.println("Quorum for update (" + msg.uid.epoch + "/" + msg.uid.seqNum + ") is " + quorum.get(msg.uid));
                    System.out.flush();
                }
                if(quorum.get(msg.uid) >= (groupOfReplicas.size()/2+1)){
                    localHistory.get(msg.uid.seqNum-1).setStable(true);

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
