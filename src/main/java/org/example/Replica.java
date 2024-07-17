package org.example;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.example.Utils.DEBUG;


public class Replica extends AbstractActor {

    private static int electionNumber=0;
    private static int a=0;
    private static int syncmsg = 0;
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

    private final HashMap<TimeId, Integer> quorum; //this is the quorum hashmap, because we assume that it can be possible that two quorums are asked concurrently (quorum for message m1 can start before
                                                    // the quorum request for message m0 is reached)

    private boolean isInElectionBehavior;
    private Messages.ElectionMsg cachedMsg; //this is the election message that, during election, remain cached till the election ack is received

    private Integer candidateCoordinatorID;
    private Snapshot lastKnownUpdate;

    private final Random rand = new Random();





    public enum TimeoutType{
        UPDATE(700),
        WRITEOK(1000),
        ELECTION_ACK(900),
        ELECTION_PROTOCOL(5000),
        RECEIVE_HEARTBEAT(1500),
        SEND_HEARTBEAT(500);



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
        this.heartbeatScheduler = new ArrayList<Cancellable>();
        this.lastKnownUpdate = Snapshot.defaultSnapshot();
    }

    void delay() {
        try {Thread.sleep(rand.nextInt(50));} catch (Exception ignored) {}
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
                //System.out.println("setTimeout: ** "+this.getSelf().path().name() + " ** is scheduling the timeout: " + type.name());
                //System.out.flush();
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

                .match(Messages.HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(Messages.TimeoutMsg.class, this::onTimeoutMsg)

                .matchAny(msg -> {})

                .build();
    }


    public Receive replicaDuringElectionBehavior() {
        // Define behavior for election state
        return receiveBuilder()

                .match(Messages.ReadReqMsg.class, this::onReadReqMsg)

                .match(Messages.ElectionMsg.class, this::onElectionMsg)
                .match(Messages.ElectionAckMsg.class, this::onElectionAckMsg)
                .match(Messages.SyncMsg.class, this::onSyncMsg)

                .match(Messages.TimeoutMsg.class, this::onTimeoutMsg)
                .matchAny(msg -> {})

                .build();
    }

    public Receive coordinatorBehavior() {
        // Define behavior for coordinator state
        return receiveBuilder()

                .match(Messages.ReadReqMsg.class, this::onReadReqMsg)
                .match(Messages.WriteReqMsg.class, this::onWriteReqMsg)

                .match(Messages.UpdateMsg.class, this::onUpdateMsg)
                .match(Messages.UpdateAckMsg.class, this::onUpdateAckMsg)

                .match(Messages.ElectionMsg.class, this::onElectionMsg)

                .match(Messages.HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(Messages.TimeoutMsg.class, this::onTimeoutMsg)

                .matchAny(msg -> {})

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
        if(DEBUG)
            System.out.println("Replica "+getSelf().path().name() + " received onStartMsg from "+getSender().path().name());

        setGroup(msg);

        groupOfReplicas = msg.getGroup();
        this.successor = this.groupOfReplicas.get((this.groupOfReplicas.indexOf(this.getSelf())+1) % this.groupOfReplicas.size());

        if(DEBUG){
            System.out.println("Replica " + replicaId + " started");
            System.out.flush();
        }

        this.setTimeout(TimeoutType.RECEIVE_HEARTBEAT);


    }

    public void crash() {
        if(DEBUG){
        System.out.println("********************* "+getSelf().path().name()+ " has crashed. **************************");
        System.out.flush();
        }
        for(Map.Entry<TimeoutType, Cancellable> entry : this.timeoutSchedule.entrySet())
            entry.getValue().cancel();
        this.timeoutSchedule.clear();

        if(this.coordinator == this.getSelf()) {
            for(Cancellable heartbeat : heartbeatScheduler)
                heartbeat.cancel();
            heartbeatScheduler.clear();
        }
        crashed = true;
        getContext().become(crashedBehavior());

    }

    public void onTimeoutMsg(Messages.TimeoutMsg msg){
        if(DEBUG)
            System.out.println(this.getSelf().path().name() + " receives ----- onTimeoutMsg ----- of type " + msg.type.name());

        Map<Integer, Messages.ElectionMsg.ActorData> actorData;
        switch (msg.type){
            case WRITEOK, RECEIVE_HEARTBEAT, UPDATE, ELECTION_PROTOCOL:
                //coordinator crashed
                this.getContext().become(this.replicaDuringElectionBehavior());
                this.isInElectionBehavior = true;
                this.candidateCoordinatorID = this.replicaId;
                this.lastKnownUpdate = !localHistory.isEmpty() ? this.localHistory.get(this.localHistory.size() - 1) : Snapshot.defaultSnapshot();
                actorData = new HashMap<Integer, Messages.ElectionMsg.ActorData>();
                actorData.put(this.replicaId, new Messages.ElectionMsg.ActorData(getSelf(), this.lastKnownUpdate));

                this.cachedMsg = new Messages.ElectionMsg(this.candidateCoordinatorID, this.lastKnownUpdate, actorData);
                delay();
                this.successor.tell(cachedMsg, this.getSelf()); //send election message to your successor
                this.setTimeout(TimeoutType.ELECTION_ACK);

                if(DEBUG && msg.type != TimeoutType.ELECTION_PROTOCOL){
                    electionNumber++;
                    System.out.println(electionNumber+"- "+this.getSelf().path().name() + " is starting an election with itself as candidate and its own last update. Its last update is: " + this.lastKnownUpdate.toString());
                    System.out.flush();
                }
                else if(DEBUG&& msg.type == TimeoutType.ELECTION_PROTOCOL){
                    System.out.println(this.getSelf().path().name() + " is  timed out for the ELECTION_PROTOCOL timeout and is re-starting an election with itself as candidate and its own last update. Its last update is: " + this.lastKnownUpdate.toString());
                    System.out.flush();
                }
                break;

            case ELECTION_ACK:
                if(isInElectionBehavior) {
                    //set the new successor and send the cached message
                    ActorRef oldSuccessor = this.successor;
                    this.successor = this.groupOfReplicas.get((this.groupOfReplicas.indexOf(oldSuccessor) + 1) % this.groupOfReplicas.size());

                    if(DEBUG){
                        System.out.println(getSelf().path().name() + " has timed out for election ack of the old successor " + oldSuccessor.path().name() + " and is changing the successor to the new one: " + this.successor.path().name());
                        System.out.flush();
                    }
                    delay();
                    this.successor.tell(this.cachedMsg, this.getSelf());
                    this.setTimeout(TimeoutType.ELECTION_ACK);
                }
                break;
        }
    }


    public void onReadReqMsg(Messages.ReadReqMsg req) {
        // Handle ReadReqMsg
        file.appendToFile(this.getSender().path().name()+" read req to "+this.getSelf().path().name());
        int i;
        for(i=this.localHistory.size()-1; i>=0 && !this.localHistory.get(i).isStable(); i--);
        if(i < 0) i = 0;

        delay();
        if(!localHistory.isEmpty())
            this.getSender().tell(this.localHistory.get(i).getV(), this.getSelf());
        else{
            this.getSender().tell(Snapshot.defaultSnapshot().getV(), this.getSelf());
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
            Snapshot snap = new Snapshot(this.timeStamp, req.getV(),false);
            this.localHistory.add(snap); // add the Msg to the local history of the coordinator with a specification that it's unstable

            Messages.UpdateMsg updateMsg = new Messages.UpdateMsg(snap); // create the update message with our Snapshot
            for (ActorRef replica: this.groupOfReplicas){// Broadcast the Update message to all the Replicas

                if(!replica.equals(this.coordinator)){
                    //int x = rand.nextInt(300);
                    delay();
                    if(DEBUG){
                        System.out.println("onWriteReqMsg: Coordinator ** " + this.getSelf().path().name() + " ** is sending the update msg with the value: "+req.getV()+" to replica ** " + replica.path().name()+" **");
                        System.out.flush();
                    }
                    //a++;
                    //if(a == 13){
                    //    crash(); break;}
                    replica.tell(updateMsg, this.getSender());
                }
            }

        }else{ // if the Replica is not the coordinator
            if(DEBUG){
                System.out.println("onWriteReqMsg: The ** " + this.getSelf().path().name() + " ** is not the coordinator so it forward the message to the Coordinator ** " + ((this.coordinator != null) ? this.coordinator.path().name() : "NULL") +" **");
                System.out.flush();
            }
            delay();
            setTimeout(TimeoutType.UPDATE);
            this.coordinator.tell(req, this.getSelf());

        }
    }


    public void onUpdateMsg(Messages.UpdateMsg msg) {
        // Handle UpdateMsg
        if(this.getSelf().equals(this.getSender()))
            this.timeoutSchedule.remove(TimeoutType.UPDATE).cancel(); // canceling the Update timeout of the replica
        this.localHistory.add(msg.snap); // add the unstable message to the replicas localHistory
        if(DEBUG){
            System.out.println("onUpdateMsg: ** " + getSelf().path().name() + " ** received update message from ** " + getSender().path().name()+" **");
            System.out.flush();
        }
        Messages.UpdateAckMsg updateAckMsg = new Messages.UpdateAckMsg(msg.snap.getTimeId());
        delay();
        setTimeout(TimeoutType.WRITEOK);
        this.coordinator.tell(updateAckMsg, this.getSelf()); //send the Ack of this specific message with associating the timeId of the message
    }

    public void onUpdateAckMsg(Messages.UpdateAckMsg msg){// **** work in this to stay alive and timeout while it doesn't receive the ACK cuz this is called each tile it receives an ACK
        if(DEBUG){
            System.out.println("onUpdateAckMsg: ** "+this.getSelf().path().name() + " received updateACK from ** " + this.getSender().path().name()+" **");
            System.out.flush();
        }

        if(!this.localHistory.get(msg.updateID.seqNum-1).isStable()){
            if(!this.quorum.containsKey(msg.updateID)){
                this.quorum.put(msg.updateID, 2); // the quorum == 2 because the coordinator ACK + the replicas ACK
                if(DEBUG){
                    System.out.println(" ----- ADD NEW Quorum for update (" + msg.updateID.epoch + "/" + msg.updateID.seqNum + ") is " + this.quorum.get(msg.updateID)+" -----");
                    System.out.flush();
                }
            }else{
                this.quorum.put(msg.updateID, quorum.get(msg.updateID) + 1);
                if(DEBUG){
                    System.out.println(" ----- Quorum for update (" + msg.updateID.epoch + "/" + msg.updateID.seqNum + ") is " + this.quorum.get(msg.updateID)+" -----");
                    System.out.flush();
                }
                if(this.quorum.get(msg.updateID) >= (this.groupOfReplicas.size()/2+1)){
                    if(DEBUG){
                        System.out.println(" ----- Set the message" + msg.updateID.epoch + "/" + msg.updateID.seqNum + ") to Stable -----");
                        System.out.flush();
                    }
                    this.localHistory.get(msg.updateID.seqNum-1).setStable(true);
                    Messages.WriteOKMsg writeOk= new Messages.WriteOKMsg(msg.updateID);
                    for (ActorRef replica: this.groupOfReplicas){
                        if(!this.getSelf().equals(replica)) {
                            delay();
                            replica.tell(writeOk, this.getSelf());
                        }
                    }
                    this.quorum.remove(msg.updateID);
                }
            }
        }
    }

    public void onWriteOKMsg(Messages.WriteOKMsg msg) {
        // Handle WriteOKMsg
        this.timeoutSchedule.remove(TimeoutType.WRITEOK).cancel(); // Canceling the WriteOk timeout of the replica
        if(DEBUG){
            System.out.println("onWriteOKMsg: ** " + this.getSelf().path().name() + "** received writeOK message of the message with timestamp: " + msg.updateID.toString() + " from ** " + this.getSender().path().name()+" **");
            System.out.flush();
        }
        //we need to keep the whole history across the epochs, so I do some stuff to get the index
        int lastSeqNum = this.localHistory.get(this.localHistory.size()-1).getTimeId().seqNum; //eqNum of the last msg in the array
        int seqNumDiff =  lastSeqNum - msg.updateID.seqNum; //difference between the last msg and the msg that I want (channels are FIFO, msgs are always ordered)
        this.localHistory.get((this.localHistory.size()-1) - seqNumDiff).setStable(true); //set stable the message
        if(DEBUG){
            System.out.println(" ----- ** "+this.getSelf().path().name() + "** update " + msg.updateID.toString() + " " + this.localHistory.get((this.localHistory.size() - 1) - seqNumDiff).getV()+" -----");
            System.out.flush();
        }
        file.appendToFile(this.getSelf().path().name()+" update "+msg.updateID.epoch+" : "+msg.updateID.seqNum+" "+this.localHistory.get((this.localHistory.size()-1) - seqNumDiff).getV());
    }

    public void onElectionAckMsg(Messages.ElectionAckMsg msg){
        if(this.timeoutSchedule.containsKey(TimeoutType.ELECTION_ACK))
            this.timeoutSchedule.remove(TimeoutType.ELECTION_ACK).cancel();
        if(this.cachedMsg != null)
            this.cachedMsg = null;
        if(DEBUG){
            System.out.println("** " + getSelf().path().name() + " ** received ElectionACKMsg from " + getSender().path().name() + ", so it has canceled the ElectionACK timout");
            System.out.flush();
        }
    }


    public void onElectionMsg(Messages.ElectionMsg msg){

        if(!this.isInElectionBehavior){
            this.isInElectionBehavior = true;
            this.getContext().become(replicaDuringElectionBehavior());
            //if(this.timeoutSchedule.containsKey(TimeoutType.RECEIVE_HEARTBEAT))
            //    this.timeoutSchedule.remove(TimeoutType.RECEIVE_HEARTBEAT).cancel();
            for(Map.Entry<TimeoutType, Cancellable> entry : this.timeoutSchedule.entrySet())
                entry.getValue().cancel();
            this.timeoutSchedule.clear();

            Snapshot lastSnapshot = !this.localHistory.isEmpty() ? this.localHistory.get(this.localHistory.size()-1) : Snapshot.defaultSnapshot();
            //if the first time you receive the election message, then put your data inside and forward.
            //Moreover, check if you are a candidate coordinator and if yes, update these data in the message and in yourself
            if(DEBUG) {
                System.out.println(getSelf().path().name() + " received the ElectionMsg for the first time from " + getSender().path().name() + " and it's entering into election behavior");
            }
            Map<Integer, Messages.ElectionMsg.ActorData> data = new HashMap<Integer, Messages.ElectionMsg.ActorData>(msg.actorDatas);
            data.put(this.replicaId, new Messages.ElectionMsg.ActorData(this.getSelf(), lastSnapshot));

            int compare = lastSnapshot.getTimeId().compareTo(msg.lastKnownUpdate.getTimeId());
            if((compare > 0) || ((compare == 0) && (this.replicaId > msg.candidateCoordinatorID))){
                if(DEBUG){
                    System.out.println(getSelf().path().name() + " is finding out from the token (sent by "+this.getSender().path().name() +") that it is currently the best candidate, with the snapshot: " + lastSnapshot.toString() +
                            " so it is updating itself and the token");
                    System.out.flush();
                }

                this.lastKnownUpdate = lastSnapshot;
                this.candidateCoordinatorID = this.replicaId;
            }

            else{
                if(DEBUG) {
                    System.out.println(getSelf().path().name() + " is finding out that the best candidate is the one already in the token (sent by "+this.getSender().path().name() +"), the candidate is: "
                            + msg.candidateCoordinatorID + " with the snapshot: " + msg.lastKnownUpdate.toString() +
                            " so it is updating itself and leaves the token unchanged");
                    System.out.flush();
                }

                this.lastKnownUpdate = msg.lastKnownUpdate;
                this.candidateCoordinatorID = msg.candidateCoordinatorID;
            }
            Messages.ElectionMsg token = new Messages.ElectionMsg(this.candidateCoordinatorID, this.lastKnownUpdate, data);
            delay();
            this.forwardSuccessor(token);
            delay();
            this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());

        }
        else{
            if(DEBUG) {
                System.out.println(getSelf().path().name() + " received another ElectionMsg from " + getSender().path().name());
            }
            //if it's not the first time you receive an election message, then:
            //   -- The token is passing for the second time and it has the true coordinator: your candidate has to be updated and you have to forward (put your data inside also, if you are not in the token)
            //   --  The  token has a candidate less updated then yours: is an election started after the election you already received, drop the message
            //   -- The token has a candidate that is the same as yours: or this is another election that can be dropped or this is a non-terminating token, drop it
            //   -- The candidate leader is you: you are the new coordinator

            int compare = this.lastKnownUpdate.getTimeId().compareTo(msg.lastKnownUpdate.getTimeId());
            if((compare < 0) || ( (compare == 0) && this.replicaId < msg.candidateCoordinatorID)  ){
                if(DEBUG) {
                        System.out.println(getSelf().path().name() + " finds out that the ElectionMsg from " + getSender().path().name() +
                                " brings a more updated candidate (or equally updated but with higher ID)that the current one in this replica (" + this.lastKnownUpdate.toString() + ") from the local candidate Replica" + this.candidateCoordinatorID +"." +
                                "The last known update in the token is instead: " + msg.lastKnownUpdate.toString() + " from the candidate " + msg.candidateCoordinatorID);
                }
                //if the message is bringing one update that is more recent then the one that I know to be the last one (so it is bringing a more updated leader then the one I have as candidate)
                // of, if comp==0, brings the same update
                this.lastKnownUpdate = msg.lastKnownUpdate;
                this.candidateCoordinatorID = msg.candidateCoordinatorID;

                Map<Integer, Messages.ElectionMsg.ActorData> data = new HashMap<Integer, Messages.ElectionMsg.ActorData>(msg.actorDatas);
                if(!msg.actorDatas.containsKey(this.replicaId))
                    data.put(this.replicaId, new Messages.ElectionMsg.ActorData(this.getSelf(), !this.localHistory.isEmpty() ? this.localHistory.get(this.localHistory.size()-1) : Snapshot.defaultSnapshot()));

                Messages.ElectionMsg token = new Messages.ElectionMsg(msg.candidateCoordinatorID, msg.lastKnownUpdate, data);
                //int x = rand.nextInt(200);
                delay();
                this.forwardSuccessor(token);
                delay();
                this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());

            }
            else if(this.replicaId == msg.candidateCoordinatorID && this.lastKnownUpdate == msg.lastKnownUpdate){ //if you are the coordinator
                if(DEBUG) {
                    System.out.println(getSelf().path().name() + " finds out that the ElectionMsg from " + getSender().path().name() +
                            " brings itself as candidate with the last known update as: " + msg.lastKnownUpdate.toString() +". The last known update of this replica is in fact: " + this.lastKnownUpdate.toString() +
                            " so it is becoming coordinator"
                    );
                    System.out.flush();
                }
                delay();
                this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());
                this.becomeCoordinatorAndUpdateReplicas(new TreeMap<Integer, Messages.ElectionMsg.ActorData>(msg.actorDatas));

            }
            else { //else drop message
                delay();
                this.getSender().tell(new Messages.ElectionAckMsg(), this.getSelf());
                if(DEBUG) {
                    System.out.println(getSelf().path().name() + " finds out that the ElectionMsg from " + getSender().path().name() +
                            " brings a less updated candidate that the current one in the replica (" + this.lastKnownUpdate.toString() + ") from the candidate Replica" + this.candidateCoordinatorID +"." +
                            "The last known update in the token is instead: " + msg.lastKnownUpdate.toString() + " from the candidate " + msg.candidateCoordinatorID);
                    System.out.flush();
                }
            }
        }

    }

    private void becomeCoordinatorAndUpdateReplicas(TreeMap<Integer, Messages.ElectionMsg.ActorData> actorDatas){
        this.isInElectionBehavior = false;
        this.getContext().unbecome();
        this.getContext().become(coordinatorBehavior());
        this.setCoordinator(this.getSelf());

        for(Map.Entry<Integer, Messages.ElectionMsg.ActorData> entry : actorDatas.entrySet()){
            ActorRef currentReplica = entry.getValue().replicaRef;
            Queue<Snapshot> partialHistory = new LinkedList<Snapshot>(); //queue of the missing updates to be sent to the current replica

            for(int i=this.localHistory.size()-1; i>=0; i--) {//add to the queue the missing updates to send to the current replica
                if (this.localHistory.get(i).equals(entry.getValue().lastUpdate)) break;
                else{
                    Snapshot snap = this.localHistory.get(i);
                    snap.setStable(true);
                    partialHistory.add(snap);
                }
            }
            this.timeStamp = new TimeId(this.timeStamp.epoch+1, 0);
            //send update
            Messages.SyncMsg syncMsg = new Messages.SyncMsg(partialHistory);
            delay();
            currentReplica.tell(syncMsg, this.getSelf());
        }

        setTimeout(TimeoutType.SEND_HEARTBEAT);

    }


    private void forwardSuccessor(Messages.ElectionMsg token){
        this.cachedMsg = token;
        if(DEBUG){
            a++;
            System.out.println(a+"- ************ "+getSelf().path().name()+" is sending an a token message to "+this.successor.path().name()+" **********");
            System.out.flush();
        }

        this.successor.tell(this.cachedMsg, this.getSelf());
        this.setTimeout(TimeoutType.ELECTION_ACK);
        if(DEBUG){
            a++;
            System.out.println(a+"- ************ "+getSelf().path().name()+" is sending an Election ack message to "+getSender().path().name()+" **********");
            System.out.flush();
        }

    }


    public void onHeartbeatMsg(Messages.HeartbeatMsg msg) {
        // Handle HeartbeatMsg
        if(this.timeoutSchedule.containsKey(TimeoutType.RECEIVE_HEARTBEAT))
            this.timeoutSchedule.remove(TimeoutType.RECEIVE_HEARTBEAT).cancel();
        this.setTimeout(TimeoutType.RECEIVE_HEARTBEAT);
    }

    public void onSyncMsg(Messages.SyncMsg msg) {
        // Handle SyncMsg
        if(DEBUG){
            syncmsg++;
            System.out.println("Sync Message number: "+syncmsg );
            System.out.print("-------- Replica " + this.getSelf().path().name() + " received SyncMsg from the new coordinator " + this.getSender().path().name()+"." +
                    "The message contains the updates: ");
            for(Snapshot snap : msg.sync)
                System.out.print(snap.toString() + ", ");
            System.out.println("The last update of the replica" + this.replicaId + "was: " + (!this.localHistory.isEmpty() ? this.localHistory.get(this.localHistory.size()-1) : Snapshot.defaultSnapshot()));
            System.out.flush();
        }

        this.setCoordinator(this.getSender());
        this.isInElectionBehavior = false;
        this.localHistory.addAll(msg.sync);
        this.timeStamp = new TimeId(this.timeStamp.epoch+1, 0);

        if(DEBUG){
            System.out.println("*************** this is the nes TimeStamp "+ timeStamp.epoch+" / "+timeStamp.seqNum);
            System.out.flush();
        }

        this.getContext().unbecome(); //return to normal behavior

        if(this.timeoutSchedule.containsKey(TimeoutType.ELECTION_PROTOCOL)) this.timeoutSchedule.remove(TimeoutType.ELECTION_PROTOCOL).cancel();
        if(this.timeoutSchedule.containsKey(TimeoutType.ELECTION_ACK)) this.timeoutSchedule.remove(TimeoutType.ELECTION_ACK).cancel();

        setTimeout(TimeoutType.RECEIVE_HEARTBEAT);
    }



    }
