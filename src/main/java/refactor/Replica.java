package refactor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import refactor.Messages.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Replica extends AbstractActor {
    public static Props props(int replicaId) {
        return Props.create(refactor.Replica.class, () -> new refactor.Replica(replicaId));
    }


    Utils.FileAdd file = new Utils.FileAdd("SystemLog.txt");

    /*
    ********************************************************************
    CRASH stuff
    ********************************************************************
    */

    private CrashMode crashEvent;

    /*
    ********************************************************************
    General stuff
    ********************************************************************
    */
    private final int replicaId;
    private final List<ActorRef> groupOfReplicas;
    private ActorRef successor;

    private final LinkedHashMap<Timestamp, Data> localHistory; // Hashmap of the history of values. It is used a linked list for maintain the insertion order.
    private Timestamp lastUpdate; // clock that contains the current epoch and sequence number of the last update
    private Timestamp lastStable;


    /*
   ********************************************************************
   Coordinator stuff
   ********************************************************************
    */
    private final Map<Timestamp, Integer> quorum; //Hashmap for the quorum. Every entry corresponds to a specific update (a coordinator might be waiting for more quorums at the same time)

    /*
   ********************************************************************
   Replica stuff
   ********************************************************************
    */
    private final Map<TimeoutType, Queue<Cancellable>> timeoutSchedule; //hashMap ot the timeouts of the replica.The value is a list because the SEND_HEARTBEAT is one for replica
                                                                        // and due to multiple elections there may be multiple election ack enqueued
    private ActorRef coordinator;

    /***************ELECTION PROTOCOL VARIABLES***************/
    private Queue<ReplicaMessages.ElectionMsg> tokenBuffer; //these are the token that, during election, get cached till the successor sends the ack back.
                                                   //it is a Queue of messages because there might be multiple elections going on concurrently
                                                    // and so multiple token sent to the successor one after the other
    private int candidateID; //The last best candidate known by this replica
    private Timestamp mostRecentUpdate; //the most recent update known by this replica, received from the token
                                        //These 2 variables are used to discriminate multiple tokens coming from multiple concurrent elections
    private boolean isCoordinatorElected; //used for ignoring travelling tokens when the first election is winned
    private int electionInstanceCounter; //used for distinguishing between old token (befor the ELECTION_PROTOCOL timeout) and the new tokens:
                                         //If the candidate with the last update crashes during the election, the token will circulate forever.
                                         //We need this counter for having a way to discard the old token with the best but crashed, candidate


    public Replica(int replicaId){
        this.replicaId = replicaId;
        this.successor = ActorRef.noSender(); //just for initialization
        this.groupOfReplicas = new ArrayList<ActorRef>();
        this.lastUpdate = Timestamp.defaultTimestamp(); //initialize with default value. The coordinator, when elected, will set (0,0)
        this.lastStable = Timestamp.defaultTimestamp();
        this.localHistory = new LinkedHashMap<Timestamp, Data>();
        this.localHistory.put(this.lastStable, Data.defaultData());
        this.quorum = new HashMap<Timestamp, Integer>();
        this.timeoutSchedule = new HashMap<TimeoutType, Queue<Cancellable>>();
        for(TimeoutType type : TimeoutType.values())
            this.timeoutSchedule.put(type, new LinkedList<Cancellable>());
        this.coordinator = ActorRef.noSender(); //just for initialization
        this.tokenBuffer = new LinkedList<ReplicaMessages.ElectionMsg>();
        this.candidateID = this.replicaId;
        this.mostRecentUpdate = Timestamp.defaultTimestamp();
        this.isCoordinatorElected = false;
        this.electionInstanceCounter = 0;
        this.crashEvent = new CrashMode(CrashMode.CrashType.NO_CRASH, -1);

    }

    /*
    ********************************************************************
    Define Behaviors
    ********************************************************************
    */
    @Override
    public Receive createReceive() {
        return receiveBuilder()

                .match(ReplicaMessages.TimeoutMsg.class, this::onTimeoutMsg)
                .match(ReplicaMessages.StartMsg.class, this::onStartMsg)
                .match(ReplicaMessages.ElectionMsg.class, this::onNewElectionMsg)

                .match(ClientMessages.ReadReqMsg.class, this::onReadReqMsg)
                .match(ClientMessages.WriteReqMsg.class, this::onReplicaWriteReqMsg)

                .match(CoordinatorMessages.UpdateMsg.class, this::onUpdateMsg)
                .match(CoordinatorMessages.WriteOKMsg.class, this::onWriteOKMsg)
                .match(CoordinatorMessages.HeartbeatMsg.class, this::onHeartbeatMsg)

                //.match(DebugMessages.CrashMsg.class, this::setCrashEvent)

                .matchAny(msg -> {})

                .build();
    }


    public Receive replicaDuringElectionBehavior() {
        // Define behavior for election state
        return receiveBuilder()

                .match(ReplicaMessages.TimeoutMsg.class, this::onTimeoutMsg)
                .match(ReplicaMessages.ElectionMsg.class, this::onAnotherElectionMsg)
                .match(ReplicaMessages.ElectionAckMsg.class, this:: onElectionAckMsg)

                .match(ClientMessages.ReadReqMsg.class, this::onReadReqMsg)

                .match(CoordinatorMessages.SyncMsg.class, this::onSyncMsg)

                .match(DebugMessages.CrashMsg.class, this::setCrashEvent)

                .matchAny(msg -> {})

                .build();

    }

    public Receive coordinatorBehavior() {
        // Define behavior for coordinator state
        return receiveBuilder()

                .match(ReplicaMessages.UpdateAckMsg.class, this::onUpdateAckMsg)

                .match(ClientMessages.ReadReqMsg.class, this::onReadReqMsg)
                .match(ClientMessages.WriteReqMsg.class, this::onCoordinatorWriteReqMsg)
                .match(ReplicaMessages.ElectionMsg.class, msg -> { //can happen, if a replica times out for whatever reason, e.g. because of heavy load
                    this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());
                    for(ActorRef replica : this.groupOfReplicas)
                        if(!replica.equals(this.getSelf())) {
                            delay();
                            replica.tell(new CoordinatorMessages.SyncMsg(), this.getSelf());
                        }
                })


                .match(DebugMessages.CrashMsg.class, this::setCrashEvent)

                .matchAny(msg -> {})

                .build();

    }

    public Receive crashedBehavior() {
        return receiveBuilder()
                .matchAny(msg -> {}) // Ignore all messages when crashed
                .build();
    }

    /*
    ********************************************************************
    Private auxiliary methods
    ********************************************************************
    */
    private ReplicaMessages.ElectionMsg defaultElectionMessage(){
        SortedSet<ReplicaMessages.ElectionMsg.LocalState> localState = new TreeSet<>();
        localState.add(new ReplicaMessages.ElectionMsg.LocalState(this.replicaId, this.lastUpdate, this.lastStable));
        return new ReplicaMessages.ElectionMsg(this.electionInstanceCounter, localState);
    }

    private void setTimeout(TimeoutType type, int time){
        int t = (time > 0) ? time : type.getMillis();

        //Timeout for the coordinator: this schedules the hearbeat message
        if(type == TimeoutType.SEND_HEARTBEAT){
            for(ActorRef replica : this.groupOfReplicas){
                if(!replica.equals(this.getSelf())){
                    Cancellable timeout = this.getContext().getSystem().scheduler().scheduleAtFixedRate(
                            Duration.ZERO,
                            Duration.ofMillis(t),
                            replica,
                            new CoordinatorMessages.HeartbeatMsg(),
                            getContext().system().dispatcher(),
                            this.getSelf()
                    );
                    this.timeoutSchedule.get(TimeoutType.SEND_HEARTBEAT).add(timeout);
                }
            }
        }
        //all the others timeouts
        else{
            Cancellable timeout = this.getContext().system().scheduler().scheduleOnce(
                    Duration.ofMillis(t),
                    this.getSelf(),
                    new ReplicaMessages.TimeoutMsg(type),
                    this.getContext().system().dispatcher(),
                    this.getSelf()
            );
            this.timeoutSchedule.get(type).add(timeout);
        }

    }

    private void removeTimeout(TimeoutType type){
        //remove the oldest timeout in the queue of the type specified.
        Queue<Cancellable> timeoutQueue = this.timeoutSchedule.get(type);
        if(timeoutQueue != null && !timeoutQueue.isEmpty()) {
            Cancellable timeout = timeoutQueue.poll();
            if(timeout != null)
                timeout.cancel();
        }
    }

    private void clearTimeoutType(TimeoutType type){
        //cancel and remove all timeouts of the specified type
        Queue<Cancellable> timeoutQueue = this.timeoutSchedule.get(type);
        if(timeoutQueue != null){
            Iterator<Cancellable> iterator = timeoutQueue.iterator();
            while(iterator.hasNext()) {
                Cancellable timeout = iterator.next();
                if(timeout != null) {
                    timeout.cancel();
                    iterator.remove(); // Safe removal while iterating
                }
            }
        }
        //System.out.println("the size of the queue is: "+ timeoutQueue.size());
    }

    private void setCrashEvent(DebugMessages.CrashMsg msg) {

        if(this.getSelf() == this.coordinator) { //crash modes dedicated to the current coordinator
            switch (msg.mode.type) {
                case INSTANT_CRASH -> {
                    this.crash();
                    return;
                }
                case BEFORE_WRITEREQ, DURING_UPDATE_BROADCAST, NO_CRASH -> {
                    System.out.println(this.getSelf().path().name() + " Setting crash mode:" + msg.mode.toString());
                    this.crashEvent = new CrashMode(msg.mode.type, msg.mode.param);
                }
                default -> {}
            }
        }
        else {
            switch (msg.mode.type){
                case CAND_COORD_AFTER_ACK, CAND_COORD_BEFORE_ACK, NO_CRASH -> {  //these are crashes that have to be set to all the replicas because we don't know who the coordinator will be
                    System.out.println(this.getSelf().path().name() + " Setting crash mode:" + msg.mode.toString());
                    this.crashEvent = new CrashMode(msg.mode.type, msg.mode.param);
                }
                default -> {}
            }

        }
    }

    private void crash(){
        //cancel all timeouts and crash
        System.out.println("** "+getSelf().path().name()+" ** IS CRASHED ...");
        for(TimeoutType timeouts : TimeoutType.values())
            this.clearTimeoutType(timeouts);
        this.getContext().become(crashedBehavior());

    }

    public static void delay() {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(6));
        }
        catch (Exception ignored){}
    }

    private void becomeCoordinator(SortedSet<ReplicaMessages.ElectionMsg.LocalState> localStates) throws ReplicaMessages.ElectionMsg.InconsistentTokenException {
        //become coordinator, send the sync message to the replica announcing that you are coordinator
        //and continue with the interrupted broadcasts
        System.out.println(this.getSelf().path().name() + " is becoming coordinator");
        System.out.flush();
        this.coordinator = this.getSelf();
        this.getContext().become(coordinatorBehavior());
        this.isCoordinatorElected = true;
        for(ActorRef replica : this.groupOfReplicas)
            if(!replica.equals(this.getSelf())){
                delay();
                replica.tell(new CoordinatorMessages.SyncMsg(), this.getSelf());
            }
        /*
        Now, regarding the states (last update and last stable update) of the replicas we can have four cases:
           (1) everyone has the last update corresponding to the last stable, so there is nothing to do
                (replicas detected the coordinator crash via the missing heartbeat message).
                It is also the case of the first election when the system starts up.
           (2) The last update is the same for everyone, but it is unstable (lastUpdate != lastStable) and the last
               stable message is the same for everyone: the coordinator crashed while it was gathering the quorum
               for this update (and possibly for older updates).
               In this case we have to re-ask for the quorum of all the updates from lastStable to lastUpdate
           (3) The last stable update is the same for everyone and there are some replicas that know one (and only one)
               lastUpdate that other replicas do not know: the coordinator crashed while it was broadcasting the UPDATE message.
               This update, say U, will always be the last one in the local history of the replicas that know U, due to FIFO links.
               In this case we have to send the UPDATE message to the replicas that do not know the UPDATE.
               For simplicity, we resend the update to all the replicas in order to re-trigger the ACK of all of them and
               gather the quorum, since we have to do the same also for the possible older unstable updates.
          (4) There are some replicas that know one (and only one) lastStable that other replicas do not know:
              the coordinator crashed while it was broadcasting the WRITEOK message. This update (say U) corresponds to the oldest
              unstable message of the replicas that do not know U as stable (because due to FIFO links, the only update that can become stable
              is the oldest unstable one). In this case we already know that the quorum was reached, so we can just send the WRITEOK
              message to the replicas that did not receive it.
              Furthermore, in this case there might be also more recent updates than this one that are unstable (that they
              did not reach the quorum yet). For these  messages, we have to re-trigger the ACK of the replica in order to
              gather the quorum. Since the coordinator was crashed while sending the WRITEOK of U, we know for sure that
              all the more recent unstable messages are known by ALL the replicas (due to the link assumptions).


        Due to the assumption on the links, these are the only cases possible (there cannot be, for example, a case in which (3) and (4)
        happens together, although it might happen if we remove the FIFO link assumption)
         */

        //case (1)
        if(localStates.last().compareTo(localStates.first()) == 0){
            return;
        }

        //case (2) and (3)
        else if(localStates.last().lastUpdate.compareTo(localStates.first().lastUpdate) >= 0
                && localStates.first().lastStable.compareTo(localStates.last().lastStable) == 0){
            Timestamp oldestUnstable = localStates.first().lastUpdate;
            //Timestamp missingUpdate = new Timestamp(oldestUnstable.getEpoch(),oldestUnstable.getSeqNum()+1);
            Timestamp missingUpdate = this.localHistory.containsKey(new Timestamp(oldestUnstable.getEpoch(),oldestUnstable.getSeqNum()+1)) ?
                    new Timestamp(oldestUnstable.getEpoch(),oldestUnstable.getSeqNum()+1) :
                    new Timestamp(oldestUnstable.getEpoch()+1,1);

            //iterate over the  localHistory and send all the missing updates
            while(missingUpdate.compareTo(this.lastUpdate) <= 0){
                for(ActorRef replica : this.groupOfReplicas)
                    if(!replica.equals(this.getSelf())) {
                        delay();
                        replica.tell(
                                new CoordinatorMessages.UpdateMsg(
                                        missingUpdate,
                                        this.localHistory.get(missingUpdate)
                                ),
                                this.getSelf()
                        );
                    }
                missingUpdate = missingUpdate.incrementSeqNum();
            }
        }
        //case (4)
        else if(localStates.last().lastUpdate.compareTo(localStates.first().lastUpdate) == 0
                && localStates.first().lastStable.compareTo(localStates.last().lastStable) > 0){

            Timestamp incompleteWriteOK = localStates.first().lastStable;
            //set the update as stable and broadcast the stability of the message
            this.localHistory.get(incompleteWriteOK).setStable(true);
            for(ActorRef replica : this.groupOfReplicas)
                if(!replica.equals(this.getSelf())) {
                    delay();
                    replica.tell(
                            new CoordinatorMessages.WriteOKMsg(incompleteWriteOK), this.getSelf()
                    );
                }
        }
        else throw new ReplicaMessages.ElectionMsg.InconsistentTokenException(localStates);
    }

    /*
    ********************************************************************
    Start message  handler
    ********************************************************************
    */
    public void onStartMsg(ReplicaMessages.StartMsg msg){
        System.out.println(getSelf().path().name()+" started.");
        System.out.flush();
        this.groupOfReplicas.addAll(msg.group);
        this.successor = this.groupOfReplicas.get((this.replicaId+1) % this.groupOfReplicas.size());
        //set a random timeout for the heartbeat in order to trigger one replica (and possibly only one) to begin an election
        this.setTimeout(TimeoutType.RECEIVE_HEARTBEAT, ThreadLocalRandom.current().nextInt(20,60));

    }

    /*
    ********************************************************************
    Timeout handler
    ********************************************************************
    */

    public void onTimeoutMsg(ReplicaMessages.TimeoutMsg msg){
        switch(msg.type){
            case UPDATE, WRITEOK, RECEIVE_HEARTBEAT, ELECTION_PROTOCOL -> {
                //Coordinator crash detected (Election went wrong), (re)start an election
                this.isCoordinatorElected = false;
                //TODO: there is still a problem here, in case of ELECTION_PROTOCOL timeout. Even if we restart the election, the old tokens with the most updated replica (but crashed during election) might be still around, and with the current algorithm there is no way to discard them. Simple solution is adding the election instance counter.
                if(msg.type == TimeoutType.ELECTION_PROTOCOL) {
                    this.getContext().unbecome();
                    this.tokenBuffer.clear();
                    this.electionInstanceCounter = this.electionInstanceCounter + 1;
                }
                System.out.println("** "+getSelf().path().name()+"** timeout first cause of "+msg.type.name()+" and start the Election protocol by sending an ElectionMsg to his successor **  **");
                System.out.flush();
                this.getContext().become(replicaDuringElectionBehavior());//first, become in election behavior
                for(TimeoutType timeouts : TimeoutType.values())
                    this.clearTimeoutType(timeouts);

                this.mostRecentUpdate = this.lastUpdate;
                this.candidateID = this.replicaId;

                ReplicaMessages.ElectionMsg token = defaultElectionMessage();
                this.tokenBuffer.add(token);
                delay();
                this.successor.tell(token, this.getSelf());
                this.setTimeout(TimeoutType.ELECTION_ACK, -1);
                this.setTimeout(TimeoutType.ELECTION_PROTOCOL, -1);
            }
            case ELECTION_ACK -> {
                if(!this.isCoordinatorElected) {
                    //Successor crashed, set a new successor
                    this.successor = this.groupOfReplicas.get(
                            (this.groupOfReplicas.indexOf(this.successor) + 1) % this.groupOfReplicas.size()
                    );
                    this.timeoutSchedule.get(TimeoutType.ELECTION_ACK).poll();
                    this.setTimeout(TimeoutType.ELECTION_ACK, -1);
                    //TODO: check if the queue is empty, or add a condition to do this only if your are still in election behavior
                    System.out.println("** "+getSelf().path().name()+" ** is TIMED OUT BECAUSE OF THE ELECTION_ACK and it is sending msg from buffer to ** "+this.successor.path().name()+" **");
                    delay();
                    this.successor.tell(this.tokenBuffer.peek(), this.getSelf());
                }
            }
        }
    }

    /*
    ********************************************************************
    Election protocol handlers
    ********************************************************************
    */

    //This method is called when the replica receives an election message and is NOT in election behavior
    public void onNewElectionMsg(ReplicaMessages.ElectionMsg msg){
        /*
        This function triggers the election behavior of the replica. The message received contains
        a candidate and the most recent known update by the replicas behind this one. If this replica has
        a more recent update, or it has the same update, but it has higher ID than the candidate, then it
        replaces the candidate and the most recent update in the token and forwards. Otherwise, the replica updates
        its own candidate and its own last known (by the system) update and forwards.
        */
        this.getContext().become(replicaDuringElectionBehavior());//first, become in election behavior
        this.isCoordinatorElected = false;
        this.electionInstanceCounter = msg.instanceCounter;
        for(TimeoutType timeouts : TimeoutType.values())
            this.clearTimeoutType(timeouts);
        this.setTimeout(TimeoutType.ELECTION_PROTOCOL, -1);


        //First see if you are a candidate coordinator and update your candidate accordingly
        int compare = this.lastUpdate.compareTo(msg.localStates.last().lastUpdate);
        if((compare > 0) || ((compare == 0) && (this.replicaId > msg.localStates.last().replicaID))){
            this.candidateID = this.replicaId;
            this.mostRecentUpdate = this.lastUpdate;
        }
        else{
            this.candidateID = msg.localStates.last().replicaID;
            this.mostRecentUpdate = msg.localStates.last().lastUpdate;
        }
        //Now put yourself in the token and forward
        SortedSet<ReplicaMessages.ElectionMsg.LocalState> tokenData = new TreeSet<>(msg.localStates);
        tokenData.add(new ReplicaMessages.ElectionMsg.LocalState(this.replicaId, this.lastUpdate, this.lastStable));
        ReplicaMessages.ElectionMsg token = new ReplicaMessages.ElectionMsg(msg.instanceCounter, tokenData);
        this.tokenBuffer.add(token);
        delay();
        this.successor.tell(token, this.getSelf());
        System.out.println("** "+this.getSelf().path().name()+" ** received an NewElectionMsg from ** "+getSender().path().name()+" ** and it is sending the message to it's successor ** "+this.successor.path().name()+" **  with the last condidate: "+msg.localStates.last().replicaID );
        System.out.flush();
        this.setTimeout(TimeoutType.ELECTION_ACK, -1);
        delay();
        this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());

    }

    //This method is called when the replica receives an election message and is in election behavior
    public void onAnotherElectionMsg(ReplicaMessages.ElectionMsg msg) throws ReplicaMessages.ElectionMsg.InconsistentTokenException {
        /*
        If you are already in election behavior nd you receive a token, 3(+1) things can happen:
        0)
            0.1) You receive a token that has an instance counter bigger than yours: some replica timed out for the election and began a more recent one, so do the same
            0.2) You receive a token that has an instance counter smaller than yours: it's an old token, discard it.
        1) You receive a token that brings a known more update recent than yours -> update your candidate coordinator and put yourself in the token if you are not in it.
        2) You receive a token that brings you as coordinator -> the token has completed the round and didn't find anyone more updated, so you become coordinator
        3) You receive a token that is less updated than you -> you can safely discard it, since for sure is a token of
            another election (not one that you already saw) because otherwise the most recent update of the token would be at least
            as updated as you.
         */

        System.out.println("** "+getSelf().path().name()+" ** is receiving onAntherElectionMsg from ** "+getSender().path().name()+" **");
        System.out.flush();

        //case 0): Manage protocol timeout
        if(msg.instanceCounter < this.electionInstanceCounter){
            //case 0.1) the token is old: discard and return
            System.out.println(this.getSelf().path().name() + " Case 0.1): receives an old token: it sends the ack and then discards it ");
            System.out.flush();
            delay();
            this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());
            return;
        }
        else if(msg.instanceCounter > this.electionInstanceCounter){
            //case 0.2) the token is from a new election instance, reset your variables and restart your protocol instance
            System.out.println(this.getSelf().path().name() + " Case 0.2): receives an token with instance counter bigger than its own: it restarts the protocol");
            System.out.flush();
            this.candidateID = this.replicaId;
            this.mostRecentUpdate = this.lastUpdate;
            this.getContext().unbecome();
            this.onNewElectionMsg(msg);
            return;
        }


        //case 1)
        int compare = this.mostRecentUpdate.compareTo(msg.localStates.last().lastUpdate);
        if((compare < 0) || ((compare == 0) && (this.replicaId < msg.localStates.last().replicaID))){
            this.candidateID = msg.localStates.last().replicaID;
            this.mostRecentUpdate = msg.localStates.last().lastUpdate;

            SortedSet<ReplicaMessages.ElectionMsg.LocalState> tokenData = new TreeSet<>(msg.localStates);
            tokenData.add(new ReplicaMessages.ElectionMsg.LocalState(this.replicaId, this.lastUpdate, this.lastStable));
            ReplicaMessages.ElectionMsg token = new ReplicaMessages.ElectionMsg(msg.instanceCounter, tokenData);
            this.tokenBuffer.add(token);
            delay();
            System.out.println("CASE 1:  ** "+getSelf().path().name()+" ** sending to his successor ** "+this.successor.path().name()+" ** an ElectionMsg with the last condidate: "+msg.localStates.last().replicaID);
            System.out.flush();
            this.successor.tell(token, this.getSelf());
            this.setTimeout(TimeoutType.ELECTION_ACK, -1);
            delay();
            this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());
        }
        //case 2)
        else if(compare == 0 && this.replicaId == msg.localStates.last().replicaID){
            if(this.crashEvent.type == CrashMode.CrashType.CAND_COORD_BEFORE_ACK){
                this.crash();
                return;
            }
            delay();
            this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());
            if(this.crashEvent.type == CrashMode.CrashType.CAND_COORD_AFTER_ACK){
                this.crash();
                return;
            }
            System.out.println("CASE 2:  ** "+getSelf().path().name()+" ** become coordinator with the last condidate: "+msg.localStates.last().replicaID);
            System.out.flush();
            this.becomeCoordinator(msg.localStates);
            this.lastUpdate = new Timestamp(this.lastUpdate.getEpoch() == Integer.MIN_VALUE ? 1 : this.lastUpdate.getEpoch()+1, 0);
            this.setTimeout(TimeoutType.SEND_HEARTBEAT, -1);
        }
        //case 3)
        else{
            delay();
            this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());
            System.out.println("CASE 3: ** "+getSelf().path().name()+" ** DROP the message from ** "+getSender().path().name()+" ** with the last condidate: "+msg.localStates.last().replicaID);

        }
    }

    public void onElectionAckMsg(ReplicaMessages.ElectionAckMsg msg){
        System.out.println(getSelf().path().name()+" is cancelling the ElectionAck from "+getSender().path().name());
        this.removeTimeout(TimeoutType.ELECTION_ACK);
        this.tokenBuffer.poll();
    }

    public void onSyncMsg(CoordinatorMessages.SyncMsg msg){
        this.getContext().unbecome();
        this.isCoordinatorElected = true;
        this.coordinator = this.getSender();
        this.removeTimeout(TimeoutType.ELECTION_PROTOCOL);
        this.clearTimeoutType(TimeoutType.ELECTION_ACK); //cancel all ACK timeout possibly still around due to concurrent elections
        this.tokenBuffer.clear();
        this.electionInstanceCounter = 0;
        //TODO: uncomment this and see what happens (cuz otherwise with new elections it cmay be wrong)
        //this.candidateID = this.replicaId;
        //this.mostRecentUpdate = Timestamp.defaultTimestamp();
    }

    public void onHeartbeatMsg(CoordinatorMessages.HeartbeatMsg msg){
        this.removeTimeout(TimeoutType.RECEIVE_HEARTBEAT);
        this.setTimeout(TimeoutType.RECEIVE_HEARTBEAT,-1);

    }


    /*
    ********************************************************************
    Read and write requests handlers
    ********************************************************************
    */

    public void onReadReqMsg(ClientMessages.ReadReqMsg msg){
        //send the last stable update (default value if the replica has no local history)
        delay();
        //System.out.println("LASTSTABLE: " + this.lastStable.toString());
        this.getSender().tell(
                (!this.localHistory.isEmpty() ? this.localHistory.get(this.lastStable).value : Data.defaultData().value),
                this.getSelf()
        );
    }

    public void onReplicaWriteReqMsg(ClientMessages.WriteReqMsg msg){
        //forward to coordinator
        System.out.println(this.getSelf().path().name()+" is receiving WriteRequest from "+this.getSender().path().name() + " with value: " + msg.getValue());

        delay();
        this.coordinator.tell(new ClientMessages.WriteReqMsg(msg.getValue()), this.getSelf());
        this.setTimeout(TimeoutType.UPDATE, -1);
    }

    /*
    ********************************************************************
    Two-phase broadcast handlers
    ********************************************************************
    */

    public void onCoordinatorWriteReqMsg(ClientMessages.WriteReqMsg msg){
        System.out.println(this.getSelf().path().name() + " crash event type:" +this.crashEvent.toString());
        if(this.crashEvent.type == CrashMode.CrashType.BEFORE_WRITEREQ) {
            this.crash();
            return;
        }
        //put the new value in the local history (as unstable) and send the update to the replicas
        this.lastUpdate = this.lastUpdate.incrementSeqNum();
        System.out.println("Coordinator " + this.getSelf().path().name()+" is receiving WriteRequest from "+this.getSender().path().name() + " with value: " + msg.getValue() + " and is assigning the timestamp: " + this.lastUpdate.toString());
        System.out.flush();
        Data newData = new Data(msg.getValue(), false);
        this.localHistory.put(this.lastUpdate, newData);
        this.quorum.put(this.lastUpdate, 1); //add yourself to the quorum for this update
        for(ActorRef replica : this.groupOfReplicas)
            if(!replica.equals(this.getSelf())){
                delay();
                if(this.crashEvent.type == CrashMode.CrashType.DURING_UPDATE_BROADCAST){
                    if(crashEvent.param == this.groupOfReplicas.indexOf(replica)) {
                        this.crash();
                        return;
                    }
                }
                replica.tell(new CoordinatorMessages.UpdateMsg(this.lastUpdate, newData), this.getSelf());
            }
    }

    public void onUpdateMsg(CoordinatorMessages.UpdateMsg msg){
        System.out.println(this.getSelf().path().name()+" is receiving an UPDATE message from the coordinator "+this.getSender().path().name() + " with timestamp: " + msg.timestamp);
        System.out.flush();
        //put the incoming unstable update into your local history and send the ACK to the coordinator
        this.removeTimeout(TimeoutType.UPDATE); //safely remove the timeout only if present
        this.lastUpdate = new Timestamp(msg.timestamp);
        this.localHistory.putIfAbsent(this.lastUpdate, new Data(msg.data));
        ReplicaMessages.UpdateAckMsg ack = new ReplicaMessages.UpdateAckMsg(this.lastUpdate);
        delay();
        System.out.println(this.getSelf().path().name()+" is sending UpdateAckMsg to "+this.getSender().path().name() + " for the udpdate: " + this.lastUpdate.toString());
        System.out.flush();
        this.getSender().tell(ack, this.getSelf());
        System.out.println("** "+getSelf().path().name()+" ** is setting out the timeOutType WRITEOK");
        System.out.flush();
        this.setTimeout(TimeoutType.WRITEOK, -1);
    }

    public void onUpdateAckMsg(ReplicaMessages.UpdateAckMsg msg){
        //add this ACK to the quorum of the specific message (if it is already stable, just ignore)
        System.out.println(this.getSelf().path().name()+" is receiving UpdateAckMsg from "+this.getSender().path().name() + " for the update: " + msg.timestamp.toString());
        System.out.flush();
        if(!this.quorum.containsKey(msg.timestamp)) this.quorum.put(msg.timestamp,1); //this is for new elected coordinators
        if(this.localHistory.get(msg.timestamp).isStable())
            return;
        int newQuorum = this.quorum.get(msg.timestamp)+1;
        this.quorum.put(msg.timestamp, newQuorum);

        if(newQuorum > (this.groupOfReplicas.size()/2)){
            System.out.println("Coordinator "+ this.getSelf().path().name()+" says: quorum reached for the update: " + msg.timestamp.toString());
            System.out.flush();
            //Quorum reached. Set the update stable and broadcast the WRITEOK
            this.localHistory.get(msg.timestamp).setStable(true);
            this.lastStable = new Timestamp(msg.timestamp);
            for(ActorRef replica : this.groupOfReplicas)
                if(!replica.equals(this.getSelf())) {
                    delay();
                    replica.tell(new CoordinatorMessages.WriteOKMsg(msg.timestamp), this.getSelf());
                }
            this.quorum.remove(msg.timestamp); //no more needed
        }
    }

    public void onWriteOKMsg(CoordinatorMessages.WriteOKMsg msg){
        System.out.println(this.getSelf().path().name()+" received the WRITEOK from "+this.getSender().path().name() + " for the update: " + msg.timestamp.toString());
        System.out.flush();
        file.appendToFile(this.getSelf().path().name()+" update "+ msg.timestamp.getEpoch() +":"+msg.timestamp.getSeqNum()+" <"+this.localHistory.get(msg.timestamp).value+">");
        this.removeTimeout(TimeoutType.WRITEOK);
        this.localHistory.get(msg.timestamp).setStable(true);
        this.lastStable = new Timestamp(msg.timestamp);
    }

}
