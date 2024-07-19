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
        return Props.create(org.example.Replica.class, () -> new org.example.Replica(replicaId));
    }


    /*
    ********************************************************************
    Debug stuff
    ********************************************************************
    */
    //private int cnt = 0;

    /*
    ********************************************************************
    General stuff
    ********************************************************************
    */
    Utils.FileAdd file = new Utils.FileAdd("output.txt");
    private final int replicaId;
    private final List<ActorRef> groupOfReplicas;
    private ActorRef successor;

    private final LinkedHashMap<Timestamp, Data> localHistory; // Hashmap of the history of values. It is used a linked list for maintaingn the insertion order.
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
    //private boolean isInElectionBehavior;
    private ReplicaMessages.ElectionMsg cachedMsg; //this is the election message that, during election, remain cached till the election ack is received
    private int candidateID; //The last best candidate known by this replica
    private Timestamp mostRecentUpdate; //the most recent update known by this replica, received from the token
                                        //These 2 variables are used to discriminate multiple tokens coming from multiple concurrent elections


    public Replica(int replicaId){
        this.replicaId = replicaId;
        this.successor = ActorRef.noSender(); //just for initialization
        this.groupOfReplicas = new ArrayList<ActorRef>();
        this.localHistory = new LinkedHashMap<Timestamp, Data>();
        this.lastUpdate = Timestamp.defaultTimestamp(); //initialize with default value. The coordinator, when elected, will set (0,0)
        this.lastStable = Timestamp.defaultTimestamp();
        this.quorum = new HashMap<Timestamp, Integer>();
        this.timeoutSchedule = new HashMap<TimeoutType, Queue<Cancellable>>();
        for(TimeoutType type : TimeoutType.values())
            this.timeoutSchedule.put(type, new LinkedList<Cancellable>());
        this.coordinator = ActorRef.noSender(); //just for initialization
        this.cachedMsg = defaultElectionMessage();
        this.candidateID = this.replicaId;
        this.mostRecentUpdate = Timestamp.defaultTimestamp();

    }

    private ReplicaMessages.ElectionMsg defaultElectionMessage(){
        SortedSet<ReplicaMessages.ElectionMsg.ReplicaUpdate> replicaUpdate = new TreeSet<>();
        replicaUpdate.add(new ReplicaMessages.ElectionMsg.ReplicaUpdate(this.replicaId, this.lastUpdate, this.lastStable));
        return new ReplicaMessages.ElectionMsg(replicaUpdate);
    }
    /*
    ********************************************************************
    Private auxiliary methods
    ********************************************************************
    */
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
        this.timeoutSchedule.get(type).poll().cancel();
        }

    private void clearTimeoutType(TimeoutType type){
        //cancel and remove all timeouts of the specified type
        Queue<Cancellable> timeoutQueue = this.timeoutSchedule.get(type);
        for(Cancellable timeout : timeoutQueue)
            timeout.cancel();
    }


    private void becomeCoordinator(SortedSet<ReplicaMessages.ElectionMsg.ReplicaUpdate> replicaUpdates){}


    private void crash(){
        //cancel all timeouts and crash
        for(TimeoutType timeouts : TimeoutType.values())
            this.clearTimeoutType(timeouts);
        this.getContext().become(crashedBehavior());
    }

    public static void delay() {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(21));
        }
        catch (Exception ignored){}
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
                .match(ClientMessages.WriteReqMsg.class, this::onWriteReqMsg)

                .match(CoordinatorMessages.UpdateMsg.class, this::onUpdateMsg)
                .match(CoordinatorMessages.WriteOKMsg.class, this::onWriteOKMsg)
                .match(CoordinatorMessages.HeartbeatMsg.class, this::onHeartbeatMsg)

                .match(DebugMessages.PrintHistoryMsg.class, this::onPrintHistoryMsg)

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

                .match(DebugMessages.PrintHistoryMsg.class, this::onPrintHistoryMsg)

                .matchAny(msg -> {})

                .build();

    }

    public Receive coordinatorBehavior() {
        // Define behavior for coordinator state
        return receiveBuilder()

                .match(ReplicaMessages.UpdateAckMsg.class, this::onUpdateAckMsg)

                .match(ClientMessages.ReadReqMsg.class, this::onReadReqMsg)
                .match(ClientMessages.WriteReqMsg.class, this::onWriteReqMsg)


                .match(DebugMessages.PrintHistoryMsg.class, this::onPrintHistoryMsg)

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
    Start message  handler
    ********************************************************************
    */
    public void onStartMsg(ReplicaMessages.StartMsg msg){
        this.groupOfReplicas.addAll(msg.group);
        this.successor = this.groupOfReplicas.get((this.replicaId+1) % this.groupOfReplicas.size());
        //set a random timeout for the heartbeat in order to trigger one replica (and possibly only one) to begin an election
        setTimeout(TimeoutType.RECEIVE_HEARTBEAT, ThreadLocalRandom.current().nextInt(50));

    }

    /*
    ********************************************************************
    Timeout handler
    ********************************************************************
    */

    public void onTimeoutMsg(ReplicaMessages.TimeoutMsg msg){}

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
        for(TimeoutType timeouts : TimeoutType.values())
            this.clearTimeoutType(timeouts);
        this.setTimeout(TimeoutType.ELECTION_PROTOCOL, -1);


        //First see if you are a candidate coordinator and update your candidate accordingly
        int compare = this.lastUpdate.compareTo(msg.replicaUpdates.last().lastUpdate);
        if((compare > 0) || ((compare == 0) && (this.replicaId > msg.replicaUpdates.last().replicaID))){
            this.candidateID = this.replicaId;
            this.mostRecentUpdate = this.lastUpdate;
        }
        else{
            this.candidateID = msg.replicaUpdates.last().replicaID;
            this.mostRecentUpdate = msg.replicaUpdates.last().lastUpdate;
        }
        //Now put yourself in the token and forward
        this.cachedMsg = new ReplicaMessages.ElectionMsg(msg.replicaUpdates);
        this.cachedMsg.replicaUpdates.add(new ReplicaMessages.ElectionMsg.ReplicaUpdate(this.replicaId, this.lastUpdate, this.lastStable));
        delay();
        this.successor.tell(this.cachedMsg, this.getSelf());
        this.setTimeout(TimeoutType.ELECTION_ACK, -1);
        delay();
        this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());

    }

    //This method is called when the replica receives an election message and is in election behavior
    public void onAnotherElectionMsg(ReplicaMessages.ElectionMsg msg){
        /*
        If you are already in election behavior nd you receive a token, 3 things can happen:
        1) You receive a token that brings a known more update recent than yours -> update your candidate coordinator and put yourself in the token if you are not in it.
        2) You receive a token that brings you as coordinator -> the token has completed the round and didn't find anyone more updated, so you become coordinator
        3) You receive a token that is less updated than you -> you can safely discard it, since for sure is a token of
            another election (not one that you already saw) because otherwise the most recent update of the token would be at least
            as updated as you.
         */

        //case 1)
        int compare = this.mostRecentUpdate.compareTo(msg.replicaUpdates.last().lastUpdate);
        if((compare < 0) || ((compare == 0) && (this.replicaId < msg.replicaUpdates.last().replicaID))){
            this.candidateID = msg.replicaUpdates.last().replicaID;
            this.mostRecentUpdate = msg.replicaUpdates.last().lastUpdate;

            this.cachedMsg = new ReplicaMessages.ElectionMsg(msg.replicaUpdates);
            this.cachedMsg.replicaUpdates.add(new ReplicaMessages.ElectionMsg.ReplicaUpdate(this.replicaId, this.lastUpdate, this.lastStable));
            delay();
            this.successor.tell(this.cachedMsg, this.getSelf());
            this.setTimeout(TimeoutType.ELECTION_ACK, -1);
            delay();
            this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());
        }
        //case 2)
        else if(compare == 0 && this.replicaId == msg.replicaUpdates.last().replicaID){
            delay();
            this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());
            this.becomeCoordinator(msg.replicaUpdates);
        }
        //case 3)
        else{
            delay();
            this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());
        }


    }
    public void onElectionAckMsg(ReplicaMessages.ElectionAckMsg msg){
        this.removeTimeout(TimeoutType.ELECTION_ACK);
    }
    public void onSyncMsg(CoordinatorMessages.SyncMsg msg){
        this.removeTimeout(TimeoutType.ELECTION_PROTOCOL);
        this.clearTimeoutType(TimeoutType.ELECTION_ACK); //cancel all possibly ACK timeout still around due to concurrent elections
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

        file.appendToFile(this.getSender().path().name()+" read req to "+this.getSelf().path().name());

    }
    public void onWriteReqMsg(ClientMessages.WriteReqMsg msg){
        this.setTimeout(TimeoutType.UPDATE, -1);
    }

    /*
    ********************************************************************
    Two-phase broadcast handlers
    ********************************************************************
    */

    public void onUpdateMsg(CoordinatorMessages.UpdateMsg msg){
        this.removeTimeout(TimeoutType.UPDATE);
        this.setTimeout(TimeoutType.WRITEOK, -1);
    }
    public void onUpdateAckMsg(ReplicaMessages.UpdateAckMsg msg){}
    public void onWriteOKMsg(CoordinatorMessages.WriteOKMsg msg){
        this.removeTimeout(TimeoutType.WRITEOK);
    }

    /*
    ********************************************************************
    Debug messages handlers
    ********************************************************************
    */

    public void onPrintHistoryMsg(DebugMessages.PrintHistoryMsg msg){}





}
