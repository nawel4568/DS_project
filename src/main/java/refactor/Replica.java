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
    //private boolean isInElectionBehavior;
    private Queue<ReplicaMessages.ElectionMsg> tokenBuffer; //these are the token that, during election, get cached till the successor sends the ack back.
                                                   //it is a Queue of messages because there might be multiple elections going on concurrently
                                                    // and so multiple token sent to the successor one after the other
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
        this.tokenBuffer = new LinkedList<ReplicaMessages.ElectionMsg>();
        this.candidateID = this.replicaId;
        this.mostRecentUpdate = Timestamp.defaultTimestamp();

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
                .match(ClientMessages.WriteReqMsg.class, this::onCoordinatorWriteReqMsg)


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
    Private auxiliary methods
    ********************************************************************
    */
    private ReplicaMessages.ElectionMsg defaultElectionMessage(){
        SortedSet<ReplicaMessages.ElectionMsg.LocalState> localState = new TreeSet<>();
        localState.add(new ReplicaMessages.ElectionMsg.LocalState(this.replicaId, this.lastUpdate, this.lastStable));
        return new ReplicaMessages.ElectionMsg(localState);
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
        if(timeoutQueue != null)
            for(Cancellable timeout : timeoutQueue)
                if(timeout != null)
                    timeout.cancel();
    }

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

    private void becomeCoordinator(SortedSet<ReplicaMessages.ElectionMsg.LocalState> localStates) throws ReplicaMessages.ElectionMsg.InconsistentTokenException {
        //become coordinator, send the sync message to the replica announcing that you are coordinator
        //and continue with the interrupted broadcasts
        this.coordinator = this.getSelf();
        this.getContext().become(coordinatorBehavior());
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
            Timestamp missingUpdate = new Timestamp(oldestUnstable.getEpoch(),oldestUnstable.getSeqNum()+1);
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
                missingUpdate.incrementSeqNum();
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

        this.lastUpdate = new Timestamp(this.lastUpdate.getEpoch()+1, 0);
        this.setTimeout(TimeoutType.SEND_HEARTBEAT, -1);
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

    public void onTimeoutMsg(ReplicaMessages.TimeoutMsg msg){
        switch(msg.type){
            case UPDATE, WRITEOK, RECEIVE_HEARTBEAT, ELECTION_PROTOCOL -> {
                //Coordinator crash detected (Election went wrong), (re)start an election
                //TODO: there is still a problem here, in case of ELECTION_PROTOCOL timeout. Even if we restart the election, the old tokens with the most updated replica (but crashed during election) might be still around, and with the current algorithm there is no way to discard them. Simple solution is adding the election instance counter.
                if(msg.type == TimeoutType.ELECTION_PROTOCOL) {
                    this.getContext().unbecome();
                    this.tokenBuffer.clear();
                }
                this.getContext().become(replicaDuringElectionBehavior());//first, become in election behavior
                for(TimeoutType timeouts : TimeoutType.values())
                    this.clearTimeoutType(timeouts);

                ReplicaMessages.ElectionMsg token = defaultElectionMessage();
                this.tokenBuffer.add(token);
                delay();
                this.successor.tell(token, this.getSelf());
                this.setTimeout(TimeoutType.ELECTION_ACK, -1);
                this.setTimeout(TimeoutType.ELECTION_PROTOCOL, -1);
            }
            case ELECTION_ACK -> {
                //Successor crashed, set a new successor
                this.successor = this.groupOfReplicas.get(
                        (this.groupOfReplicas.indexOf(this.successor) + 1) % this.groupOfReplicas.size()
                );
                delay();
                //TODO: check if the queue is empty, or add a condition to do this only if your are still in election behavior
                this.successor.tell(this.tokenBuffer.peek(), this.getSelf());
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
        ReplicaMessages.ElectionMsg token = new ReplicaMessages.ElectionMsg(msg.localStates);
        token.localStates.add(new ReplicaMessages.ElectionMsg.LocalState(this.replicaId, this.lastUpdate, this.lastStable));
        this.tokenBuffer.add(token);
        delay();
        this.successor.tell(token, this.getSelf());
        this.setTimeout(TimeoutType.ELECTION_ACK, -1);
        delay();
        this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());

    }

    //This method is called when the replica receives an election message and is in election behavior
    public void onAnotherElectionMsg(ReplicaMessages.ElectionMsg msg) throws ReplicaMessages.ElectionMsg.InconsistentTokenException {
        /*
        If you are already in election behavior nd you receive a token, 3 things can happen:
        1) You receive a token that brings a known more update recent than yours -> update your candidate coordinator and put yourself in the token if you are not in it.
        2) You receive a token that brings you as coordinator -> the token has completed the round and didn't find anyone more updated, so you become coordinator
        3) You receive a token that is less updated than you -> you can safely discard it, since for sure is a token of
            another election (not one that you already saw) because otherwise the most recent update of the token would be at least
            as updated as you.
         */

        //case 1)
        int compare = this.mostRecentUpdate.compareTo(msg.localStates.last().lastUpdate);
        if((compare < 0) || ((compare == 0) && (this.replicaId < msg.localStates.last().replicaID))){
            this.candidateID = msg.localStates.last().replicaID;
            this.mostRecentUpdate = msg.localStates.last().lastUpdate;

            ReplicaMessages.ElectionMsg token = new ReplicaMessages.ElectionMsg(msg.localStates);
            token.localStates.add(new ReplicaMessages.ElectionMsg.LocalState(this.replicaId, this.lastUpdate, this.lastStable));
            this.tokenBuffer.add(token);
            delay();
            this.successor.tell(token, this.getSelf());
            this.setTimeout(TimeoutType.ELECTION_ACK, -1);
            delay();
            this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());
        }
        //case 2)
        else if(compare == 0 && this.replicaId == msg.localStates.last().replicaID){
            delay();
            this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());
            this.becomeCoordinator(msg.localStates);
        }
        //case 3)
        else{
            delay();
            this.getSender().tell(new ReplicaMessages.ElectionAckMsg(), this.getSelf());
        }
    }

    public void onElectionAckMsg(ReplicaMessages.ElectionAckMsg msg){
        this.removeTimeout(TimeoutType.ELECTION_ACK);
        this.tokenBuffer.poll();
    }

    public void onSyncMsg(CoordinatorMessages.SyncMsg msg){
        this.getContext().unbecome();
        this.coordinator = this.getSender();
        this.removeTimeout(TimeoutType.ELECTION_PROTOCOL);
        this.clearTimeoutType(TimeoutType.ELECTION_ACK); //cancel all ACK timeout possibly still around due to concurrent elections
        this.tokenBuffer.clear();
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
        this.getSender().tell(
                (!this.localHistory.isEmpty() ? this.localHistory.get(this.lastStable).value : Data.defaultData().value),
                this.getSelf()
        );
    }

    public void onReplicaWriteReqMsg(ClientMessages.WriteReqMsg msg){
        //forward to coordinator
        delay();
        this.coordinator.tell(msg, this.getSelf());
        this.setTimeout(TimeoutType.UPDATE, -1);
    }

    /*
    ********************************************************************
    Two-phase broadcast handlers
    ********************************************************************
    */

    public void onCoordinatorWriteReqMsg(ClientMessages.WriteReqMsg msg){
        //put the new value in the local history (as unstable) and send the update to the replicas
        this.lastUpdate.incrementSeqNum();
        Data newData = new Data(msg.getV(), false);
        this.localHistory.put(this.lastUpdate, newData);
        this.quorum.put(this.lastUpdate, 1); //add yourself to the quorum for this update
        for(ActorRef replica : this.groupOfReplicas)
            if(!replica.equals(this.getSelf())){
                delay();
                replica.tell(new CoordinatorMessages.UpdateMsg(this.lastUpdate, newData), this.getSelf());
            }
    }

    public void onUpdateMsg(CoordinatorMessages.UpdateMsg msg){
        //put the incoming unstable update into your local history and send the ACK to the coordinator
        this.removeTimeout(TimeoutType.UPDATE); //safely remove the timeout only if present
        this.lastUpdate = msg.timestamp;
        this.localHistory.putIfAbsent(this.lastUpdate, msg.data);
        ReplicaMessages.UpdateAckMsg ack = new ReplicaMessages.UpdateAckMsg(msg.timestamp);
        delay();
        this.getSender().tell(ack, this.getSelf());
        this.setTimeout(TimeoutType.WRITEOK, -1);
    }

    public void onUpdateAckMsg(ReplicaMessages.UpdateAckMsg msg){
        //add this ACK to the quorum of the specific message (if it is already stable, just ignore)
        if(this.localHistory.get(msg.timestamp).isStable())
            return;
        int newQuorum = this.quorum.get(msg.timestamp)+1;
        this.quorum.put(msg.timestamp, newQuorum);

        if(newQuorum > (this.groupOfReplicas.size()/2)){
            //Quorum reached. Set the update stable and broadcast the WRITEOK
            this.localHistory.get(msg.timestamp).setStable(true);
            for(ActorRef replica : this.groupOfReplicas)
                if(!replica.equals(this.getSelf())) {
                    delay();
                    replica.tell(new CoordinatorMessages.WriteOKMsg(msg.timestamp), this.getSelf());
                }
            this.quorum.remove(msg.timestamp); //no more needed
        }
    }

    public void onWriteOKMsg(CoordinatorMessages.WriteOKMsg msg){
        this.removeTimeout(TimeoutType.WRITEOK);
        this.localHistory.get(msg.timestamp).setStable(true);
        this.lastStable = msg.timestamp;
    }

    /*
    ********************************************************************
    Debug messages handlers
    ********************************************************************
    */

    public void onPrintHistoryMsg(DebugMessages.PrintHistoryMsg msg){}





}
