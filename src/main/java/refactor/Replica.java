package refactor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import refactor.Messages.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

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
    private ActorRef successor;
    private final List<ActorRef> groupOfReplicas;

    private final LinkedHashMap<Timestamp, Data> localHistory; // Hashmap of the history of values. It is used a linked list for maintaingn the insertion order.


    /*
   ********************************************************************
   Coordinator stuff
   ********************************************************************
    */
    private final List<Cancellable> heartbeatScheduler; //list of the scheduled messages, there is one schchedule for each replica
    private Timestamp clock; // clock that contains the current epoch and sequence number of the last update

    private final HashMap<Timestamp, Integer> quorum; //Hashmap for the quorum. Every entry corresponds to a specific update (a coordinator might be waiting for more quorums at the same time)

    /*
   ********************************************************************
   Replica stuff
   ********************************************************************
    */
    private final HashMap<TimeoutType, Cancellable> timeoutSchedule; //hashMpa ot the timeouts of the replica (not coordinator)
    private ActorRef coordinator;

    private boolean isInElectionBehavior;
    private ReplicaMessages.ElectionMsg cachedMsg; //this is the election message that, during election, remain cached till the election ack is received

    private Integer candidateID;
    private Timestamp lastKnownUpdate;


    @Override
    public Receive createReceive() {
        return receiveBuilder()

                .match(ReplicaMessages.TimeoutMsg.class, this::onTimeoutMsg)
                .match(ReplicaMessages.StartReplicaMsg.class, this::onStartMessage)
                .match(ReplicaMessages.ElectionMsg.class, this::onElectionMsg)

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
                .match(ReplicaMessages.ElectionMsg.class, this::onElectionMsg)
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
    Stat message  handler
    ********************************************************************
    */
    public void onStartMsg(ReplicaMessages.StartReplicaMsg msg){}

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

    public void onElectionMsg(ReplicaMessages.ElectionMsg msg ){}
    public void onElectionAckMsg(ReplicaMessages.ElectionAckMsg msg){}
    public void onSyncMsg(CoordinatorMessages.SyncMsg msg){}
    public void onHeartbeatMsg(CoordinatorMessages.HeartbeatMsg msg){}


    /*
    ********************************************************************
    Read and write requests handlers
    ********************************************************************
    */

    public void onReadReqMsg(ClientMessages.ReadReqMsg msg){}
    public void onWriteReqMsg(ClientMessages.WriteReqMsg msg){}

    /*
    ********************************************************************
    Two-phase broadcast handlers
    ********************************************************************
    */

    public void onUpdateMsg(CoordinatorMessages.UpdateMsg msg){}
    public void onUpdateAckMsg(ReplicaMessages.UpdateAckMsg msg){}
    public void onWriteOKMsg(CoordinatorMessages.WriteOKMsg msg){}

    /*
    ********************************************************************
    Debug messages handlers
    ********************************************************************
    */

    public void onPrintHistoryMsg(DebugMessages.PrintHistoryMsg msg){}





}
