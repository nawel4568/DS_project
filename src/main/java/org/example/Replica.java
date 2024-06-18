package org.example;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.io.Serializable;


public class Replica extends AbstractActor {
    private final int replicaId;
    private String value;
    private int epoch; // it change if the coordinator change
    private int i; // it change every time there is an UPDATE
    private boolean isCoordinator;


    public static Props props(int replicaId) {
        return Props.create(Replica.class, () -> new Replica(replicaId));
    }

    public Replica(int replicaId) {
        this.replicaId = replicaId;
        this.isCoordinator = false;
        this.epoch = 0;
        this.i = 0;
        this.value = "";


    }
    
    public static class Recovery implements Serializable {}

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.ReadRequest.class, this::onReadRequest)
                .build();
    }

    public void crash(int interval){
        getContext().become(crashed());



    }
    public Receive crashed() {
        return receiveBuilder()
                .match(Recovery.class, this::onRecovery)
                .matchAny(msg -> {})
                .build();
    }
    public Receive onRecovery(){

    }

    public void onReadRequest(Messages.ReadRequest req){

    }


}
