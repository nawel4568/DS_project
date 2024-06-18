package org.example;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class Client extends AbstractActor {
    private final int clientId;
    private int v;


    public Client(int clientId) {
        this.clientId = clientId;
    }
    public void setV(int v) {
        this.v = v;
    }

    @Override
    public Receive createReceive() {
        return null;
    }

    public static Props props(int clientId) {
        return Props.create(Client.class, () -> new Client(clientId));
    }


}
