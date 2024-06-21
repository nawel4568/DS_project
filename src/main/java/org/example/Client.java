package org.example;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class Client extends AbstractActor {
    private final int clientId;
    private int v;


    public Client(int clientId) {
        this.clientId = clientId;
    }

    public void setValue(int v) {
        this.v = v;
    }
    public int getValue() { return v; }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Integer.class, this::OnReceiveValue).build();
    }

    private void OnReceiveValue(Integer val){ this.v = val; }

    public static Props props(int clientId) {
        return Props.create(Client.class, () -> new Client(clientId));
    }


}
