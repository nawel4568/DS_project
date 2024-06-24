package org.example;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class Client extends AbstractActor {
    private final int clientId;


    public Client(int clientId) {
        this.clientId = clientId;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Integer.class, this::OnReceiveValue).build();
    }

    public int getClientId(){
        return clientId;
    }

    private void OnReceiveValue(Integer val){ System.out.println(getSelf().path().name()+" read done "+val);}

    public static Props props(int clientId) {
        return Props.create(Client.class, () -> new Client(clientId));
    }


}
