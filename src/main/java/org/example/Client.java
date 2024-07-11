package org.example;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class Client extends AbstractActor {
    private final static int TIMEOUT = 100;
    private final int clientId;
    FileAdd file = new FileAdd("output.txt");


    public Client(int clientId) {
        this.clientId = clientId;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Integer.class, this::OnReceiveValue).build();
    }



    private void OnReceiveValue(Integer val){
        file.appendToFile(getSelf().path().name()+" read done "+val);
        System.out.println(getSelf().path().name()+" read done "+val);}

    public static Props props(int clientId) {
        return Props.create(Client.class, () -> new Client(clientId));
    }


}
