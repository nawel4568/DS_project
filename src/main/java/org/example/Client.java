package org.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public class Client extends AbstractActor {
    private final static int READ_REQ = 500;
    private final int clientId;
    Utils.FileAdd file = new Utils.FileAdd("output.txt");


    public Client(int clientId) {
        this.clientId = clientId;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Integer.class, this::onReceiveValue).build();
    }





    private void onReceiveValue(Integer val){

        System.out.println("\nClient "+getSelf().path().name() + " received value " + val+ " from "+getSender().path().name()+"\n");
        System.out.flush();
        file.appendToFile("\n"+getSelf().path().name()+" read done "+val+"\n");
        System.out.println(getSelf().path().name()+" read done "+val);
    }

    public static Props props(int clientId) {
        return Props.create(Client.class, () -> new Client(clientId));
    }


}
