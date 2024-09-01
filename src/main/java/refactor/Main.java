package refactor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import refactor.Messages.ClientMessages;
import refactor.Messages.DebugMessages;
import refactor.Messages.ReplicaMessages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    final static int N_REPLICAS = 10;
    final static int N_CLIENTS = 10;
    Random rand = new Random();


    public static void main(String[] args) {
        Utils.clearFileContent("SystemLog.txt");
        int increasingVal = 0;

        final ActorSystem system = ActorSystem.create("DistributedSystem");

        System.out.println("Bootstrapping the actors...");

        // Create the Group of Replicas
        List<ActorRef> group = new ArrayList<ActorRef>();
        for (int i=0; i<N_REPLICAS; i++)
            group.add(system.actorOf(refactor.Replica.props(i), "Replica" + i));

        // ensure that no one can modify the group
        group = Collections.unmodifiableList(group);

        // Send the Start message to all the Replicas
        //ReplicaMessages.StartMsg start = new ReplicaMessages.StartMsg(group);

        for (ActorRef peer: group) {
            peer.tell(new ReplicaMessages.StartMsg(group), ActorRef.noSender());
        }
        sleep(5000);





        // Create the clients
        List<ActorRef> clients = new ArrayList<ActorRef>();
        for (int i=0; i<N_CLIENTS; i++) {
            String logFile = "C" + i + "Log.txt";
            clients.add(system.actorOf(refactor.Client.props(i, logFile), "Client" + i));
            Utils.clearFileContent(logFile);
        }

        // ensure that no one can modify the group
        clients = Collections.unmodifiableList(clients);

        // Send the Start message to all the Replicas
        //ReplicaMessages.StartMsg start = new ReplicaMessages.StartMsg(group);

        for (ActorRef client : clients) {
            client.tell(new ReplicaMessages.StartMsg(group), ActorRef.noSender());
        }
        ActorRef scClient = system.actorOf(refactor.Client.props(100, "SeqConClient.txt"), "SeqConClient");
        scClient.tell(new ReplicaMessages.StartMsg(group), ActorRef.noSender());
        Utils.clearFileContent("SeqConClient.txt");

        sleep(1000);

        System.out.println("System started, press Enter to begin the simulation with basic read/write operation testing");
        inputContinue();
        scClient.tell(new ClientMessages.ReadScheduleMsg(1000, 2), ActorRef.noSender());

        System.out.println("Press Enter to make Client0 send a WRITEREQ with value 4 to Replica3");
        inputContinue();
        clients.get(0).tell(new ClientMessages.TriggerWriteOperation(3, increasingVal++), ActorRef.noSender());
        sleep(100);

        System.out.println("Press Enter to make Client1 send a READREQ to Replica9");
        inputContinue();
        clients.get(1).tell(new ClientMessages.TriggerReadOperation(9),ActorRef.noSender());
        sleep(100);

        System.out.println("Press Enter to make Client0 send a WRITEREQ with value 8 to Replica2");
        inputContinue();
        clients.get(0).tell(new ClientMessages.TriggerWriteOperation(2,increasingVal++), ActorRef.noSender());
        sleep(100);

        System.out.println("Press Enter to make Client0 send a READREQ to Replica3");
        inputContinue();
        clients.get(0).tell(new ClientMessages.TriggerReadOperation(3),ActorRef.noSender());
        sleep(100);

        System.out.println("Press Enter to continue the simulation testing concurrent operations");
        inputContinue();

        System.out.println("Testing multiple concurrent writes from different clients to different replicas");
        inputContinue();
        int numWrites = 6;
        for(int i=0; i < numWrites; i++)
            clients.get(i).tell(new ClientMessages.TriggerWriteOperation(ThreadLocalRandom.current().nextInt(N_REPLICAS), increasingVal++), ActorRef.noSender());
        sleep(1000);
        System.out.println("Performing a read operation from random clients to random replicas");
        clients.get(ThreadLocalRandom.current().nextInt(N_CLIENTS)).tell(new ClientMessages.TriggerReadOperation(ThreadLocalRandom.current().nextInt(N_REPLICAS)),ActorRef.noSender());
        clients.get(ThreadLocalRandom.current().nextInt(N_CLIENTS)).tell(new ClientMessages.TriggerReadOperation(ThreadLocalRandom.current().nextInt(N_REPLICAS)),ActorRef.noSender());
        clients.get(ThreadLocalRandom.current().nextInt(N_CLIENTS)).tell(new ClientMessages.TriggerReadOperation(ThreadLocalRandom.current().nextInt(N_REPLICAS)),ActorRef.noSender());
        sleep(500);

        System.out.println("Press Enter to test \"concurrent\" writes from different clients to the same replica");
        inputContinue();
        for(int i=0; i < numWrites; i++)
            clients.get(i).tell(new ClientMessages.TriggerWriteOperation(5, increasingVal++), ActorRef.noSender());
        sleep(1000);
        System.out.println("Performing a read operation from a client");
        clients.get(ThreadLocalRandom.current().nextInt(N_CLIENTS)).tell(new ClientMessages.TriggerReadOperation(ThreadLocalRandom.current().nextInt(N_REPLICAS)),ActorRef.noSender());

        System.out.println("Press Enter to test interleaved read and write operations to random replicas");
        inputContinue();
        for(int i=0; i<10; i++){
            clients.get(ThreadLocalRandom.current().nextInt(N_CLIENTS)).tell(new ClientMessages.TriggerWriteOperation(ThreadLocalRandom.current().nextInt(N_REPLICAS), increasingVal++), ActorRef.noSender());
            clients.get(ThreadLocalRandom.current().nextInt(N_CLIENTS)).tell(new ClientMessages.TriggerReadOperation(ThreadLocalRandom.current().nextInt(N_REPLICAS)),ActorRef.noSender());
        }
        sleep(1000);

        System.out.println("Press Enter to continue the simulation and test the crash events");
        inputContinue();
        System.out.print("Testing the crash event: replica tries to send a WRITEREQ to a coordinator that is crashed");
        System.out.println("Setting the crash mode...");
        for(ActorRef replica : group)
            //i don't know who the coordinator is, so I broadcast the crash mode and the replicas deal with filtering
            replica.tell(new DebugMessages.CrashMsg(new CrashMode(CrashMode.CrashType.BEFORE_WRITEREQ, -1)), ActorRef.noSender());
        sleep(1000);
        System.out.println("Press enter to test");
        inputContinue();
        clients.get(ThreadLocalRandom.current().nextInt(N_CLIENTS)).tell(new ClientMessages.TriggerWriteOperation(ThreadLocalRandom.current().nextInt(N_REPLICAS), increasingVal++), ActorRef.noSender());
        sleep(5000);

        System.out.println("Press Enter to test the crash event: coordinator crashes while broadcasting the UPDATE message");
        inputContinue();
        System.out.println("Setting the crash mode...");
        for(ActorRef replica : group)
            //I don't know who the coordinator is, so I broadcast the crash mode and the replicas deal with filtering
            replica.tell(new DebugMessages.CrashMsg(new CrashMode(CrashMode.CrashType.DURING_UPDATE_BROADCAST, 4)), ActorRef.noSender()); //Replica3 shoyld be become the new coordinator
        sleep(1000);
        System.out.println("Press enter to test");
        inputContinue();
        clients.get(ThreadLocalRandom.current().nextInt(N_CLIENTS)).tell(new ClientMessages.TriggerWriteOperation(ThreadLocalRandom.current().nextInt(N_REPLICAS), increasingVal++), ActorRef.noSender());
        sleep(5000);


        System.out.println("Press Enter to test the crash event: candidate coordinator crashes during the election, before sending the ack to the predecessor"); //predeccor times out for ELECTION_ACK
        inputContinue();
        System.out.println("Setting the crash mode...");
        for(ActorRef replica : group)
            //I don't know who the new coordinator will, so I broadcast the crash mode to set each replica to be in this crash mode
            replica.tell(new DebugMessages.CrashMsg(new CrashMode(CrashMode.CrashType.CAND_COORD_BEFORE_ACK, -1)), ActorRef.noSender()); //Replica3 shoyld be become the new coordinator
        sleep(1000);
        System.out.println("Press enter to test");
        inputContinue();
        System.out.println("Crashing the coordinator...");
        for(ActorRef replica : group)
            //I don't know who the coordinator is, so I broadcast the crash mode and the replicas deal with filtering
            replica.tell(new DebugMessages.CrashMsg(new CrashMode(CrashMode.CrashType.INSTANT_CRASH, -1)), ActorRef.noSender());
        clients.get(ThreadLocalRandom.current().nextInt(N_CLIENTS)).tell(new ClientMessages.TriggerWriteOperation(ThreadLocalRandom.current().nextInt(N_REPLICAS), increasingVal++), ActorRef.noSender());
        sleep(5000);

        System.out.println("Press Enter to test the crash event: candidate coordinator crashes during the election, after sending the ack to the predecessor but before becoming coordinator"); //token lost: replicas time out for ELECTION_PROTOCOL
        inputContinue();
        System.out.println("Setting the crash mode...");
        for(ActorRef replica : group)
            //I don't know who the new coordinator will, so I broadcast the crash mode to set each replica to be in this crash mode
            replica.tell(new DebugMessages.CrashMsg(new CrashMode(CrashMode.CrashType.CAND_COORD_AFTER_ACK, -1)), ActorRef.noSender()); //Replica3 shoyld be become the new coordinator
        sleep(1000);
        System.out.println("Press enter to test");
        inputContinue();
        System.out.println("Crashing the coordinator...");
        for(ActorRef replica : group)
            //I don't know who the coordinator is, so I broadcast the crash mode and the replicas deal with filtering
            replica.tell(new DebugMessages.CrashMsg(new CrashMode(CrashMode.CrashType.INSTANT_CRASH, -1)), ActorRef.noSender());
        clients.get(ThreadLocalRandom.current().nextInt(N_CLIENTS)).tell(new ClientMessages.TriggerWriteOperation(ThreadLocalRandom.current().nextInt(N_REPLICAS), increasingVal++), ActorRef.noSender());
        sleep(5000);


        System.out.println("Resetting crash modes...");
        for(ActorRef replica : group)
            replica.tell(new DebugMessages.CrashMsg(new CrashMode(CrashMode.CrashType.NO_CRASH, -1)), ActorRef.noSender()); //Replica3 shoyld be become the new coordinator
        sleep(100);

        inputContinue();
        sleep(100);
        system.terminate();


    }

    public static void inputContinue() {
        try {
            System.out.println(">>> Press ENTER to continue <<<");
            System.in.read();
        }
        catch (IOException ignored) {}
    }

    public static void sleep(int time) {
        try {
            Thread.sleep(time);
        }
        catch (Exception ignored){}
    }
}

