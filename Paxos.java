package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is the main class you need to implement paxos instances.
 */

class PaxosState {

    // As proposer
    int id; // Proposing id, may vary with time
    Object value; // Proposing Value
    State state; // Current state

    // As accepter
    int promisedId; // Promised id

    public PaxosState(int id, Object value, State state, int promisedId) {
        this.id = id;
        this.value = value;
        this.state = state;
        this.promisedId = promisedId;
    }
}

public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex; // lock
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here
    HashMap<Integer, PaxosState> status; // vary for different seq (or Paxos Procedures)
//    int seq; // round of Paxos

    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
        this.status = new HashMap<>();

        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]); // Set system-wide hostname
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;

        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */

    public void Start(int seq, Object value){ // seq: round
        // Your code here
        mutex.lock();
        try {
            if (!this.status.containsKey(seq)) {
//                System.out.println("Entry Set for seq: " + seq + ", idx: " + this.me + " created");
                this.status.put(seq, new PaxosState(this.me, value, State.Pending, -1));
            } else {
                if (this.status.get(seq).state != State.Decided) {
                    this.status.get(seq).value = value;
                }
            }
        } finally {
            mutex.unlock();
        }

        Thread thread = new Thread(this, String.valueOf(seq));
        thread.start();

    }

    @Override
    public void run(){
        //Your code here
        // Run proposer procedure
//        System.out.println("Thread Created from server: " + me);
        int seq = Integer.parseInt(Thread.currentThread().getName());
        int totalServers = this.peers.length;
        int maj = totalServers / 2;
        int id = this.status.get(seq).id;
        int maxId = -1;

//        mutex.lock(); // Lock this.status.get(seq) because we need to write it
        while (this.status.get(seq).state != State.Decided) {
            // Choose id
            while (id <= maxId || id == this.me) {
                id += totalServers;
            }
            this.status.get(seq).id = id;

            // Send prepare(id) to all
            Request prepare = new Request(seq, this.status.get(seq).id, null);
            int countPrepare = 0;
            int idA = -1;
            Object valueA = null;
            for (int i = 0; i < totalServers; i++) {
                Response prepare_OK = Call("Prepare", prepare, i);
                if (prepare_OK != null) maxId = Math.max(prepare_OK.id, maxId);

                if (prepare_OK != null && prepare_OK.accept) {
//                    if (seq == 1 && this.me == 0) {
//                        System.out.println(prepare_OK.value);
//                    }
                    countPrepare++;
                    // Memorize returned value with max id
                    if (prepare_OK.value != null && prepare_OK.id > idA) {
                        idA = prepare_OK.id;
                        valueA = prepare_OK.value;
                    }
                }
            }

            // Receive majority of prepare_OK
            if (countPrepare > maj) {
                // Consensus has been reached
                if (valueA != null) {
                    this.status.get(seq).value = valueA;
//                    this.status.get(seq).state = State.Decided;
                }
                // Send Accept(id, v) to all
                Request accept = new Request(seq, this.status.get(seq).id, this.status.get(seq).value);
                int countAccept = 0;
                for (int i = 0; i < totalServers; i++) {
                    Response accept_OK = Call("Accept", accept, i);
                    if (accept_OK != null && accept_OK.accept) countAccept++;
                }
                // Send Decide(v) to all
                if (countAccept > maj) {
                    Request decide = new Request(seq, this.status.get(seq).id, this.status.get(seq).value);
                    for (int i = 0; i < totalServers; i++) {
                        Call("Decide", decide, i);
                    }
                    this.status.get(seq).state = State.Decided;
                }

            }
        }
//        mutex.unlock();


    }

    // RMI handler
    public Response Prepare(Request req){
        // your code here
        // Create Entry Set for myself if I don't have one

//        synchronized (this.status.get(req.seq)) {
//
//        }
        mutex.lock();

        if (!this.status.containsKey(req.seq)) {
//            System.out.println("Entry Set for seq: " + req.seq + ", idx: " + this.me + " created");
            this.status.put(req.seq, new PaxosState(req.seq, null, State.Pending, -1));
        }

        // Prepare Handler
        Response prepare_OK = null;
//        if (this.me == 1 && req.seq == 1) {
//            System.out.println(req.id + " " + this.status.get(req.seq).promisedId + " " + this.status.get(req.seq).value);
//        }
        if (req.id > this.status.get(req.seq).promisedId) {
            this.status.get(req.seq).promisedId = req.id;
            prepare_OK = new Response(this.status.get(req.seq).promisedId, this.status.get(req.seq).value, true);
        } else prepare_OK = new Response(this.status.get(req.seq).promisedId, this.status.get(req.seq).value, false);
        mutex.unlock();
        return prepare_OK;

    }

    public Response Accept(Request req){
        // your code here
        mutex.lock();
        if (!this.status.containsKey(req.seq)) {
            System.out.println("Entry Set for seq: " + req.seq + ", idx: " + this.me + " created");
            this.status.put(req.seq, new PaxosState(req.seq, null, State.Pending, -1));
        }

        Response accept_OK = null;
        if (req.id >= this.status.get(req.seq).promisedId) {
            this.status.get(req.seq).promisedId = req.id;
            this.status.get(req.seq).value = req.value;
            accept_OK = new Response(this.status.get(req.seq).promisedId, this.status.get(req.seq).value, true);
        } else accept_OK = new Response(this.status.get(req.seq).promisedId, this.status.get(req.seq).value, false);
        mutex.unlock();
        return accept_OK;
    }

    public Response Decide(Request req){
        // your code here
        mutex.lock();
        if (!this.status.containsKey(req.seq)) {
            System.out.println("Entry Set for seq: " + req.seq + ", idx: " + this.me + " created");
            this.status.put(req.seq, new PaxosState(req.seq, null, State.Pending, -1));
        }
        this.status.get(req.seq).value = req.value;
        this.status.get(req.seq).state = State.Decided;
        mutex.unlock();
        return null;
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        // Your code here
        return 0;
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        // Your code here
        return 1;
    }



    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq){
        // Your code here
        retStatus ret = null;
        if (!this.status.containsKey(seq)) {
            ret = new retStatus(State.Pending, null);
        } else {
            ret = new retStatus(this.status.get(seq).state, this.status.get(seq).value);
        }
        return ret;
    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable(){
        return this.unreliable.get();
    }


}
