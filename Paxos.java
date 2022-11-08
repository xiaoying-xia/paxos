package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.*;
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

    // Contains status information of this,
    // vary for different seq (or Paxos Procedures)
    HashMap<Integer, PaxosState> status;

    // Contains local memory of max seq of peers that consensus has been reached
    // done[me] = max seq of this
    int[] done;

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
        this.done = new int[peers.length];
        for (int i = 0; i < peers.length; i++) {
            done[i] = -1;
        }

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
        // Your code here

        // Run proposer procedure
        int seq = Integer.parseInt(Thread.currentThread().getName());
        int totalServers = this.peers.length;
        int maj = totalServers / 2;
        int id = this.status.get(seq).id;
        int maxId = -1;

        while (this.status.get(seq).state != State.Decided) {
            if (this.isDead()) break; // Break when this is dead
            // Choose id
            while (id <= maxId) {
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

                if (prepare_OK != null) {
                    this.done[i] = prepare_OK.done;
                    maxId = Math.max(prepare_OK.id, maxId);
                }

                if (prepare_OK != null && prepare_OK.accept) {
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
                }
                // Send Accept(id, v) to all
                Request accept = new Request(seq, this.status.get(seq).id, this.status.get(seq).value);
                int countAccept = 0;
                for (int i = 0; i < totalServers; i++) {
                    Response accept_OK;
                    if (i != this.me) {
                        accept_OK = Call("Accept", accept, i); // Call through RMI
                    } else {
                        accept_OK = this.Accept(accept); // Call self through direct function call
                    }
                    if (accept_OK != null) this.done[i] = accept_OK.done;
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
    }

    // RMI handler
    public Response Prepare(Request req){
        // your code here
        mutex.lock(); // Lock this
        try {
            // Create Entry Set for myself if I don't have one
            if (!this.status.containsKey(req.seq)) {
                this.status.put(req.seq, new PaxosState(req.seq, null, State.Pending, -1));
            }

            // Prepare Handler
            Response prepare_OK;
            if (req.id > this.status.get(req.seq).promisedId) {
                this.status.get(req.seq).promisedId = req.id;
                prepare_OK = new Response(this.status.get(req.seq).promisedId, this.status.get(req.seq).value, true, this.done[this.me]);
            } else prepare_OK = new Response(this.status.get(req.seq).promisedId, this.status.get(req.seq).value, false, this.done[this.me]); // Return highest id seen so far
            return prepare_OK;

        } finally {
            mutex.unlock();
        }
    }

    public Response Accept(Request req){
        // your code here
        mutex.lock();
        try {
            if (!this.status.containsKey(req.seq)) {
                System.out.println("Entry Set for seq: " + req.seq + ", idx: " + this.me + " created");
                this.status.put(req.seq, new PaxosState(req.seq, null, State.Pending, -1));
            }

            Response accept_OK;
            if (req.id >= this.status.get(req.seq).promisedId) {
                this.status.get(req.seq).promisedId = req.id;
                this.status.get(req.seq).value = req.value;
                accept_OK = new Response(this.status.get(req.seq).promisedId, this.status.get(req.seq).value, true, this.done[this.me]);
            } else accept_OK = new Response(this.status.get(req.seq).promisedId, this.status.get(req.seq).value, false, this.done[this.me]); // Return highest id seen so far
            return accept_OK;

        } finally {
            mutex.unlock();
        }
    }

    public Response Decide(Request req){
        // your code here
        mutex.lock();
        try {
            if (!this.status.containsKey(req.seq)) {
                this.status.put(req.seq, new PaxosState(req.seq, null, State.Pending, -1));
            }
            this.status.get(req.seq).promisedId = req.id;
            this.status.get(req.seq).value = req.value;
            this.status.get(req.seq).state = State.Decided;
            return null;

        } finally {
            mutex.unlock();
        }
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        if (done[this.me] < seq) {
            done[this.me] = seq;
        }
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        // Your code here
        mutex.lock();
        try {
            int maxSeq = -1;
            if (this.status.isEmpty()) return maxSeq;
            for (int i : this.status.keySet()) {
                maxSeq = Math.max(i, maxSeq);
            }
            return maxSeq;

        } finally {
            mutex.unlock();
        }
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
        // Find out the minimum done value
        mutex.lock();
        try {
            int minSeq = Integer.MAX_VALUE;
            for (int seqDone : this.done) {
                minSeq = Math.min(seqDone, minSeq);
            }
            minSeq += 1;

            // Discard instances with a seq lower than min
            // Use iterator to avoid ConcurrentModificationException
            Iterator<Map.Entry<Integer, PaxosState>> it = this.status.entrySet().iterator();
            while (it.hasNext()) {
                if (it.next().getKey() < minSeq) it.remove();
            }
            return minSeq;

        } finally {
            mutex.unlock();

        }

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
        mutex.lock();
        try {
            retStatus ret;
            if (seq < Min()) {
                ret = new retStatus(State.Forgotten, null);
            } else if (!this.status.containsKey(seq)) {
                ret = new retStatus(State.Pending, null);
            } else {
                ret = new retStatus(this.status.get(seq).state, this.status.get(seq).value);
            }
            return ret;

        } finally {
            mutex.unlock();
        }

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
