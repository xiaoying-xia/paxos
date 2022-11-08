package paxos;

import java.lang.Object;

import static org.junit.Assert.assertFalse;


class Test {

	private static int ndecided(Paxos[] pxa, int seq){
        int counter = 0;
        Object v = null;
        Paxos.retStatus ret;
        for(int i = 0; i < pxa.length; i++){
            if(pxa[i] != null){
                ret = pxa[i].Status(seq);
                if(ret.state == State.Decided) {
                    // assertFalse("decided values do not match: seq=" + seq + " i=" + i + " v=" + v + " v1=" + ret.v, counter > 0 && !v.equals(ret.v));
                    counter++;
                    v = ret.v;
                }

            }
        }
        return counter;
    }

    private static void waitn(Paxos[] pxa, int seq, int wanted){
        int to = 10;
        for(int i = 0; i < 30; i++){
            if(ndecided(pxa, seq) >= wanted){
                break;
            }
            try {
                Thread.sleep(to);
            } catch (Exception e){
                e.printStackTrace();
            }
            if(to < 1000){
                to = to * 2;
            }
        }

        int nd = ndecided(pxa, seq);
//        assertFalse("too few decided; seq=" + seq + " ndecided=" + nd + " wanted=" + wanted, nd < wanted);

    }

    private static void waitmajority(Paxos[] pxa, int seq){
        waitn(pxa, seq, (pxa.length/2) + 1);
    }

    private static void cleanup(Paxos[] pxa){
        for(int i = 0; i < pxa.length; i++){
            if(pxa[i] != null){
                pxa[i].Kill();
            }
        }
    }

    private Paxos[] initPaxos(int npaxos){
        String host = "127.0.0.1";
        String[] peers = new String[npaxos];
        int[] ports = new int[npaxos];
        Paxos[] pxa = new Paxos[npaxos];
        for(int i = 0 ; i < npaxos; i++){
            ports[i] = 1200+i;
            peers[i] = host;
        }
        for(int i = 0; i < npaxos; i++){
            pxa[i] = new Paxos(i, peers, ports);
        }
        return pxa;
    }


	public static void main(String[] args) {
		Test test = new Test();
        int npaxos = 3;
		Paxos[] paxos = test.initPaxos(npaxos);

        // Custom Test for Forget
        for(int i = 0; i < npaxos; i++){
            int m = paxos[i].Min();
            System.out.println(m);
        }
        System.out.println("=============");

        paxos[0].Start(0,"00");
        paxos[1].Start(1,"11");
        paxos[2].Start(2,"22");
//        paxos[0].Start(6,"66");
//        paxos[1].Start(7,"77");

        waitn(paxos, 0, npaxos);
        waitn(paxos, 1, npaxos);

        for(int i = 0; i < npaxos; i++){
            paxos[i].Done(1);
        }

        for(int i = 1; i < npaxos; i++){
            paxos[i].Done(2);
        }

        for(int i = 0; i < npaxos; i++){
            paxos[i].Start(3+i, "xx");
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < npaxos; i++) {
            System.out.println(paxos[i].Min() + " " + paxos[i].Max());
        }





	}
}

