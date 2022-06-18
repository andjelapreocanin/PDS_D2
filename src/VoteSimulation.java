
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class VoteSimulation implements Watcher {
    ZooKeeper zk;
    String hostname = "localhost:2181";
    String serverId = Integer.toHexString(new Random().nextInt());
    List<String> options = Arrays.asList("0", "1");
    List<String> numChildren = new ArrayList<>();
    List<String> yes = new ArrayList<>();
    List<String> no = new ArrayList<>();
    List<String> votes = new ArrayList<>();
    String result;
    boolean flagLeader = false;

    private void start() throws IOException {
        zk = new ZooKeeper(hostname, 500, this);
    }

    private void stop() throws InterruptedException {
        zk.close();
    }

    private void createLeader() throws InterruptedException, KeeperException {
        try {
            zk.create("/leader", "start".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            flagLeader = true;
            System.out.println(serverId + " je uspesno postao leader!");
            waitForVotes();
        } catch (KeeperException e) {
            flagLeader = false;
            followersVote();
            System.out.println(serverId + " je uspesno postao folover, leader vec postoji!");
        }

    }

    private void waitForVotes() throws InterruptedException {
        try {
            numChildren = zk.getChildren("/votes", this, null);
            if (numChildren.size() == 3) {
                System.out.println("Prikupljanje glasova, molimo vas pricekajte");
                collectVotes();
            } else {
                System.out.println("Glasanje jos uvek u toku");
            }
        } catch (KeeperException e) {
            System.out.println(serverId + ": potrebno je kreirati cvor /votes!");
            waitForVotes();
        }
    }

    private void followersVote() throws InterruptedException {
        try {
            Random r = new Random();
            String vote = options.get(r.nextInt(options.size()));
            zk.create("/votes/vote-", vote.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    private void collectVotes() throws InterruptedException, KeeperException {

            numChildren = zk.getChildren("/votes", this, null);
            for (int i = 0; i < 3; i++) {
                byte[] vote = zk.getData("/votes/" + numChildren.get(i), true, null);
                votes.add(new String(vote));
                if (new String(vote).equals("1")) {
                    yes.add(new String(vote));
                    result = "DA";
                } else if (new String(vote).equals("0")) {
                    no.add(new String(vote));
                    result = "NE";
                }
            }
            //System.out.println("Glasovi: "+ votes);
            byte[] data = zk.getData("/leader", this, null);
            if (yes.size() > no.size()) {
                zk.setData("/leader", "DA".getBytes(), 0);
            } else if (yes.size() < no.size()) {
                zk.setData("/leader", "NE".getBytes(), 0);
            }
    }

    @Override
    public void process(WatchedEvent notification) {
        if (notification.getType() == Event.EventType.NodeChildrenChanged && flagLeader == true) {
            try {
                waitForVotes();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (notification.getType() == Event.EventType.NodeDataChanged) {
            try {
                byte[] byteD = zk.getData("/leader", this, null);
                System.out.println("Finalni rezultat: " + new String(byteD));
                score();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

   private void score() throws KeeperException, InterruptedException {
        for (int i = 0; i < numChildren.size(); i++) {
            if (new String(zk.getData("/leader", this, null)).equals("DA") && new String(zk.getData("/votes/" + numChildren.get(i), this, null)).equals("1")) {
                System.out.println(this.serverId + " : Moj glas je pobedio!");
            } else if (new String(zk.getData("/leader", this, null)).equals("NE") && new String(zk.getData("/votes/" + numChildren.get(i), this, null)).equals("0")) {
                System.out.println(this.serverId + " : Moj glas je izgubio...");
            }else {
                System.out.println(this.serverId + " : Moj glas je izgubio...");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.apache.zookeeper.ClientCnxn").setLevel(Level.OFF);
        Logger.getLogger("org.apache.zookeeper.ZooKeeper").setLevel(Level.OFF);

        List<VoteSimulation> votes = new ArrayList<>();

        for(int i = 0; i < 4; i++){
            votes.add(new VoteSimulation());
        }

        for(VoteSimulation vs: votes){
            vs.start();
        }

        for(VoteSimulation vs: votes){
            vs.createLeader();
        }

        Thread.sleep(12000000);

        for(VoteSimulation vs: votes){
            vs.stop();
        }
    }
}
