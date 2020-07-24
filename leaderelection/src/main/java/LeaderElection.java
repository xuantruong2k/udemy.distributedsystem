import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private static final String TARGET_ZNODE = "/target_znode";

    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        LeaderElection leaderElection = new LeaderElection(); // create an instance of app, this app will be a node too

        leaderElection.connectToZookeeper(); // connect this app instance to ZooKeeper server

        leaderElection.checkNodeAndCreate(ELECTION_NAMESPACE); // check and create the election node (parent) node of this app instance 's node
        leaderElection.volunteerForLeadership(); // create this node
        leaderElection.reelecLeader(); // self-elect this node to leader

        leaderElection.run();
        leaderElection.close();

        System.out.println("Disconnected from ZooKeeper, exit application");
    }

    /**
     * Check and create the node if it isn't exist
     * @param nodeName
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void checkNodeAndCreate(String nodeName) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(nodeName, false);
        if (stat == null) { // the node is not exist
            String path = zooKeeper.create(nodeName, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.CONTAINER);
        }
    }

    /**
     * Create volunteer (candidate) node for leader
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
        System.out.println("znode name: " + znodeFullPath);
        System.out.println("current znode name: " + this.currentZnodeName);
    }

    /**
     * when adding a node, elect this node (itself) as a candidate for leader
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void reelecLeader() throws KeeperException, InterruptedException {

        Stat predecessorStat = null;
        String predecessorZnodeName = "";
        while (predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader");
                return;
            } else {
                System.out.println("I am not the leader, " + smallestChild + " is the leader");
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName); // get the current node index
                predecessorIndex = predecessorIndex - 1; // get the predecessor index
                predecessorZnodeName = children.get(predecessorIndex); // get the predecessor znode name
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }
        System.out.println("Watching znode " + predecessorZnodeName);
    }

    /**
     * connect to ZooKeeper server
     * @throws IOException
     */
    private void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    private void run() throws InterruptedException {
        synchronized (zooKeeper) {
            this.zooKeeper.wait();
        }
    }

    private void close() throws InterruptedException {
        synchronized (zooKeeper) {
            this.zooKeeper.close();
        }
    }

    private void watchTargetZnode() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
        if (stat == null) { // this znodee does not exist
            return;
        }

        byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("Data: " + new String(data) + " children : " + children);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from ZooKeeper event");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
//                System.out.println(TARGET_ZNODE + " was deleted");
                // when the node which this node is watching delete
                // reelectLeader or rewatching another node
                try {
                    reelecLeader();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
                break;
            case NodeCreated:
//                System.out.println(TARGET_ZNODE + " was created");
                break;
            case NodeDataChanged:
//                System.out.println(TARGET_ZNODE + " data changed");
                break;
            case NodeChildrenChanged:
//                System.out.println(TARGET_ZNODE + " children changed");
                break;
        }

        try {
            watchTargetZnode();
        } catch (KeeperException  e) {

        } catch (InterruptedException e) {

        }
    }
}
