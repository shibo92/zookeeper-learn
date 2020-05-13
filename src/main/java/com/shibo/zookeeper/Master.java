package com.shibo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * @author by shibo on 2020/5/6.
 */
public class Master implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(Master.class);
    private Random random = new Random(this.hashCode());
    String serverId = Integer.toHexString(random.nextInt());
    boolean isLeader = false;

    ZooKeeper zk;
    String hostPort;

    public Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() {
        try {
            zk = new ZooKeeper(hostPort, 15000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Closes the ZooKeeper session
     *
     * @throws InterruptedException
     */
    void stopZK() throws InterruptedException {
        zk.close();
    }

    /**
     * 获取管理权（同步）
     * 通过捕获异常来判断节点状态
     * @throws KeeperException
     * @throws InterruptedException
     */
    void runForMasterAsync() throws KeeperException, InterruptedException {
        while (true) {
            try {
                zk.create("/maser", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException.NodeExistsException e) {
                isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException e) {
            }
            if (checkMaster()) {
                break;
            }
        }
    }
    /**
     * 异步获取管理权限回调方法
     */
    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I'm " + (isLeader ? "" : "not ") + "the leader.");
        }
    };
    /**
     * 获取管理权（异步）
     * 通过回调函数获取节点状态
     */
    void runForMaster() {
        zk.create("/maser", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    boolean checkMaster() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException e) {
                // no master, so try create again
                return false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }

        }
    }

    public void process(WatchedEvent event) {
        System.out.println("===>" + event);
    }



    public static void main(String[] args) throws Exception {
        Master m = new Master("127.0.0.1:2181");
        m.startZk();
        m.runForMaster();
        if (m.isLeader) {
            System.out.println("I'm the leader!");
            Thread.sleep(60000);
        } else {
            System.out.println("我不是leader~");
        }
        m.stopZK();
    }
}
