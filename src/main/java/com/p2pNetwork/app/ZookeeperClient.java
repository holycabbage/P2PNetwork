package com.p2pNetwork.app;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;


public class ZookeeperClient implements Watcher{
    private final ZooKeeper zookeeper;

    public ZookeeperClient(String connectionString, int sessionTimeout) throws IOException {
        this.zookeeper = new ZooKeeper(connectionString, sessionTimeout, this);
    }

    public String findTargetNode(String hashedKey) throws KeeperException, InterruptedException {
        List<String> children = zookeeper.getChildren("/nodes", false);
    
        if (children == null || children.isEmpty()) {
            System.out.println("No nodes found in Zookeeper.");
            return null;
        }
    
        // Sort the list based on the node ID (assuming that's how you define the ring)
        Collections.sort(children, (node1, node2) -> {
            String hash1 = HashUtil.sha1(node1); // Assuming HashUtil.sha1() hashes the node IDs
            String hash2 = HashUtil.sha1(node2);
            return hash1.compareTo(hash2);
        });
    
        // Find the appropriate node for the hashed key
        String targetNode = null;
        for (String child : children) {
            String childHash = HashUtil.sha1(child);
            if (hashedKey.compareTo(childHash) <= 0) {
                targetNode = child;
                break;
            }
        }
    
        // If no node was found, the hashed key is larger than any node ID; it wraps around to the first node.
        if (targetNode == null) {
            targetNode = children.get(0);
        }
    
        // Assuming the data stored at the child node in Zookeeper is the node's address (ip:port)
        byte[] nodeData = zookeeper.getData("/nodes/" + targetNode, false, null);
        String nodeAddress = new String(nodeData);
    
        return nodeAddress;
    }

    public ZooKeeper getZookeeper() {
        return zookeeper;
    }

    public void run() throws InterruptedException {
        synchronized (zookeeper) {
            zookeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zookeeper.close();
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (getZookeeper()) {
                        System.out.println("Disconnected from Zookeeper event");
                        getZookeeper().notifyAll();
                    }
                }
            default:
                break;
        }
    }
}
