package com.p2pNetwork.app;

public class App {
    public static void main(String[] args) {
        try {
            ZookeeperClient zooKeeperClient = new ZookeeperClient("localhost:2181", 3000);
            Node s1 = new Node("s1", "localhost", 8080, zooKeeperClient);
            Node s2 = new Node("s2", "localhost", 8081, zooKeeperClient);
            Node s3 = new Node("s3", "localhost", 8082, zooKeeperClient);
            Node s4 = new Node("s4", "localhost", 8083, zooKeeperClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
