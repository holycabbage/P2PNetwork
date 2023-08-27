package com.p2pNetwork.app;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;

public class Node{
    private final String nodeID;
    private final String ipAddress;
    private final int port;
    private final ZookeeperClient zooKeeperClient;
    private HashMap<String, String> inMemoryHashmap;

    public Node(String uniqueIdentifier, String ipAddress, int port, ZookeeperClient zooKeeperClient) throws IOException {
        this.nodeID = HashUtil.sha1(uniqueIdentifier);
        this.ipAddress = ipAddress;
        this.port = port;
        this.zooKeeperClient = zooKeeperClient;
        this.inMemoryHashmap = new HashMap<>();
        registerWithZookeeper();
        initHttpServer();
    }

    private void registerWithZookeeper() {
        try {
            ZooKeeper zk = zooKeeperClient.getZookeeper();
            String path = "/node_" + nodeID;
            if (zk.exists(path, false) == null) {
                zk.create(path, (ipAddress + ":" + port).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            //zk.create("/node_" + nodeID, (ipAddress + ":" + port).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Registered with " + nodeID);
    }

    private void initHttpServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/put", new PutHandler());
            server.createContext("/get", new GetHandler());
            server.setExecutor(null);
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    class PutHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            // Handle the PUT request
            String query = t.getRequestURI().getQuery();
            String[] params = query.split("=");
            if (params.length != 2) {
                System.out.println("Invalid query");
                return;
            }
            String key = params[0];
            String value = params[1];
            String response = "";

            String hashedKey = HashUtil.sha1(key);
            if (hashedKey.compareTo(nodeID) > 0) {
                inMemoryHashmap.put(key, value);
                System.out.println("Successfully put " + key + " " + value);
            } else {
                String targetAddress;
                try{
                    targetAddress = zooKeeperClient.findTargetNode(hashedKey);
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }

                try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                    HttpPut httpPut = new HttpPut("http://" + targetAddress + "/put?key=" + key + "&value=" + value);
                    try (CloseableHttpResponse httpResponse = httpClient.execute(httpPut)) {
                        response = EntityUtils.toString(httpResponse.getEntity());
                    }
                }
            }
            
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    class GetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            if (t.getRequestURI().getQuery() == null) {
                System.out.println("Invalid query");
                return;
            }

            String key = t.getRequestURI().getQuery().split("=")[0];
            String hashedKey = HashUtil.sha1(key);
            String response = "";

            if (hashedKey.compareTo(nodeID) > 0) {
                String value = inMemoryHashmap.get(key);
                value = value == null ? "Key not found" : value;
            } else {
                // Find the appropriate node using Zookeeper
                String targetAddress;
                try {
                    targetAddress = zooKeeperClient.findTargetNode(hashedKey);
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
                try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                    HttpGet httpGet = new HttpGet("http://" + targetAddress + "/get?key=" + key);
                    try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                        response = EntityUtils.toString(httpResponse.getEntity());
                    }
                }
            }

            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

}
