package com.tmax.hf.batch;

import com.sun.net.httpserver.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;

public class HttpServerManager {

    static final Logger logger = LogManager.getLogger(HttpServerManager.class);
    private final String HOSTNAME = "0.0.0.0";
    private final int PORT = 1200;
    private final int BACKLOG = 0;
    private HttpServer server = null;


    // Constructor
    public HttpServerManager() throws IOException {
        createServer(HOSTNAME, PORT);
    }

    public HttpServerManager(int port) throws IOException {
        createServer(HOSTNAME, port);
    }
    public HttpServerManager(String host, int port) throws IOException {
        createServer(host, port);
    }

    //CreateServer
    private void createServer(String host, int port) throws IOException {
        //Create HTTP SERVER
        this.server = HttpServer.create(new InetSocketAddress(host, port),BACKLOG);
        // HTTP SERVER Context Setting
        server.createContext("/api/v1/kafka", new KafkaHandler());
    }
    // start Server
    public void start(){
//        server.setExecutor(null);
        server.start();
    }

    //Server Stop
    public void stop(int delay){
        server.stop(delay);
    }

    public static void main(String[] args) {
        HttpServerManager httpServerManager = null;


        try{
            logger.info("[HTTP SERVER] [START]");
            //create Server
            httpServerManager = new HttpServerManager("0.0.0.0",1200);

            httpServerManager.start();



        } catch (IOException e) {
            logger.error("Exception",e);
            logger.error("Exception",e);
        } finally {
//            httpServerManager.stop(0);
        }
    }
}
