package me.yingrui.data.api;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.util.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataAPIServer {
    private static Logger LOG = LoggerFactory.getLogger(DataAPIServer.class);
    private String url = "jdbc:h2:file:/tmp/dora-test-db;MODE=MYSQL";
    private int port = 8080;
    private Driver.Serialization serialization = Driver.Serialization.JSON;

    private HttpServer server;

    public static void main(String[] args) {
        DataAPIServer server = new DataAPIServer();
        server.start();

        // Try to clean up when the server is stopped.
        Runtime.getRuntime().addShutdownHook(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        LOG.info("Stopping server");
                        server.stop();
                        LOG.info("DataAPIServer stopped");
                    }
                }));

        try {
            server.join();
        } catch (InterruptedException e) {
            // Reset interruption
            Thread.currentThread().interrupt();
            // And exit now.
            return;
        }
    }

    private void start() {
        try {
            JdbcMeta meta = new JdbcMeta(url, "sa", "");
            LocalService service = new LocalService(meta);

            this.server = new HttpServer.Builder()
                    .withHandler(service, serialization)
                    .withPort(port)
                    .build();
            server.start();
            LOG.info("Started Avatica server on port {} with serialization {}", server.getPort(), serialization);
        } catch (Exception e) {
            LOG.error("Failed to start Avatica server", e);
            Unsafe.systemExit(DataAPIServer.ExitCodes.START_FAILED.ordinal());
        }
    }

    public void stop() {
        if (null != server) {
            server.stop();
            server = null;
        }
    }

    public void join() throws InterruptedException {
        server.join();
    }

    private enum ExitCodes {
        NORMAL,
        ALREADY_STARTED, // 1
        START_FAILED,    // 2
        USAGE;           // 3
    }

}
