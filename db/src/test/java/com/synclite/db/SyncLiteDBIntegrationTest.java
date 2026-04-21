package com.synclite.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import sampleapp.SyncLiteDBClient;
import sampleapp.SyncLiteDBClient.SyncLiteDBResult;

public class SyncLiteDBIntegrationTest {

    private static final String APP_ID = "junit-app";
    private static final String APP_SECRET = "junit-secret-for-signing";
    private static final String AUTH_TOKEN = "junit-global-token";

    private Thread serverThread;
    private int serverPort;
    private Path testRoot;
    private Path deviceDir;
    private Path stageDir;
    private Path loggerConfigPath;

    @Before
    public void setUp() throws Exception {
        Path userHome = Path.of(System.getProperty("user.home"));
        testRoot = userHome.resolve("synclite").resolve("test");
        deviceDir = testRoot.resolve("testsynclitedb");
        stageDir = testRoot.resolve("stageDir");

        // Clean requested directories before test run.
        deleteRecursively(deviceDir.toFile());
        deleteRecursively(stageDir.toFile());
        Files.createDirectories(deviceDir);
        Files.createDirectories(stageDir);

        loggerConfigPath = writeLoggerConfig(deviceDir, stageDir);

        serverPort = reserveFreePort();
        Path configPath = writeServerConfig(deviceDir, serverPort);

        serverThread = new Thread(() -> Main.main(new String[] {"--config", configPath.toString()}));
        serverThread.setName("synclite-db-it-server");
        serverThread.start();

        waitForServerReady(Duration.ofSeconds(15));
        SyncLiteDBClient.setSyncLiteDBAddress("http://127.0.0.1:" + serverPort);
    }

    @After
    public void tearDown() throws Exception {
        if (serverThread != null && serverThread.isAlive()) {
            serverThread.interrupt();
            serverThread.join(5000);
        }
    }

    @Test
    public void testAllAuthEnabledComprehensiveSQLiteOperations() throws Exception {
        Path dbPath = deviceDir.resolve("auth-it.db");

        JSONObject unauthorizedBody = new JSONObject()
            .put("db-path", dbPath.toString())
            .put("db-type", "SQLITE")
            .put("db-name", "authIT")
            .put("synclite-logger-config", loggerConfigPath.toString())
            .put("sql", "initialize");

        SyncLiteDBClient.clearAuthConfiguration();
        JSONObject unauthorized = SyncLiteDBClient.processRequest(unauthorizedBody);
        assertFalse(unauthorized.getBoolean("result"));
        assertEquals("ERR_UNAUTHORIZED", unauthorized.getString("code"));

        SyncLiteDBClient.setAuthConfiguration(AUTH_TOKEN, APP_ID, APP_SECRET);

        SyncLiteDBResult initializeResult = SyncLiteDBClient.initializeDB(dbPath, "SQLITE", "authIT", loggerConfigPath);
        assertTrue(initializeResult.result);

        SyncLiteDBResult beginResult = SyncLiteDBClient.beginTransaction(dbPath);
        assertTrue(beginResult.result);
        String txnHandle = beginResult.txnHandle;
        assertNotNull(txnHandle);

        SyncLiteDBResult createTableResult = SyncLiteDBClient.executeSQL(
            dbPath,
            txnHandle,
            "create table if not exists t1(a int, b text)",
            null);
        assertTrue(createTableResult.result);

        JSONArray args = new JSONArray()
                .put(new JSONArray().put(1).put("one"))
                .put(new JSONArray().put(2).put("two"));

        SyncLiteDBResult insertResult = SyncLiteDBClient.executeSQL(dbPath, txnHandle, "insert into t1 (a,b) values(?, ?)", args);
        assertTrue(insertResult.result);

        SyncLiteDBResult updateResult = SyncLiteDBClient.executeSQL(dbPath, txnHandle, "update t1 set b='ONE' where a=1", null);
        assertTrue(updateResult.result);

        SyncLiteDBResult deleteResult = SyncLiteDBClient.executeSQL(dbPath, txnHandle, "delete from t1 where a=2", null);
        assertTrue(deleteResult.result);

        SyncLiteDBResult alterAddResult = SyncLiteDBClient.executeSQL(dbPath, txnHandle, "alter table t1 add column c text", null);
        assertTrue(alterAddResult.result);

        SyncLiteDBResult updateNewColumnResult = SyncLiteDBClient.executeSQL(dbPath, txnHandle, "update t1 set c='extra' where a=1", null);
        assertTrue(updateNewColumnResult.result);

        SyncLiteDBResult alterDropResult = SyncLiteDBClient.executeSQL(dbPath, txnHandle, "alter table t1 drop column c", null);
        assertTrue(alterDropResult.result);

        SyncLiteDBResult commitResult = SyncLiteDBClient.commitTransaction(dbPath, txnHandle);
        assertTrue(commitResult.result);

        SyncLiteDBResult selectResult = SyncLiteDBClient.executeSQL(dbPath, null, "select a, b from t1 order by a", null);
        assertTrue(selectResult.result);
        JSONArray resultSet = selectResult.resultSet;
        assertEquals(1, resultSet.length());
        assertEquals(1, resultSet.getJSONObject(0).getInt("a"));
        assertEquals("ONE", resultSet.getJSONObject(0).getString("b"));

        SyncLiteDBResult dropTableResult = SyncLiteDBClient.executeSQL(dbPath, null, "drop table t1", null);
        assertTrue(dropTableResult.result);

        SyncLiteDBResult closeResult = SyncLiteDBClient.closeDB(dbPath);
        assertTrue(closeResult.result);
    }

    @Test
    public void testSelectPaginationWithResultsetHandleAndNext() throws Exception {
        Path dbPath = deviceDir.resolve("pagination-it.db");
        SyncLiteDBClient.setAuthConfiguration(AUTH_TOKEN, APP_ID, APP_SECRET);

        SyncLiteDBResult initializeResult = SyncLiteDBClient.initializeDB(dbPath, "SQLITE", "paginationIT", loggerConfigPath);
        assertTrue(initializeResult.result);

        SyncLiteDBResult createTableResult = SyncLiteDBClient.executeSQL(dbPath, null, "create table if not exists t2(id int, name text)", null);
        assertTrue(createTableResult.result);

        JSONArray args = new JSONArray()
                .put(new JSONArray().put(1).put("one"))
                .put(new JSONArray().put(2).put("two"))
                .put(new JSONArray().put(3).put("three"))
                .put(new JSONArray().put(4).put("four"))
                .put(new JSONArray().put(5).put("five"));
        SyncLiteDBResult insertResult = SyncLiteDBClient.executeSQL(dbPath, null, "insert into t2 (id, name) values (?, ?)", args);
        assertTrue(insertResult.result);

        JSONObject firstPageRequest = new JSONObject()
                .put("db-path", dbPath.toString())
                .put("db-type", "SQLITE")
                .put("db-name", "paginationIT")
                .put("synclite-logger-config", loggerConfigPath.toString())
                .put("sql", "select id, name from t2 order by id")
                .put("resultset-pagination-size", 2);

        JSONObject firstPage = SyncLiteDBClient.processRequest(firstPageRequest);
        assertTrue(firstPage.getBoolean("result"));
        assertTrue(firstPage.getBoolean("has-more"));
        assertTrue(firstPage.has("resultset-handle"));
        assertEquals(2, firstPage.getJSONArray("resultset").length());
        String resultsetHandle = firstPage.getString("resultset-handle");

        JSONObject nextRequest = new JSONObject()
                .put("request-type", "next")
                .put("resultset-handle", resultsetHandle)
                .put("resultset-pagination-size", 2);
        JSONObject secondPage = SyncLiteDBClient.processRequest(nextRequest);
        assertTrue(secondPage.toString(), secondPage.getBoolean("result"));
        assertTrue(secondPage.toString(), secondPage.getBoolean("has-more"));
        assertTrue(secondPage.has("resultset-handle"));
        assertEquals(2, secondPage.getJSONArray("resultset").length());

        JSONObject lastPageRequest = new JSONObject()
                .put("request-type", "next")
                .put("resultset-handle", resultsetHandle)
                .put("resultset-pagination-size", 2);
        JSONObject lastPage = SyncLiteDBClient.processRequest(lastPageRequest);
        assertTrue(lastPage.getBoolean("result"));
        assertFalse(lastPage.getBoolean("has-more"));
        assertEquals(1, lastPage.getJSONArray("resultset").length());

        JSONObject invalidNext = SyncLiteDBClient.processRequest(lastPageRequest);
        assertFalse(invalidNext.getBoolean("result"));
        assertEquals("ERR_INVALID_RESULTSET_HANDLE", invalidNext.getString("code"));

        SyncLiteDBResult closeResult = SyncLiteDBClient.closeDB(dbPath);
        assertTrue(closeResult.result);
    }

    private static int reserveFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static Path writeServerConfig(Path root, int port) throws IOException {
        Files.createDirectories(root);
        Path conf = root.resolve("synclite_db_test.conf");
        String newline = System.lineSeparator();
        StringBuilder b = new StringBuilder();
        b.append("port=").append(port).append(newline);
        b.append("num-threads=4").append(newline);
        b.append("idle-connection-timeout-ms=30000").append(newline);
        b.append("bind-address=127.0.0.1").append(newline);
        b.append("max-request-size-bytes=1048576").append(newline);
        b.append("auth-token=").append(AUTH_TOKEN).append(newline);
        b.append("enable-app-auth=true").append(newline);
        b.append("app-auth-timestamp-skew-ms=300000").append(newline);
        b.append("app-auth-nonce-ttl-ms=600000").append(newline);
        b.append("app-auth-nonce-cache-max-entries=10000").append(newline);
        b.append("authorized-apps=").append(APP_ID).append(newline);
        b.append("app.").append(APP_ID).append(".secret=").append(APP_SECRET).append(newline);
        b.append("app.").append(APP_ID).append(".allowed-ops=initialize,begin,commit,rollback,select,next,execute,close").append(newline);
        b.append("trace-directory=").append(root.toString()).append(newline);
        b.append("trace-level=INFO").append(newline);
        Files.writeString(conf, b.toString(), StandardCharsets.UTF_8);
        return conf;
    }

    private static Path writeLoggerConfig(Path root, Path stageDirectory) throws IOException {
        Files.createDirectories(root);
        Files.createDirectories(stageDirectory);
        Path conf = root.resolve("synclite_logger_test.conf");
        String newline = System.lineSeparator();
        StringBuilder b = new StringBuilder();
        b.append("local-data-stage-directory=").append(stageDirectory.toString()).append(newline);
        b.append("destination-type=FS").append(newline);
        Files.writeString(conf, b.toString(), StandardCharsets.UTF_8);
        return conf;
    }


    private void waitForServerReady(Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        Exception lastError = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                URL url = new URL("http://127.0.0.1:" + serverPort + "/");
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(1000);
                conn.setReadTimeout(1000);
                int code = conn.getResponseCode();
                if (code == 200) {
                    return;
                }
            } catch (Exception e) {
                lastError = e;
            }
            Thread.sleep(200);
        }

        throw new IllegalStateException("Server did not start within timeout", lastError);
    }

    private static void deleteRecursively(File file) {
        if (file == null || !file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }

        file.delete();
    }

}
