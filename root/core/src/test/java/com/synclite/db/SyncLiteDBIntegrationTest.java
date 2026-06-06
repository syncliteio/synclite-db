package com.synclite.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
    private JSONObject loggerOptions;

    @Before
    public void setUp() throws Exception {
        Path userHome = Path.of(System.getProperty("user.home"));
        testRoot = userHome.resolve("synclite").resolve("tests");
        deviceDir = testRoot.resolve("db").resolve("synclitedb").resolve("testsynclitedb");
        stageDir = testRoot.resolve("stageDir");

        // Clean requested directories before test run.
        deleteRecursively(deviceDir.toFile());
        deleteRecursively(stageDir.toFile());
        Files.createDirectories(deviceDir);
        Files.createDirectories(stageDir);

        loggerOptions = new JSONObject()
            .put("local-data-stage-directory", stageDir.toString())
            .put("device-stage-type", "FS");

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
            .put("db-type", "SQLITE")
            .put("db-name", "authIT")
            .put("synclite-logger-options", loggerOptions)
            .put("sql", "initialize");

        SyncLiteDBClient.clearAuthConfiguration();
        JSONObject unauthorized = SyncLiteDBClient.processRequest(unauthorizedBody);
        assertFalse(unauthorized.getBoolean("result"));
        assertEquals("ERR_UNAUTHORIZED", unauthorized.getString("code"));

        SyncLiteDBClient.setAuthConfiguration(AUTH_TOKEN, APP_ID, APP_SECRET);

        SyncLiteDBResult initializeResult = SyncLiteDBClient.initializeDB("SQLITE", "authIT", loggerOptions);
        assertTrue(initializeResult.result);

        SyncLiteDBResult beginResult = SyncLiteDBClient.beginTransaction("authIT");
        assertTrue(beginResult.result);
        String txnHandle = beginResult.txnHandle;
        assertNotNull(txnHandle);

        SyncLiteDBResult createTableResult = SyncLiteDBClient.executeSQL(
            "authIT",
            txnHandle,
            "create table if not exists t1(a int, b text)",
            null);
        assertTrue(createTableResult.result);

        JSONArray args = new JSONArray()
                .put(new JSONArray().put(1).put("one"))
                .put(new JSONArray().put(2).put("two"));

        SyncLiteDBResult insertResult = SyncLiteDBClient.executeSQL("authIT", txnHandle, "insert into t1 (a,b) values(?, ?)", args);
        assertTrue(insertResult.result);

        SyncLiteDBResult updateResult = SyncLiteDBClient.executeSQL("authIT", txnHandle, "update t1 set b='ONE' where a=1", null);
        assertTrue(updateResult.result);

        SyncLiteDBResult deleteResult = SyncLiteDBClient.executeSQL("authIT", txnHandle, "delete from t1 where a=2", null);
        assertTrue(deleteResult.result);

        SyncLiteDBResult alterAddResult = SyncLiteDBClient.executeSQL("authIT", txnHandle, "alter table t1 add column c text", null);
        assertTrue(alterAddResult.result);

        SyncLiteDBResult updateNewColumnResult = SyncLiteDBClient.executeSQL("authIT", txnHandle, "update t1 set c='extra' where a=1", null);
        assertTrue(updateNewColumnResult.result);

        SyncLiteDBResult alterDropResult = SyncLiteDBClient.executeSQL("authIT", txnHandle, "alter table t1 drop column c", null);
        assertTrue(alterDropResult.result);

        SyncLiteDBResult commitResult = SyncLiteDBClient.commitTransaction("authIT", txnHandle);
        assertTrue(commitResult.result);

        SyncLiteDBResult selectResult = SyncLiteDBClient.executeSQL("authIT", null, "select a, b from t1 order by a", null);
        assertTrue(selectResult.result);
        JSONArray resultSet = selectResult.resultSet;
        assertEquals(1, resultSet.length());
        assertEquals(1, resultSet.getJSONObject(0).getInt("a"));
        assertEquals("ONE", resultSet.getJSONObject(0).getString("b"));

        SyncLiteDBResult dropTableResult = SyncLiteDBClient.executeSQL("authIT", null, "drop table t1", null);
        assertTrue(dropTableResult.result);

        SyncLiteDBResult closeResult = SyncLiteDBClient.closeDB("authIT");
        assertTrue(closeResult.result);
    }

    @Test
    public void testSelectPaginationWithResultsetHandleAndNext() throws Exception {
        Path dbPath = deviceDir.resolve("pagination-it.db");
        SyncLiteDBClient.setAuthConfiguration(AUTH_TOKEN, APP_ID, APP_SECRET);

        SyncLiteDBResult initializeResult = SyncLiteDBClient.initializeDB("SQLITE", "paginationIT", loggerOptions);
        assertTrue(initializeResult.result);

        SyncLiteDBResult createTableResult = SyncLiteDBClient.executeSQL("paginationIT", null, "create table if not exists t2(id int, name text)", null);
        assertTrue(createTableResult.result);

        JSONArray args = new JSONArray()
                .put(new JSONArray().put(1).put("one"))
                .put(new JSONArray().put(2).put("two"))
                .put(new JSONArray().put(3).put("three"))
                .put(new JSONArray().put(4).put("four"))
                .put(new JSONArray().put(5).put("five"));
        SyncLiteDBResult insertResult = SyncLiteDBClient.executeSQL("paginationIT", null, "insert into t2 (id, name) values (?, ?)", args);
        assertTrue(insertResult.result);

        JSONObject firstPageRequest = new JSONObject()
                .put("db-type", "SQLITE")
                .put("db-name", "paginationIT")
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

        SyncLiteDBResult closeResult = SyncLiteDBClient.closeDB("paginationIT");
        assertTrue(closeResult.result);
    }

    @Test
    public void testResultsetDataFormatAndMetadata() throws Exception {
        Path dbPath = deviceDir.resolve("format-it.db");
        SyncLiteDBClient.setAuthConfiguration(AUTH_TOKEN, APP_ID, APP_SECRET);

        SyncLiteDBResult initializeResult = SyncLiteDBClient.initializeDB("SQLITE", "formatIT", loggerOptions);
        assertTrue(initializeResult.result);

        SyncLiteDBResult createTableResult = SyncLiteDBClient.executeSQL("formatIT", null,
                "create table if not exists t3(id int, name text)", null);
        assertTrue(createTableResult.result);

        JSONArray args = new JSONArray()
                .put(new JSONArray().put(1).put("one"))
                .put(new JSONArray().put(2).put("two"))
                .put(new JSONArray().put(3).put("three"));
        SyncLiteDBResult insertResult = SyncLiteDBClient.executeSQL("formatIT", null,
                "insert into t3 (id, name) values (?, ?)", args);
        assertTrue(insertResult.result);

        // ---- JSON format (default) with metadata ON (default) ----
        SyncLiteDBResult jsonFmtResult = SyncLiteDBClient.executeSQL("formatIT", null,
                "select id, name from t3 order by id", null, "JSON", true);
        assertTrue(jsonFmtResult.result);

        // metadata must be present and have 2 columns
        assertNotNull("resultset-metadata must be present for JSON format with includeMetadata=ON",
                jsonFmtResult.columnMetadata);
        assertEquals(2, jsonFmtResult.columnMetadata.length());

        // verify metadata fields
        JSONObject idMeta = jsonFmtResult.columnMetadata.getJSONObject(0);
        assertTrue(idMeta.has("name"));
        assertTrue(idMeta.has("label"));
        assertTrue(idMeta.has("type"));
        assertTrue(idMeta.has("type-name"));

        // rows must be JSON objects (col→val map)
        assertNotNull(jsonFmtResult.resultSet);
        assertEquals(3, jsonFmtResult.resultSet.length());
        assertEquals(1, jsonFmtResult.resultSet.getJSONObject(0).getInt("id"));
        assertEquals("one", jsonFmtResult.resultSet.getJSONObject(0).getString("name"));
        assertEquals(2, jsonFmtResult.resultSet.getJSONObject(1).getInt("id"));
        assertEquals(3, jsonFmtResult.resultSet.getJSONObject(2).getInt("id"));

        // ---- JSON format with metadata OFF ----
        SyncLiteDBResult noMetaResult = SyncLiteDBClient.executeSQL("formatIT", null,
                "select id, name from t3 order by id", null, "JSON", false);
        assertTrue(noMetaResult.result);
        assertFalse("resultset-metadata must be absent when includeMetadata=OFF",
                noMetaResult.columnMetadata != null);
        assertNotNull(noMetaResult.resultSet);
        assertEquals(3, noMetaResult.resultSet.length());
        // rows still as JSON objects
        assertEquals("one", noMetaResult.resultSet.getJSONObject(0).getString("name"));

        // ---- DB format with metadata ON ----
        SyncLiteDBResult dbFmtResult = SyncLiteDBClient.executeSQL("formatIT", null,
                "select id, name from t3 order by id", null, "DB", true);
        assertTrue(dbFmtResult.result);

        // metadata must be present
        assertNotNull("resultset-metadata must be present for DB format with includeMetadata=ON",
                dbFmtResult.columnMetadata);
        assertEquals(2, dbFmtResult.columnMetadata.length());
        assertEquals("id", dbFmtResult.columnMetadata.getJSONObject(0).getString("label"));
        assertEquals("name", dbFmtResult.columnMetadata.getJSONObject(1).getString("label"));

        // rows must be arrays (positional values)
        assertNotNull(dbFmtResult.resultSet);
        assertEquals(3, dbFmtResult.resultSet.length());
        JSONArray firstRow = dbFmtResult.resultSet.getJSONArray(0);
        assertEquals(2, firstRow.length());
        assertEquals(1, firstRow.getInt(0));
        assertEquals("one", firstRow.getString(1));

        JSONArray secondRow = dbFmtResult.resultSet.getJSONArray(1);
        assertEquals(2, secondRow.getInt(0));

        // ---- DB format + pagination with next ----
        SyncLiteDBResult page1 = SyncLiteDBClient.executeSQL("formatIT", null,
                "select id, name from t3 order by id", null, "DB", true);
        // override pagination size via raw request to get 1 row per page
        JSONObject page1Req = new JSONObject()
                .put("db-name", "formatIT")
                .put("sql", "select id, name from t3 order by id")
                .put("resultset-data-format", "DB")
                .put("resultset-include-metadata", "ON")
                .put("resultset-pagination-size", 1);
        JSONObject page1Resp = SyncLiteDBClient.processRequest(page1Req);
        assertTrue(page1Resp.getBoolean("result"));
        assertTrue(page1Resp.getBoolean("has-more"));
        assertEquals(1, page1Resp.getJSONArray("resultset").length());
        // resultset-metadata present on first page
        assertTrue("resultset-metadata must be present on first page of DB format",
                page1Resp.has("resultset-metadata"));
        assertEquals(2, page1Resp.getJSONArray("resultset-metadata").length());
        // first row is array
        assertTrue(page1Resp.getJSONArray("resultset").get(0) instanceof JSONArray);
        assertEquals(1, page1Resp.getJSONArray("resultset").getJSONArray(0).getInt(0));

        String handle = page1Resp.getString("resultset-handle");

        // next page — DB format, metadata OFF
        SyncLiteDBResult page2 = SyncLiteDBClient.next(handle, 1, "DB", false);
        assertTrue(page2.result);
        assertTrue(page2.hasMore);
        assertNull("resultset-metadata must be absent when includeMetadata=OFF on next",
                page2.columnMetadata);
        assertNotNull(page2.resultSet);
        assertEquals(1, page2.resultSet.length());
        JSONArray page2Row = page2.resultSet.getJSONArray(0);
        assertEquals(2, page2Row.getInt(0));
        assertEquals("two", page2Row.getString(1));

        // last page
        SyncLiteDBResult page3 = SyncLiteDBClient.next(page2.resultsetHandle, 1, "DB", false);
        assertTrue(page3.result);
        assertFalse(page3.hasMore);
        assertEquals(1, page3.resultSet.length());
        JSONArray page3Row = page3.resultSet.getJSONArray(0);
        assertEquals(3, page3Row.getInt(0));
        assertEquals("three", page3Row.getString(1));

        SyncLiteDBResult closeResult = SyncLiteDBClient.closeDB("formatIT");
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
        b.append("trace-level=INFO").append(newline);
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
