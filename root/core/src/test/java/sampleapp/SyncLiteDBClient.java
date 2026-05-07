package sampleapp;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Base64;
import java.util.UUID;
import java.security.MessageDigest;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.json.JSONArray;
import org.json.JSONObject;

public class SyncLiteDBClient {

    public static class SyncLiteDBResult {
        public boolean result;
        public String message;
        public JSONArray resultSet;
        public String txnHandle;
        public String resultsetHandle;
        public boolean hasMore;
        public JSONArray columnMetadata;
    }

    private static String syncLiteDBAddress = "http://localhost:5555";
    private static String configuredAuthToken;
    private static String configuredAppId;
    private static String configuredAppSecret;

    public static void setSyncLiteDBAddress(String address) {
        syncLiteDBAddress = address;
    }

    public static void setAuthConfiguration(String token, String appId, String appSecret) {
        configuredAuthToken = token;
        configuredAppId = appId;
        configuredAppSecret = appSecret;
    }

    public static void clearAuthConfiguration() {
        configuredAuthToken = null;
        configuredAppId = null;
        configuredAppSecret = null;
    }

    private static String sha256Hex(String value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
        StringBuilder builder = new StringBuilder();
        for (byte b : hash) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }

    private static String sign(String secret, String payload) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return Base64.getEncoder().encodeToString(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
    }

    public static JSONObject processRequest(JSONObject jsonRequest) throws SQLException {
        JSONObject jsonResponse;
        try {
            URL url = new URL(syncLiteDBAddress);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(10000);

            String token = configuredAuthToken != null ? configuredAuthToken : System.getenv("SYNCLITE_DB_AUTH_TOKEN");
            if (token != null && !token.isBlank()) {
                conn.setRequestProperty("X-SyncLite-Token", token);
            }

            String appId = configuredAppId != null ? configuredAppId : System.getenv("SYNCLITE_DB_APP_ID");
            String appSecret = configuredAppSecret != null ? configuredAppSecret : System.getenv("SYNCLITE_DB_APP_SECRET");

            String payload = jsonRequest.toString();
            if (appId != null && !appId.isBlank() && appSecret != null && !appSecret.isBlank()) {
                String timestamp = String.valueOf(System.currentTimeMillis());
                String nonce = UUID.randomUUID().toString();
                String canonical = "POST\n/\n" + timestamp + "\n" + nonce + "\n" + sha256Hex(payload);
                String signature = sign(appSecret, canonical);

                conn.setRequestProperty("X-SyncLite-App-Id", appId);
                conn.setRequestProperty("X-SyncLite-Timestamp", timestamp);
                conn.setRequestProperty("X-SyncLite-Nonce", nonce);
                conn.setRequestProperty("X-SyncLite-Signature", signature);
            }

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = payload.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK
                    || responseCode == HttpURLConnection.HTTP_BAD_REQUEST
                    || responseCode == HttpURLConnection.HTTP_UNAUTHORIZED
                    || responseCode == HttpURLConnection.HTTP_FORBIDDEN
                    || responseCode == 413) {
                BufferedReader in;
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                } else {
                    in = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8));
                }

                String inputLine;
                StringBuilder response = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                jsonResponse = new JSONObject(response.toString());
            } else {
                throw new SQLException("Failed to get a valid response from the server : " + responseCode);
            }
        } catch (Exception e) {
            throw new SQLException("Failed to process request : " + e.getMessage(), e);
        }
        return jsonResponse;
    }

    public static SyncLiteDBResult initializeDB(String dbType, String dbName, JSONObject syncLiteLoggerOptions)
            throws SQLException {
        SyncLiteDBResult dbResult;
        try {
            JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("db-type", dbType);
            jsonRequest.put("db-name", dbName);
            if (syncLiteLoggerOptions != null) {
                jsonRequest.put("synclite-logger-options", syncLiteLoggerOptions);
            }
            jsonRequest.put("sql", "initialize");

            JSONObject jsonResponse = processRequest(jsonRequest);

            dbResult = new SyncLiteDBResult();
            dbResult.result = jsonResponse.getBoolean("result");
            dbResult.message = jsonResponse.getString("message");
        } catch (Exception e) {
            throw new SQLException("Failed to initialize DB : " + dbName + " : " + e.getMessage(), e);
        }
        return dbResult;
    }

    public static SyncLiteDBResult beginTransaction(String dbName) throws SQLException {
        SyncLiteDBResult dbResult;
        try {
            JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("db-name", dbName);
            jsonRequest.put("sql", "begin");

            JSONObject jsonResponse = processRequest(jsonRequest);

            dbResult = new SyncLiteDBResult();
            dbResult.result = jsonResponse.getBoolean("result");
            dbResult.message = jsonResponse.getString("message");
            dbResult.txnHandle = jsonResponse.getString("txn-handle");
        } catch (Exception e) {
            throw new SQLException("Failed to begin transaction on DB : " + dbName + " : " + e.getMessage(), e);
        }
        return dbResult;
    }

    public static SyncLiteDBResult commitTransaction(String dbName, String txnHandle) throws SQLException {
        SyncLiteDBResult dbResult;
        try {
            JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("db-name", dbName);
            jsonRequest.put("txn-handle", txnHandle);
            jsonRequest.put("sql", "commit");

            JSONObject jsonResponse = processRequest(jsonRequest);

            dbResult = new SyncLiteDBResult();
            dbResult.result = jsonResponse.getBoolean("result");
            dbResult.message = jsonResponse.getString("message");
        } catch (Exception e) {
            throw new SQLException("Failed to commit transaction on DB : " + dbName + " : " + e.getMessage(), e);
        }
        return dbResult;
    }

    public static SyncLiteDBResult rollbackTransaction(String dbName, String txnHandle) throws SQLException {
        SyncLiteDBResult dbResult;
        try {
            JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("db-name", dbName);
            jsonRequest.put("txn-handle", txnHandle);
            jsonRequest.put("sql", "rollback");

            JSONObject jsonResponse = processRequest(jsonRequest);

            dbResult = new SyncLiteDBResult();
            dbResult.result = jsonResponse.getBoolean("result");
            dbResult.message = jsonResponse.getString("message");
        } catch (Exception e) {
            throw new SQLException("Failed to rollback transaction on DB : " + dbName + " : " + e.getMessage(), e);
        }
        return dbResult;
    }

    public static SyncLiteDBResult executeSQL(String dbName, String txnHandle, String sql, JSONArray arguments)
            throws SQLException {
        return executeSQL(dbName, txnHandle, sql, arguments, null, null);
    }

    public static SyncLiteDBResult executeSQL(String dbName, String txnHandle, String sql, JSONArray arguments,
            String dataFormat, Boolean includeMetadata) throws SQLException {
        SyncLiteDBResult dbResult;
        try {
            JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("db-name", dbName);
            jsonRequest.put("sql", sql);
            if (txnHandle != null) {
                jsonRequest.put("txn-handle", txnHandle);
            }
            if (arguments != null) {
                jsonRequest.put("arguments", arguments);
            }
            if (dataFormat != null) {
                jsonRequest.put("resultset-data-format", dataFormat);
            }
            if (includeMetadata != null) {
                jsonRequest.put("resultset-include-metadata", includeMetadata ? "ON" : "OFF");
            }

            JSONObject jsonResponse = processRequest(jsonRequest);

            dbResult = toDBResult(jsonResponse);
        } catch (Exception e) {
            throw new SQLException("Failed to execute sql on DB : " + dbName + " : " + e.getMessage(), e);
        }
        return dbResult;
    }

    public static SyncLiteDBResult next(String resultsetHandle, int resultsetPaginationSize,
            String dataFormat, Boolean includeMetadata) throws SQLException {
        SyncLiteDBResult dbResult;
        try {
            JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("request-type", "next");
            jsonRequest.put("resultset-handle", resultsetHandle);
            if (resultsetPaginationSize > 0) {
                jsonRequest.put("resultset-pagination-size", resultsetPaginationSize);
            }
            if (dataFormat != null) {
                jsonRequest.put("resultset-data-format", dataFormat);
            }
            if (includeMetadata != null) {
                jsonRequest.put("resultset-include-metadata", includeMetadata ? "ON" : "OFF");
            }

            JSONObject jsonResponse = processRequest(jsonRequest);
            dbResult = toDBResult(jsonResponse);
        } catch (Exception e) {
            throw new SQLException("Failed to fetch next page for resultset-handle : " + resultsetHandle + " : " + e.getMessage(), e);
        }
        return dbResult;
    }

    private static SyncLiteDBResult toDBResult(JSONObject jsonResponse) {
        SyncLiteDBResult dbResult = new SyncLiteDBResult();
        dbResult.result = jsonResponse.getBoolean("result");
        dbResult.message = jsonResponse.getString("message");
        if (jsonResponse.has("resultset")) {
            dbResult.resultSet = jsonResponse.getJSONArray("resultset");
        }
        if (jsonResponse.has("txn-handle")) {
            dbResult.txnHandle = jsonResponse.getString("txn-handle");
        }
        if (jsonResponse.has("resultset-handle")) {
            dbResult.resultsetHandle = jsonResponse.getString("resultset-handle");
        }
        if (jsonResponse.has("has-more")) {
            dbResult.hasMore = jsonResponse.getBoolean("has-more");
        }
        if (jsonResponse.has("resultset-metadata")) {
            dbResult.columnMetadata = jsonResponse.getJSONArray("resultset-metadata");
        }
        return dbResult;
    }

    public static SyncLiteDBResult closeDB(String dbName) throws SQLException {
        SyncLiteDBResult dbResult;
        try {
            JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("db-name", dbName);
            jsonRequest.put("sql", "close");

            JSONObject jsonResponse = processRequest(jsonRequest);

            dbResult = new SyncLiteDBResult();
            dbResult.result = jsonResponse.getBoolean("result");
            dbResult.message = jsonResponse.getString("message");
        } catch (Exception e) {
            throw new SQLException("Failed to close DB : " + dbName + " : " + e.getMessage(), e);
        }
        return dbResult;
    }
}
