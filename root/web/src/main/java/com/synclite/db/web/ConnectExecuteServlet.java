package com.synclite.db.web;

import java.io.BufferedReader;
import java.net.ConnectException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.net.SocketTimeoutException;
import java.util.LinkedHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

public class ConnectExecuteServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        JSONObject jsonResponse = new JSONObject();
        String endpoint = null;

        try {
            String requestJson = readBody(request);
            if (requestJson.isBlank()) {
                throw new ServletException("Request JSON cannot be empty.");
            }

            // Validate request payload is valid JSON before forwarding.
            new JSONObject(requestJson);

            Path dbRoot = DBWebSupport.getDbRoot(request);
            Path configPath = DBWebSupport.getConfigPath(dbRoot);
            if (!Files.exists(configPath)) {
                throw new ServletException("Configuration not found under selected Database Root Directory.");
            }

            LinkedHashMap<String, String> config = DBWebSupport.readConfig(dbRoot);
            String bindAddress = config.getOrDefault("bind-address", "127.0.0.1").trim();
            if (bindAddress.isEmpty() || "0.0.0.0".equals(bindAddress)) {
                bindAddress = "127.0.0.1";
            }
            String port = config.getOrDefault("port", "5555").trim();
            if (port.isEmpty()) {
                port = "5555";
            }

            endpoint = "http://" + bindAddress + ":" + port + "/synclite";
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URL(endpoint).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            conn.setDoOutput(true);
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(60000);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(requestJson.getBytes(StandardCharsets.UTF_8));
            }

            int statusCode = conn.getResponseCode();
            String responseBody = readResponseBody(conn, statusCode >= 400);

            jsonResponse.put("result", true);
            jsonResponse.put("code", statusCode);
            jsonResponse.put("endpoint", endpoint);
            jsonResponse.put("request-json", requestJson);
            jsonResponse.put("response-json", formatJsonIfPossible(responseBody));
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            jsonResponse.put("result", false);
            jsonResponse.put("message", buildUserFacingErrorMessage(e, endpoint));
        }

        response.getWriter().print(jsonResponse.toString());
    }

    private static String readBody(HttpServletRequest request) throws IOException {
        StringBuilder body = new StringBuilder();
        try (BufferedReader reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                body.append(line).append('\n');
            }
        }
        return body.toString().trim();
    }

    private static String readResponseBody(java.net.HttpURLConnection conn, boolean useErrorStream) throws IOException {
        InputStream stream = useErrorStream ? conn.getErrorStream() : conn.getInputStream();
        if (stream == null) {
            return "";
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            StringBuilder body = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                body.append(line).append('\n');
            }
            return body.toString().trim();
        }
    }

    private static String formatJsonIfPossible(String text) {
        if (text == null || text.isBlank()) {
            return "";
        }
        try {
            return new JSONObject(text).toString(2);
        } catch (Exception e) {
            return text;
        }
    }

    private static String buildUserFacingErrorMessage(Exception e, String endpoint) {
        String safeEndpoint = endpoint == null || endpoint.isBlank() ? "configured SyncLite DB endpoint" : endpoint;
        if (e instanceof ConnectException) {
            return "Could not connect to SyncLite DB at " + safeEndpoint + ". The server appears to be stopped or not listening on the configured bind address/port. Please start or load the SyncLite DB job and try again.";
        }
        if (e instanceof SocketTimeoutException) {
            return "Timed out while waiting for SyncLite DB at " + safeEndpoint + ". Please verify the server is running and reachable, then try again.";
        }
        String message = e.getMessage();
        if (message == null || message.isBlank()) {
            return "Failed to execute request against SyncLite DB at " + safeEndpoint + ".";
        }
        return message;
    }
}
