package com.synclite.db.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

public class DeviceStatsServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();

        String databaseName = request.getParameter("database_name");
        if (databaseName == null || databaseName.isBlank()) {
            response.setStatus(400);
            JSONObject error = new JSONObject();
            error.put("result", false);
            error.put("message", "database_name query parameter is required");
            out.print(error.toString());
            return;
        }

        try {
            Path dbRoot = DBWebSupport.getDbRoot(request);
            Path statsPath = DBWebSupport.getMetadataDbPath(dbRoot);
            if (!Files.exists(statsPath)) {
                JSONObject info = new JSONObject();
                info.put("result", true);
                info.put("message", "No statistics file available yet.");
                info.put("resultset", new JSONArray());
                info.put("code", "OK");
                out.print(info.toString());
                return;
            }

            JSONArray results = queryDeviceStats(statsPath.toString(), databaseName.trim());
            JSONObject jsonResponse = new JSONObject();
            jsonResponse.put("result", true);
            jsonResponse.put("message", "Device statistics retrieved successfully");
            jsonResponse.put("resultset", results);
            jsonResponse.put("code", "OK");
            out.print(jsonResponse.toString());
        } catch (Exception e) {
            response.setStatus(500);
            JSONObject error = new JSONObject();
            error.put("result", false);
            error.put("message", "Error retrieving device statistics.");
            out.print(error.toString());
        }
    }

    private JSONArray queryDeviceStats(String statsFilePath, String databaseName) throws SQLException {
        JSONArray results = new JSONArray();
        String url = "jdbc:sqlite:" + statsFilePath;
        String[] tableNames = new String[] {"databases", "database_statistics"};

        try (Connection conn = DriverManager.getConnection(url)) {
            for (String tableName : tableNames) {
                String sql = "SELECT database_name, database_type, database_path, uptime_ms, request_count, request_rate, open_connections, open_resultsets, logger_options_json, "
                    + "last_heartbeat_time, last_job_start_time FROM " + tableName + " WHERE database_name = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, databaseName);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            JSONObject row = new JSONObject();
                            row.put("database_name", rs.getString("database_name"));
                            row.put("database_type", rs.getString("database_type"));
                            row.put("database_path", rs.getString("database_path"));
                            row.put("uptime_ms", rs.getLong("uptime_ms"));
                            row.put("request_count", rs.getLong("request_count"));
                            row.put("request_rate", rs.getDouble("request_rate"));
                            row.put("open_connections", rs.getLong("open_connections"));
                            row.put("open_resultsets", rs.getLong("open_resultsets"));
                            row.put("logger_options_json", rs.getString("logger_options_json"));
                            row.put("last_heartbeat_time", rs.getLong("last_heartbeat_time"));
                            row.put("last_job_start_time", rs.getLong("last_job_start_time"));
                            results.put(row);
                        }
                    }
                    if (results.length() > 0) {
                        break;
                    }
                } catch (SQLException e) {
                    if (isNoStatisticsAvailable(e) && e.getMessage() != null && e.getMessage().toLowerCase().contains("no such table")) {
                        continue;
                    }
                    throw e;
                }
            }
        } catch (SQLException e) {
            if (isNoStatisticsAvailable(e)) {
                return new JSONArray();
            }
            throw e;
        }

        return results;
    }

    private static boolean isNoStatisticsAvailable(SQLException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        String normalized = message.toLowerCase();
        return normalized.contains("no such table")
                || normalized.contains("no suitable driver")
                || normalized.contains("unable to open database file");
    }
}
