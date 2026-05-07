package com.synclite.db.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

public class DashboardServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();

        try {
            Path dbRoot = DBWebSupport.getDbRoot(request);
            Path configPath = DBWebSupport.getConfigPath(dbRoot);
            boolean running = Files.exists(configPath) && DBWebSupport.findRunningServerPid(configPath) > 0;

            JSONArray results = queryDashboardStats(DBWebSupport.getMetadataDbPath(dbRoot).toString());
            
            JSONObject jsonResponse = new JSONObject();
            jsonResponse.put("result", true);
            jsonResponse.put("message", running ? "Dashboard stats retrieved successfully" : "SyncLite DB job is not running. Showing latest available stats snapshot.");
            jsonResponse.put("running", running);
            jsonResponse.put("resultset", results);
            jsonResponse.put("code", "OK");
            
            out.print(jsonResponse.toString());
        } catch (Exception e) {
            response.setStatus(500);
            JSONObject errorResponse = new JSONObject();
            errorResponse.put("result", false);
            errorResponse.put("message", "Error: " + e.getMessage());
            out.print(errorResponse.toString());
        }
    }

    private JSONArray queryDashboardStats(String statsFilePath) throws SQLException {
        JSONArray results = new JSONArray();
        String url = "jdbc:sqlite:" + statsFilePath;
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            String[] sqls = new String[] {
                "SELECT uptime_ms, request_count, request_rate, open_connections, open_resultsets, database_count, last_heartbeat_time FROM statistics ORDER BY last_heartbeat_time DESC LIMIT 1",
                "SELECT uptime_ms, request_count, request_rate, open_connections, open_resultsets, database_count, last_heartbeat_time FROM dashboard ORDER BY last_heartbeat_time DESC LIMIT 1"
            };

            for (String sql : sqls) {
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    if (rs.next()) {
                        JSONObject row = new JSONObject();
                        row.put("uptime_ms", rs.getLong("uptime_ms"));
                        row.put("request_count", rs.getLong("request_count"));
                        row.put("request_rate", rs.getDouble("request_rate"));
                        row.put("open_connections", rs.getLong("open_connections"));
                        row.put("open_resultsets", rs.getLong("open_resultsets"));
                        row.put("database_count", rs.getLong("database_count"));
                        row.put("last_heartbeat_time", rs.getLong("last_heartbeat_time"));
                        results.put(row);
                    }
                    if (results.length() > 0) {
                        break;
                    }
                } catch (SQLException e) {
                    if (e.getMessage() != null && e.getMessage().toLowerCase().contains("no such table")) {
                        continue;
                    }
                    throw e;
                }
            }
        } catch (SQLException e) {
            if (e.getMessage() != null && (e.getMessage().contains("no such table")
                    || e.getMessage().toLowerCase().contains("no suitable driver")
                    || e.getMessage().toLowerCase().contains("unable to open database file"))) {
                return new JSONArray();
            }
            throw e;
        }
        
        return results;
    }
}
