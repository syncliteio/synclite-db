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

public class DevicesServlet extends HttpServlet {

    private static final int DEFAULT_PAGE_SIZE = 10;
    private static final int MAX_PAGE_SIZE = 100;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();

        try {
            Path dbRoot = DBWebSupport.getDbRoot(request);
            Path statsPath = DBWebSupport.getMetadataDbPath(dbRoot);
            if (!Files.exists(statsPath)) {
                JSONObject infoResponse = new JSONObject();
                infoResponse.put("result", true);
                infoResponse.put("message", "No statistics file available yet.");
                infoResponse.put("resultset", new JSONArray());
                infoResponse.put("totalRows", 0);
                infoResponse.put("page", 1);
                infoResponse.put("pageSize", DEFAULT_PAGE_SIZE);
                infoResponse.put("totalPages", 0);
                infoResponse.put("sortBy", "database_name");
                infoResponse.put("sortDir", "ASC");
                infoResponse.put("code", "OK");
                out.print(infoResponse.toString());
                return;
            }

            int page = parsePositiveInt(request.getParameter("page"), 1);
            int pageSize = Math.min(parsePositiveInt(request.getParameter("pageSize"), DEFAULT_PAGE_SIZE), MAX_PAGE_SIZE);
            String sortBy = whitelistSortBy(request.getParameter("sortBy"));
            String sortDir = "DESC".equalsIgnoreCase(request.getParameter("sortDir")) ? "DESC" : "ASC";

            JSONObject results = queryDevices(statsPath.toString(), page, pageSize, sortBy, sortDir);

            JSONObject jsonResponse = new JSONObject();
            jsonResponse.put("result", true);
            jsonResponse.put("message", "Devices retrieved successfully");
            jsonResponse.put("resultset", results.getJSONArray("rows"));
            jsonResponse.put("totalRows", results.getLong("totalRows"));
            jsonResponse.put("page", page);
            jsonResponse.put("pageSize", pageSize);
            jsonResponse.put("totalPages", results.getLong("totalPages"));
            jsonResponse.put("sortBy", sortBy);
            jsonResponse.put("sortDir", sortDir);
            jsonResponse.put("code", "OK");

            out.print(jsonResponse.toString());
        } catch (Exception e) {
            response.setStatus(500);
            JSONObject errorResponse = new JSONObject();
            errorResponse.put("result", false);
            errorResponse.put("message", "Error retrieving device statistics.");
            out.print(errorResponse.toString());
        }
    }

    private JSONObject queryDevices(String statsFilePath, int page, int pageSize, String sortBy, String sortDir) throws SQLException {
        JSONObject result = new JSONObject();
        JSONArray rows = new JSONArray();
        String url = "jdbc:sqlite:" + statsFilePath;

        String[] tableNames = new String[] {"databases", "database_statistics"};

        try (Connection conn = DriverManager.getConnection(url)) {
            for (String tableName : tableNames) {
                String countSql = "SELECT COUNT(*) FROM " + tableName;
                String dataSql = "SELECT database_name, database_type, database_path, request_count, request_rate, open_connections, open_resultsets, uptime_ms, last_heartbeat_time "
                    + "FROM " + tableName + " ORDER BY " + sortBy + " " + sortDir + " LIMIT ? OFFSET ?";
                try (PreparedStatement countStmt = conn.prepareStatement(countSql);
                     PreparedStatement dataStmt = conn.prepareStatement(dataSql)) {
                    long totalRows = 0;
                    try (ResultSet rs = countStmt.executeQuery()) {
                        if (rs.next()) {
                            totalRows = rs.getLong(1);
                        }
                    }

                    int offset = Math.max(0, (page - 1) * pageSize);
                    dataStmt.setInt(1, pageSize);
                    dataStmt.setInt(2, offset);

                    try (ResultSet rs = dataStmt.executeQuery()) {
                        while (rs.next()) {
                            JSONObject row = new JSONObject();
                            row.put("database_name", rs.getString("database_name"));
                            row.put("database_type", rs.getString("database_type"));
                            row.put("database_path", rs.getString("database_path"));
                            row.put("request_count", rs.getLong("request_count"));
                            row.put("request_rate", rs.getDouble("request_rate"));
                            row.put("open_connections", rs.getLong("open_connections"));
                            row.put("open_resultsets", rs.getLong("open_resultsets"));
                            row.put("uptime_ms", rs.getLong("uptime_ms"));
                            row.put("last_heartbeat_time", rs.getLong("last_heartbeat_time"));
                            rows.put(row);
                        }
                    }

                    long totalPages = totalRows == 0 ? 0 : (long) Math.ceil(totalRows / (double) pageSize);
                    result.put("rows", rows);
                    result.put("totalRows", totalRows);
                    result.put("totalPages", totalPages);
                    return result;
                } catch (SQLException e) {
                    if (isNoStatisticsAvailable(e) && e.getMessage() != null && e.getMessage().toLowerCase().contains("no such table")) {
                        continue;
                    }
                    throw e;
                }
            }
            result.put("rows", new JSONArray());
            result.put("totalRows", 0);
            result.put("totalPages", 0);
            return result;
        } catch (SQLException e) {
            if (isNoStatisticsAvailable(e)) {
                result.put("rows", new JSONArray());
                result.put("totalRows", 0);
                result.put("totalPages", 0);
                return result;
            }
            throw e;
        }
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

    private static int parsePositiveInt(String value, int defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value);
            return parsed > 0 ? parsed : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static String whitelistSortBy(String sortBy) {
        if (sortBy == null || sortBy.isBlank()) {
            return "database_name";
        }
        String normalized = sortBy.trim().toLowerCase();
        switch (normalized) {
            case "database_name":
            case "database_type":
            case "database_path":
            case "request_count":
            case "request_rate":
            case "open_connections":
            case "open_resultsets":
            case "uptime_ms":
            case "last_heartbeat_time":
                return normalized;
            default:
                return "database_name";
        }
    }
}
