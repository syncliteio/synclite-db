package com.synclite.db.web;

import java.io.IOException;
import java.io.PrintWriter;
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

public class DatabaseStatisticsServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();

        try {
            Path dbRoot = DBWebSupport.getDbRoot(request);
            JSONArray results = queryDatabaseStatistics(DBWebSupport.getMetadataDbPath(dbRoot).toString());

            JSONObject jsonResponse = new JSONObject();
            jsonResponse.put("result", true);
            jsonResponse.put("message", "Database statistics retrieved successfully");
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

    private JSONArray queryDatabaseStatistics(String statsFilePath) throws SQLException {
        JSONArray results = new JSONArray();
        String url = "jdbc:sqlite:" + statsFilePath;

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            String[] sqls = new String[] {
                "SELECT database_name, database_type, database_path, database_size FROM databases ORDER BY database_name",
                "SELECT database_name, database_type, database_path, database_size FROM database_statistics ORDER BY database_name"
            };

            for (String sql : sqls) {
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        JSONObject row = new JSONObject();
                        row.put("name", rs.getString("database_name"));
                        row.put("type", rs.getString("database_type"));
                        row.put("path", rs.getString("database_path"));
                        row.put("size", rs.getLong("database_size"));
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
            if (e.getMessage() != null && e.getMessage().toLowerCase().contains("no such table")) {
                return new JSONArray();
            }
            throw e;
        }

        return results;
    }
}
