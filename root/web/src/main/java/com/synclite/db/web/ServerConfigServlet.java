package com.synclite.db.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

public class ServerConfigServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();

        try {
            Path dbRoot = DBWebSupport.getDbRoot(request);
            LinkedHashMap<String, String> configValues = DBWebSupport.readConfig(dbRoot);
            long processId = DBWebSupport.findRunningServerPid(DBWebSupport.getConfigPath(dbRoot));
            JSONObject config = new JSONObject();
            for (String key : configValues.keySet()) {
                config.put(key, configValues.get(key));
            }
            config.put("version", DBWebSupport.getVersion());
            config.put("running", processId > 0);
            config.put("process-id", processId > 0 ? processId : JSONObject.NULL);
            
            JSONObject jsonResponse = new JSONObject();
            jsonResponse.put("result", true);
            jsonResponse.put("message", "Server configuration retrieved successfully");
            jsonResponse.put("resultset", new JSONArray().put(config));
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
}
