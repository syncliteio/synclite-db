/*
 * Copyright (c) 2024 mahendra.chavan@syncLite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.db.web;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

/**
 * Servlet implementation class StopServer
 */
@WebServlet("/stopServer")
public class StopServer extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public StopServer() {
		super();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			Path configPath = DBWebSupport.getConfigPath(DBWebSupport.getDbRoot(request));
			long currentJobPID = DBWebSupport.findRunningServerPid(configPath);
			if (currentJobPID > 0) {
				if (DBWebSupport.isWindows()) {
					Runtime.getRuntime().exec(new String[] {"taskkill", "/F", "/PID", String.valueOf(currentJobPID)});
				} else {
					Runtime.getRuntime().exec(new String[] {"kill", "-9", String.valueOf(currentJobPID)});
				}
			}
			request.getSession().setAttribute("job-status", "STOPPED");
			response.sendRedirect("dashboard.jsp");
		} catch (Exception e) {
			response.sendRedirect("dashboard.jsp?errorMsg=" + URLEncoder.encode(e.getMessage(), StandardCharsets.UTF_8.name()));
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			Path configPath = DBWebSupport.getConfigPath(DBWebSupport.getDbRoot(request));
			long currentJobPID = DBWebSupport.findRunningServerPid(configPath);
			if (currentJobPID > 0) {
				if (DBWebSupport.isWindows()) {
					Runtime.getRuntime().exec(new String[] {"taskkill", "/F", "/PID", String.valueOf(currentJobPID)});
				} else {
					Runtime.getRuntime().exec(new String[] {"kill", "-9", String.valueOf(currentJobPID)});
				}
			}
			request.getSession().setAttribute("job-status", "STOPPED");

			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");
			response.getWriter().print(new JSONObject()
				.put("result", true)
				.put("message", currentJobPID > 0 ? "Server stopped successfully" : "Server is not running")
				.toString());
		} catch (Exception e) {
			response.setStatus(500);
			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");
			response.getWriter().print(new JSONObject().put("result", false).put("message", "Error: " + e.getMessage()).toString());
		}
	}
}
