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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.LinkedHashMap;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

/**
 * Servlet implementation class StartServer
 */
@WebServlet("/startServer")
public class StartServer extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public StartServer() {
		super();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			Path dbRoot = DBWebSupport.getDbRoot(request);
			Path configPath = DBWebSupport.getConfigPath(dbRoot);
			if (!Files.exists(configPath)) {
				throw new ServletException("Configuration file does not exist under selected Database Root Directory. Please save configuration first.");
			}

			long currentJobPID = DBWebSupport.findRunningServerPid(configPath);
			if (currentJobPID == 0) {
				Path libDir = Path.of(getServletContext().getRealPath("/"), "WEB-INF", "lib");
				LinkedHashMap<String, String> config = DBWebSupport.readConfig(dbRoot);
				DBWebSupport.writeJvmArgsFiles(libDir, config.get("jvm-arguments"));

				if (DBWebSupport.isWindows()) {
					Runtime.getRuntime().exec(new String[] {libDir.resolve("synclite-db.bat").toString(), "--config", configPath.toString()});
				} else {
					Path scriptPath = libDir.resolve("synclite-db.sh");
					Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
					if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
						perms.add(PosixFilePermission.OWNER_EXECUTE);
						Files.setPosixFilePermissions(scriptPath, perms);
					}
					Runtime.getRuntime().exec(new String[] {scriptPath.toString(), "--config", configPath.toString()});
				}
				request.getSession().setAttribute("job-status", "STARTED");
				request.getSession().setAttribute("job-start-time", System.currentTimeMillis());
			}
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
			Path dbRoot = DBWebSupport.getDbRoot(request);
			Path configPath = DBWebSupport.getConfigPath(dbRoot);
			if (!Files.exists(configPath)) {
				throw new ServletException("Configuration file does not exist under selected Database Root Directory. Please save configuration first.");
			}

			long currentJobPID = DBWebSupport.findRunningServerPid(configPath);
			if (currentJobPID == 0) {
				Path libDir = Path.of(getServletContext().getRealPath("/"), "WEB-INF", "lib");
				LinkedHashMap<String, String> config = DBWebSupport.readConfig(dbRoot);
				DBWebSupport.writeJvmArgsFiles(libDir, config.get("jvm-arguments"));

				if (DBWebSupport.isWindows()) {
					Runtime.getRuntime().exec(new String[] {libDir.resolve("synclite-db.bat").toString(), "--config", configPath.toString()});
				} else {
					Path scriptPath = libDir.resolve("synclite-db.sh");
					Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
					if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
						perms.add(PosixFilePermission.OWNER_EXECUTE);
						Files.setPosixFilePermissions(scriptPath, perms);
					}
					Runtime.getRuntime().exec(new String[] {scriptPath.toString(), "--config", configPath.toString()});
				}
				request.getSession().setAttribute("job-status", "STARTED");
				request.getSession().setAttribute("job-start-time", System.currentTimeMillis());
			}

			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");
			response.getWriter().print(new JSONObject()
				.put("result", true)
				.put("message", currentJobPID == 0 ? "Server start command issued successfully" : "Server is already running")
				.toString());
		} catch (ServletException e) {
			response.setStatus(400);
			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");
			response.getWriter().print(new JSONObject().put("result", false).put("message", e.getMessage()).toString());
		} catch (Exception e) {
			response.setStatus(500);
			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");
			response.getWriter().print(new JSONObject().put("result", false).put("message", "Error: " + e.getMessage()).toString());
		}
	}
}
