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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/resetJob")
public class ResetJob extends HttpServlet {
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			Path dbRoot = DBWebSupport.getDbRoot(request);
			Path configPath = DBWebSupport.getConfigPath(dbRoot);

			// Ensure no server is running
			long currentJobPID = DBWebSupport.findRunningServerPid(configPath);
			if (currentJobPID != 0) {
				String errorMessage = "A SyncLite DB server is already running with Process ID : " + currentJobPID + ". Please stop the server and then run Reset Job.";
				response.sendRedirect("resetJob.jsp?errorMsg=" + URLEncoder.encode(errorMessage, StandardCharsets.UTF_8.name()));
				return;
			}

			String keepJobConfiguration = request.getParameter("keep-job-configuration");
			if (keepJobConfiguration == null) {
				keepJobConfiguration = "true";
			}

			String keepDbFilesParam = request.getParameter("keep-db-files");
			final String keepDbFiles = (keepDbFilesParam != null) ? keepDbFilesParam : "true";

			if (!Files.exists(dbRoot) || !Files.isDirectory(dbRoot)) {
				throw new ServletException("Database Root Directory does not exist: " + dbRoot);
			}

			// Build exclusion set with normalized paths
			Set<Path> excludePaths = new HashSet<>();
			excludePaths.add(dbRoot.resolve("synclite_db.trace").toAbsolutePath().normalize());

			if (keepJobConfiguration.equals("true")) {
				excludePaths.add(dbRoot.resolve("synclite_db.conf").toAbsolutePath().normalize());
				excludePaths.add(dbRoot.resolve("synclite_logger.conf").toAbsolutePath().normalize());
			}

			// If keeping DB files, query metadata DB for all database paths
			Path metadataPath = dbRoot.resolve(DBWebSupport.METADATA_DB_FILE_NAME);
			if (keepDbFiles.equals("true") && Files.exists(metadataPath)) {
				try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataPath)) {
					try (Statement stmt = conn.createStatement()) {
						stmt.execute("PRAGMA busy_timeout = 5000");
						try (ResultSet rs = stmt.executeQuery("SELECT database_path FROM databases")) {
							while (rs.next()) {
								String dbPathStr = rs.getString("database_path");
								if (dbPathStr != null && !dbPathStr.isEmpty()) {
									Path dbFilePath = Path.of(dbPathStr).toAbsolutePath().normalize();
									excludePaths.add(dbFilePath);
								}
							}
						}
					}
				} catch (Exception e) {
					// If metadata DB cannot be read, fall back to keeping it as-is
					excludePaths.add(metadataPath.toAbsolutePath().normalize());
				}
			}

			// Delete files in dbRoot, skipping excluded paths
			Files.walkFileTree(dbRoot, new SimpleFileVisitor<>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					if (excludePaths.contains(file.toAbsolutePath().normalize())) {
						return FileVisitResult.CONTINUE;
					}

					if (Files.exists(file)) {
						Files.delete(file);
					}
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					if (dir.equals(dbRoot)) {
						return FileVisitResult.CONTINUE;
					}
					// Don't delete non-empty directories (e.g. if db files were kept)
					try (var entries = Files.list(dir)) {
						if (entries.findAny().isEmpty()) {
							Files.delete(dir);
						}
					}
					return FileVisitResult.CONTINUE;
				}
			});

			// Invalidate session job state
			request.getSession().removeAttribute("job-status");

			response.sendRedirect("resetJob.jsp?successMsg=" + URLEncoder.encode("SyncLite DB job has been reset successfully.", StandardCharsets.UTF_8.name()));
		} catch (ServletException e) {
			response.setStatus(400);
			response.sendRedirect("resetJob.jsp?errorMsg=" + URLEncoder.encode(e.getMessage(), StandardCharsets.UTF_8.name()));
		} catch (Exception e) {
			response.setStatus(500);
			response.sendRedirect("resetJob.jsp?errorMsg=" + URLEncoder.encode("Error: " + e.getMessage(), StandardCharsets.UTF_8.name()));
		}
	}
}
