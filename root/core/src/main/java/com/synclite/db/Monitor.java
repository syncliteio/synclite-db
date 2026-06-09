/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
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

package com.synclite.db;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import io.synclite.DeviceType;


public class Monitor {

	private static Monitor INSTANCE;
	public static final Monitor getInstance() {
		return INSTANCE;
	}
	
	public static synchronized Monitor createAndGetInstance(Logger tracer) {		
		if (INSTANCE != null) {
			return INSTANCE;
		} else {
			INSTANCE = new Monitor(tracer);
		}
		return INSTANCE;
	}

	private abstract class Dumper extends Thread {
		public void run() {
			while (!Thread.interrupted()) {
				try {            	
					dump();
					Thread.sleep(screenRefreshIntervalMs);
				} catch (InterruptedException e) {
					Thread.interrupted();
				}
			}
		}

		protected abstract void dump();

		protected abstract void init() throws Exception;

		protected abstract void schedule();
		protected abstract void shutdown();
		
		protected ScheduledExecutorService monitorService;
	}

	private class FileDumper extends Dumper{
		Connection statsConn;
		PreparedStatement updateDashboardPstmt;
		PreparedStatement upsertDatabasesPstmt;

		private String createDashboardTableSql = "CREATE TABLE if not exists statistics(header TEXT, uptime_ms LONG, request_count LONG, request_rate REAL, open_connections LONG, open_resultsets LONG, database_count LONG, last_heartbeat_time LONG, last_job_start_time LONG)";
		private String insertDashboardTableSql = "INSERT INTO statistics (header, uptime_ms, request_count, request_rate, open_connections, open_resultsets, database_count, last_heartbeat_time, last_job_start_time) VALUES('$1', 0, 0, 0.0, 0, 0, 0, 0, 0)";
		private String updateDashboardTableSql = "UPDATE statistics SET uptime_ms = ?, request_count = ?, request_rate = ?, open_connections = ?, open_resultsets = ?, database_count = ?, last_heartbeat_time = ?, last_job_start_time = ?";
		private String selectDashboardTableSql = "SELECT uptime_ms, request_count, request_rate FROM statistics;";

		private String createDatabasesTableSql = "CREATE TABLE IF NOT EXISTS databases(database_name TEXT PRIMARY KEY, database_type TEXT, database_path TEXT, database_size LONG, logger_options_json TEXT, uptime_ms LONG, request_count LONG, request_rate REAL, open_connections LONG, open_resultsets LONG, last_heartbeat_time LONG, last_job_start_time LONG, last_updated LONG)";
		private String upsertDatabasesSql = "INSERT OR REPLACE INTO databases(database_name, database_type, database_path, database_size, logger_options_json, uptime_ms, request_count, request_rate, open_connections, open_resultsets, last_heartbeat_time, last_job_start_time, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		private FileDumper() {
		}

		private final void initStats() throws Exception {
			Path metadataPath = Main.getDbDir().resolve(Main.METADATA_DB_FILE_NAME);
			boolean metadataFileExists = Files.exists(metadataPath);
			Path legacyMetadataPath = Main.getDbDir().resolve(Main.LEGACY_STATISTICS_DB_FILE_NAME);
			String url = "jdbc:sqlite:" + metadataPath.toString();
			try {
				statsConn = DriverManager.getConnection(url);

				try (Statement stmt = statsConn.createStatement()) {
					stmt.execute("PRAGMA busy_timeout = 5000");
					stmt.execute("PRAGMA journal_mode = WAL");
					stmt.execute("PRAGMA synchronous = NORMAL");
					stmt.execute(createDashboardTableSql);
					try (ResultSet rs = stmt.executeQuery(selectDashboardTableSql)) {
						if (!rs.next()) {
							insertDashboardTableSql = insertDashboardTableSql.replace("$1", Monitor.this.header);
							stmt.execute(insertDashboardTableSql);
						}
					}	
					updateDashboardPstmt = statsConn.prepareStatement(updateDashboardTableSql);        			
					
					stmt.execute(createDatabasesTableSql);
					upsertDatabasesPstmt = statsConn.prepareStatement(upsertDatabasesSql);

					migrateLegacyTables(stmt);
					if (!metadataFileExists && Files.exists(legacyMetadataPath)) {
						migrateLegacyFile(stmt, legacyMetadataPath);
					}

					// Reload existing databases from metadata into in-memory registry
					reloadExistingDatabases(stmt);

					// Backward compatibility for existing deployments that still have database_statistics.
				}        		

			} catch (SQLException e) {
				tracer.error("Failed to create/open SyncLite DB metadata file at : " + url + " : " + e.getMessage() , e);
				throw new Exception("Failed to create/open SyncLite DB metadata file at : " + url, e);
			}

		}

		private void reloadExistingDatabases(Statement stmt) {
			try (ResultSet rs = stmt.executeQuery("SELECT database_name, database_type, database_path FROM databases")) {
				int reloaded = 0;
				while (rs.next()) {
					String dbName = rs.getString("database_name");
					String dbTypeStr = rs.getString("database_type");
					String dbPathStr = rs.getString("database_path");

					if (dbName == null || dbName.isBlank() || dbPathStr == null || dbPathStr.isBlank()) {
						continue;
					}

					// Skip if already registered in memory
					if (DB.getDatabase(dbName) != null) {
						continue;
					}

					try {
						DeviceType dbType = DeviceType.valueOf(dbTypeStr);
						Path dbPath = Path.of(dbPathStr);

						// Resolve logger config: per-DB config file or default
						Path loggerConfig = Main.getDbDir().resolve(dbName + ".synclite.conf");
						if (!Files.exists(loggerConfig)) {
							loggerConfig = Main.resolveSyncliteConf(Main.getDbDir());
						}

						DB db = new DB(dbName, dbType, dbPath, loggerConfig);
						db.init();
						DB.addDatabase(db);
						reloaded++;
					} catch (Exception e) {
						tracer.warn("Failed to reload database '" + dbName + "' from metadata: " + e.getMessage());
					}
				}
				if (reloaded > 0) {
					tracer.info("Reloaded " + reloaded + " existing database(s) from metadata on startup.");
				}
			} catch (SQLException e) {
				tracer.warn("Failed to reload existing databases from metadata: " + e.getMessage());
			}
		}

		private void migrateLegacyTables(Statement stmt) {
			try {
				stmt.execute("INSERT INTO statistics(header, uptime_ms, request_count, request_rate, open_connections, open_resultsets, database_count, last_heartbeat_time, last_job_start_time) "
					+ "SELECT header, uptime_ms, request_count, request_rate, open_connections, open_resultsets, database_count, last_heartbeat_time, last_job_start_time "
					+ "FROM dashboard WHERE NOT EXISTS (SELECT 1 FROM statistics)");
			} catch (SQLException ignored) {
			}

			try {
				stmt.execute("INSERT INTO databases(database_name, database_type, database_path, database_size, logger_options_json, uptime_ms, request_count, request_rate, open_connections, open_resultsets, last_heartbeat_time, last_job_start_time, last_updated) "
					+ "SELECT database_name, database_type, database_path, database_size, NULL, uptime_ms, request_count, request_rate, open_connections, open_resultsets, last_heartbeat_time, last_job_start_time, last_updated "
					+ "FROM database_statistics WHERE NOT EXISTS (SELECT 1 FROM databases)");
			} catch (SQLException ignored) {
			}
		}

		private void migrateLegacyFile(Statement stmt, Path legacyMetadataPath) {
			String escapedLegacyPath = legacyMetadataPath.toString().replace("'", "''");
			try {
				stmt.execute("ATTACH DATABASE '" + escapedLegacyPath + "' AS legacy_metadata");
				try {
					stmt.execute("INSERT INTO statistics(header, uptime_ms, request_count, request_rate, open_connections, open_resultsets, database_count, last_heartbeat_time, last_job_start_time) "
						+ "SELECT header, uptime_ms, request_count, request_rate, open_connections, open_resultsets, database_count, last_heartbeat_time, last_job_start_time "
						+ "FROM legacy_metadata.statistics WHERE NOT EXISTS (SELECT 1 FROM statistics)");
				} catch (SQLException ignored) {
				}
				try {
					stmt.execute("INSERT INTO statistics(header, uptime_ms, request_count, request_rate, open_connections, open_resultsets, database_count, last_heartbeat_time, last_job_start_time) "
						+ "SELECT header, uptime_ms, request_count, request_rate, open_connections, open_resultsets, database_count, last_heartbeat_time, last_job_start_time "
						+ "FROM legacy_metadata.dashboard WHERE NOT EXISTS (SELECT 1 FROM statistics)");
				} catch (SQLException ignored) {
				}
				try {
					stmt.execute("INSERT INTO databases(database_name, database_type, database_path, database_size, logger_options_json, uptime_ms, request_count, request_rate, open_connections, open_resultsets, last_heartbeat_time, last_job_start_time, last_updated) "
						+ "SELECT database_name, database_type, database_path, database_size, logger_options_json, uptime_ms, request_count, request_rate, open_connections, open_resultsets, last_heartbeat_time, last_job_start_time, last_updated "
						+ "FROM legacy_metadata.databases WHERE NOT EXISTS (SELECT 1 FROM databases)");
				} catch (SQLException ignored) {
				}
				try {
					stmt.execute("INSERT INTO databases(database_name, database_type, database_path, database_size, logger_options_json, uptime_ms, request_count, request_rate, open_connections, open_resultsets, last_heartbeat_time, last_job_start_time, last_updated) "
						+ "SELECT database_name, database_type, database_path, database_size, NULL, uptime_ms, request_count, request_rate, open_connections, open_resultsets, last_heartbeat_time, last_job_start_time, last_updated "
						+ "FROM legacy_metadata.database_statistics WHERE NOT EXISTS (SELECT 1 FROM databases)");
				} catch (SQLException ignored) {
				}
			} catch (SQLException ignored) {
			} finally {
				try {
					stmt.execute("DETACH DATABASE legacy_metadata");
				} catch (SQLException ignored) {
				}
			}
		}

		@Override
		protected void dump() {
			try {
				long currentTime = System.currentTimeMillis();
				if ((lastStatChangeTime < lastStatFlushTime) && ((currentTime - lastStatFlushTime) < heartbeatIntervalMs)) {
					//Skip the update if there is nothing to be updated
					//However force an update if last update was done 30 seconds back
					//as this update also serves as a heartbeat of the consolidator job
					return;
				}
				
				// Fetch current stats from RequestProcessor and DB
				long uptimeMs = System.currentTimeMillis() - Main.getStatsStartTime();
				long requestCount = RequestProcessor.getRequestCount();
				double requestRate = RequestProcessor.getRequestRate();
				long openConnections = DB.getTotalOpenConnectionCount();
				long openResultsets = DB.getOpenResultSetCount();
				long databaseCount = DB.getDatabaseCount();
				
				statsConn.setAutoCommit(false);
				updateDashboardPstmt.setLong(1, uptimeMs);
				updateDashboardPstmt.setLong(2, requestCount);
				updateDashboardPstmt.setDouble(3, requestRate);
				updateDashboardPstmt.setLong(4, openConnections);
				updateDashboardPstmt.setLong(5, openResultsets);
				updateDashboardPstmt.setLong(6, databaseCount);
				updateDashboardPstmt.setLong(7, currentTime);
				updateDashboardPstmt.setLong(8, Main.getJobStartTime());
				updateDashboardPstmt.addBatch();
				updateDashboardPstmt.executeBatch();

				// Update per-database statistics
				List<Map<String, Object>> dbInventory = DB.getDatabaseInventory();
				Map<String, Map<String, Object>> deviceStats = RequestProcessor.getDeviceRequestStatsSnapshot();
				Map<String, String> loggerOptionsByDb = RequestProcessor.getDatabaseLoggerOptionsSnapshot();
				Map<String, String> existingLoggerOptionsByDb = loadExistingLoggerOptions();

				for (Map<String, Object> dbInfo : dbInventory) {
					String dbName = (String) dbInfo.get("name");
					long databaseSize = ((Number) dbInfo.get("size")).longValue();

					Map<String, Object> deviceStat = deviceStats.get(dbName);
					long requestCountForDb = deviceStat != null ? ((Number) deviceStat.get("request-count")).longValue() : 0L;
					double requestRateForDb = deviceStat != null ? ((Number) deviceStat.get("request-rate")).doubleValue() : 0.0;
					long uptimeForDb = deviceStat != null ? ((Number) deviceStat.get("uptime-ms")).longValue() : 0L;
					long lastHeartbeatForDb = deviceStat != null ? ((Number) deviceStat.get("last-heartbeat-time")).longValue() : currentTime;

					String loggerOptionsJson = loggerOptionsByDb.get(dbName);
					if (loggerOptionsJson == null || loggerOptionsJson.isBlank()) {
						loggerOptionsJson = existingLoggerOptionsByDb.get(dbName);
					}

					upsertDatabasesPstmt.setString(1, dbName);
					upsertDatabasesPstmt.setString(2, (String) dbInfo.get("type"));
					upsertDatabasesPstmt.setString(3, (String) dbInfo.get("path"));
					upsertDatabasesPstmt.setLong(4, databaseSize);
					upsertDatabasesPstmt.setString(5, loggerOptionsJson);
					upsertDatabasesPstmt.setLong(6, uptimeForDb);
					upsertDatabasesPstmt.setLong(7, requestCountForDb);
					upsertDatabasesPstmt.setDouble(8, requestRateForDb);
					upsertDatabasesPstmt.setLong(9, DB.getOpenConnectionCount(dbName));
					upsertDatabasesPstmt.setLong(10, DB.getOpenResultSetCount(dbName));
					upsertDatabasesPstmt.setLong(11, lastHeartbeatForDb);
					upsertDatabasesPstmt.setLong(12, Main.getJobStartTime());
					upsertDatabasesPstmt.setLong(13, currentTime);
					upsertDatabasesPstmt.execute();
				}
				
				statsConn.commit();
				statsConn.setAutoCommit(true);
				lastStatFlushTime = currentTime;

			} catch (Exception e) {
				tracer.error("SyncLite db statistics dumper failed with exception : ", e);
			}
		}

		private Map<String, String> loadExistingLoggerOptions() {
			Map<String, String> map = new LinkedHashMap<String, String>();
			if (statsConn == null) {
				return map;
			}
			String sql = "SELECT database_name, logger_options_json FROM databases";
			try (Statement stmt = statsConn.createStatement();
				 ResultSet rs = stmt.executeQuery(sql)) {
				while (rs.next()) {
					String dbName = rs.getString("database_name");
					String loggerOptionsJson = rs.getString("logger_options_json");
					if (dbName != null && !dbName.isBlank() && loggerOptionsJson != null && !loggerOptionsJson.isBlank()) {
						map.put(dbName, loggerOptionsJson);
					}
				}
			} catch (SQLException ignored) {
			}
			return map;
		}

		@Override
		protected void init() throws Exception {
			initStats();
		}

		@Override
		protected void schedule() {
	        monitorService = Executors.newScheduledThreadPool(1);
	        monitorService.scheduleAtFixedRate(this::dump, 0, screenRefreshIntervalMs, TimeUnit.MILLISECONDS);			
		}

		@Override
		protected void shutdown() {
			try {
				if (this.monitorService != null) {
					monitorService.shutdownNow();
				}
			} catch (Exception e) {
				//Ignore
			}
		}
	}

	private String header;
	private static long screenRefreshIntervalMs = 1000;
	private static final long heartbeatIntervalMs = 30000;  
	private volatile long lastStatChangeTime = System.currentTimeMillis();
	private long lastStatFlushTime = System.currentTimeMillis();
	private Dumper dumper;
	private List<Dumper> additionalDumpers = new ArrayList<Dumper>();
	private Logger tracer;
	private boolean monitorEnabled;

	private Monitor(Logger tracer) {
		this.tracer = tracer;
		this.header = "SyncLite DB Server at " + Main.getHost() + ":" + Main.getPort();
		dumper = new FileDumper();
		try {
			dumper.init();
			dumper.schedule();
			monitorEnabled = true;
		} catch (Exception e) {
			tracer.warn("Monitor initialization failed. Dashboard stats will not be available.", e);
			monitorEnabled = false;
		}
	}

	public void disableMonitor() {
		if (dumper != null) {
			dumper.shutdown();
		}
		for (Dumper d : additionalDumpers) {
			if (d != null) {
				d.shutdown();
			}
		}
	}

	public boolean isMonitorEnabled() {
		return monitorEnabled;
	}

	public void recordStatChange() {
		lastStatChangeTime = System.currentTimeMillis();
	}
}
