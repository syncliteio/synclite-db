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

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.synclite.logger.*;

public class DB {

	public class DBConnection implements AutoCloseable {
		private Connection conn;
		private String ownerPrincipal;
		private transient boolean inUse;
		private transient long lastUsed;
		
		public DBConnection(Connection c, String ownerPrincipal) {
			this.conn = c;
			this.ownerPrincipal = ownerPrincipal;
			this.inUse = false;
			this.lastUsed = System.currentTimeMillis();
		}
		
		Connection getConnection() {
			this.inUse = true;
			this.lastUsed = System.currentTimeMillis();
			return conn;
		}

		String getOwnerPrincipal() {
			return ownerPrincipal;
		}

		//Note that we implemented AutoClosable only to be able to record lastUsed time and set inUse to false
		//The actual connection is closed either on commits/rollbacks or by separate periodic connection cleaner 

		@Override
		public void close() throws Exception {
			this.inUse = false;
			this.lastUsed = System.currentTimeMillis();
			if (Main.globalTracer != null && Main.globalTracer.isDebugEnabled()) {
				Main.globalTracer.debug("event=txn_conn_idle owner=" + ownerPrincipal + " lastUsed=" + this.lastUsed);
			}
		}		
	}
	
	public String name = "";
	private Path path;
	private DeviceType type;
	private Path syncliteLoggerConfig;
	private String url;

	public DB(String dbName, DeviceType dbType, Path dbPath, Path dbSyncLiteLoggerConfig) {
		this.name = dbName;
		this.type = dbType;
		this.path = dbPath;
		this.syncliteLoggerConfig = dbSyncLiteLoggerConfig;
		this.url = connStrPrefixes.get(dbType) + path;
	}


	public String getName() {
		return name;
	}
	
	public Path getPath() {
		return path;
	}
	
	public DeviceType getType() {
		return type;
	}
	
	public Path getSyncLiteLoggerConfig() {
		return syncliteLoggerConfig;
	}
	
	public String getURL() {
		return url;
	}
	
	@Override
	public String toString() {
		return "DB type : " + type + ", name : " + name + ", path : " + path;		
	}
	
	private static ConcurrentHashMap<Path, DB> databases = new ConcurrentHashMap<Path, DB>();
	private ConcurrentHashMap<UUID, DBConnection> openConnections = new ConcurrentHashMap<UUID, DBConnection>();
	private static ConcurrentHashMap<UUID, ResultSetPageState> openResultSets = new ConcurrentHashMap<UUID, ResultSetPageState>();

	private static class ResultSetPageState {
		private final UUID handle;
		private final Path dbPath;
		private final String requesterPrincipal;
		private final Connection connection;
		private final Statement statement;
		private final ResultSet resultSet;
		private Map<String, Object> bufferedRow;
		private int paginationSize;
		private long lastUsedMs;

		private ResultSetPageState(UUID handle, Path dbPath, String requesterPrincipal, Connection connection, Statement statement, ResultSet resultSet, Map<String, Object> bufferedRow, int paginationSize) {
			this.handle = handle;
			this.dbPath = dbPath;
			this.requesterPrincipal = requesterPrincipal;
			this.connection = connection;
			this.statement = statement;
			this.resultSet = resultSet;
			this.bufferedRow = bufferedRow;
			this.paginationSize = paginationSize;
			this.lastUsedMs = System.currentTimeMillis();
		}

		private void touch() {
			this.lastUsedMs = System.currentTimeMillis();
		}

		private void closeQuietly() {
			try {
				resultSet.close();
			} catch (Exception ignored) {
			}
			try {
				statement.close();
			} catch (Exception ignored) {
			}
			try {
				connection.close();
			} catch (Exception ignored) {
			}
		}
	}

	public static class ResultSetPage {
		public final List<Map<String, Object>> rows;
		public final UUID resultsetHandle;
		public final boolean hasMore;

		public ResultSetPage(List<Map<String, Object>> rows, UUID resultsetHandle, boolean hasMore) {
			this.rows = rows;
			this.resultsetHandle = resultsetHandle;
			this.hasMore = hasMore;
		}
	}

	private static Map<String, Object> readCurrentRow(ResultSet rs, ResultSetMetaData metaData, int columnCount) throws SQLException {
		Map<String, Object> row = new HashMap<String, Object>();
		for (int i = 1; i <= columnCount; i++) {
			row.put(metaData.getColumnLabel(i), rs.getObject(i));
		}
		return row;
	}

	private static void closeAndRemoveResultSetHandle(UUID handle) {
		if (handle == null) {
			return;
		}
		ResultSetPageState state = openResultSets.remove(handle);
		if (state != null) {
			state.closeQuietly();
		}
	}

	public static void closeAllOpenResultSets() {
		for (UUID handle : openResultSets.keySet()) {
			closeAndRemoveResultSetHandle(handle);
		}
	}

	private static void closeResultSetsForDb(Path dbPath) {
		if (dbPath == null) {
			return;
		}
		Iterator<Map.Entry<UUID, ResultSetPageState>> iterator = openResultSets.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<UUID, ResultSetPageState> entry = iterator.next();
			ResultSetPageState state = entry.getValue();
			if (dbPath.equals(state.dbPath)) {
				iterator.remove();
				state.closeQuietly();
			}
		}
	}

	public static void cleanupExpiredResultSetHandles() {
		long now = System.currentTimeMillis();
		long timeoutMs = ConfLoader.getInstance().getResultsetHandleTimeoutMs();
		Iterator<Map.Entry<UUID, ResultSetPageState>> iterator = openResultSets.entrySet().iterator();
		int cleaned = 0;
		while (iterator.hasNext()) {
			Map.Entry<UUID, ResultSetPageState> entry = iterator.next();
			ResultSetPageState state = entry.getValue();
			if (now - state.lastUsedMs > timeoutMs) {
				iterator.remove();
				state.closeQuietly();
				cleaned++;
			}
		}
		if (cleaned > 0 && Main.globalTracer != null) {
			Main.globalTracer.info("event=resultset_handle_cleanup cleaned=" + cleaned + " open=" + openResultSets.size());
		}
	}

	public boolean isResultsetConnectionRetained(Connection conn) {
		if (conn == null) {
			return false;
		}
		for (ResultSetPageState state : openResultSets.values()) {
			if (state.connection == conn) {
				return true;
			}
		}
		return false;
	}

	public ResultSetPage createPagedResultSetPage(Connection conn, Statement stmt, ResultSet rs, String requesterPrincipal, int paginationSize) throws SQLException {
		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();

		while (results.size() < paginationSize + 1 && rs.next()) {
			results.add(readCurrentRow(rs, metaData, columnCount));
		}

		if (results.size() <= paginationSize) {
			rs.close();
			stmt.close();
			return new ResultSetPage(results, null, false);
		}

		Map<String, Object> bufferedRow = results.remove(results.size() - 1);
		String effectiveRequester = requesterPrincipal == null ? "anonymous" : requesterPrincipal;
		UUID handle = UUID.randomUUID();
		ResultSetPageState state = new ResultSetPageState(handle, this.path, effectiveRequester, conn, stmt, rs, bufferedRow, paginationSize);
		openResultSets.put(handle, state);
		return new ResultSetPage(results, handle, true);
	}

	public static ResultSetPage fetchNextResultSetPage(UUID handle, String requesterPrincipal, int requestedPaginationSize) throws SQLException {
		ResultSetPageState state = openResultSets.get(handle);
		if (state == null) {
			throw new SQLException("No open resultset found for resultset-handle : " + handle);
		}

		String effectiveRequester = requesterPrincipal == null ? "anonymous" : requesterPrincipal;
		if (state.requesterPrincipal == null || !state.requesterPrincipal.equals(effectiveRequester)) {
			throw new SQLException("resultset-handle is not owned by requester");
		}

		int pageSize = requestedPaginationSize > 0 ? requestedPaginationSize : state.paginationSize;
		state.paginationSize = pageSize;
		state.touch();

		try {
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			if (state.bufferedRow != null) {
				results.add(state.bufferedRow);
				state.bufferedRow = null;
			}

			ResultSetMetaData metaData = state.resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			while (results.size() < pageSize + 1 && state.resultSet.next()) {
				results.add(readCurrentRow(state.resultSet, metaData, columnCount));
			}

			if (results.size() <= pageSize) {
				closeAndRemoveResultSetHandle(handle);
				return new ResultSetPage(results, null, false);
			}

			state.bufferedRow = results.remove(results.size() - 1);
			state.touch();
			return new ResultSetPage(results, handle, true);
		} catch (Exception e) {
			closeAndRemoveResultSetHandle(handle);
			throw new SQLException("Failed to fetch next page : " + e.getMessage(), e);
		}
	}

	// Define the doClean task as a Runnable
    private static final Runnable doClean = new Runnable() {
        @Override
        public void run() {
        	try {
        		for (DB db : databases.values()) {
        			long numOpenConns = 0;
        			long numCleanedConns = 0;
        			for (Map.Entry<UUID, DBConnection> entry : db.openConnections.entrySet()) {
        				UUID txnHandle = entry.getKey();
        				DBConnection dbConn = entry.getValue();
        				long idleSince = 0;
        				if (!dbConn.inUse) {
        					idleSince = System.currentTimeMillis() - dbConn.lastUsed;
        				}
        				
        				if (idleSince > ConfLoader.getInstance().getIdleConnectionTimeout()){
        					try {
        						dbConn.conn.rollback();
							} catch (Exception e) {
								if (Main.globalTracer != null && Main.globalTracer.isDebugEnabled()) {
									Main.globalTracer.debug("event=conn_cleaner_rollback_ignored txn-handle=" + txnHandle + " error=" + e.getMessage());
								}
							}
							try {
								dbConn.conn.close();
								dbConn.close();
        					} catch (Exception e) {
								if (Main.globalTracer != null && Main.globalTracer.isDebugEnabled()) {
									Main.globalTracer.debug("event=conn_cleaner_close_ignored txn-handle=" + txnHandle + " error=" + e.getMessage());
								}
        					}
        					db.openConnections.remove(txnHandle);
        					++numCleanedConns;
        				} else {
        					numOpenConns++;
        				}
        			}
					Main.globalTracer.info("event=conn_cleaner_summary db=\"" + db + "\" cleaned=" + numCleanedConns + " open=" + numOpenConns);
        		}
				cleanupExpiredResultSetHandles();
        	} catch (Exception e) {
				Main.globalTracer.error("event=conn_cleaner_failure error=" + e.getMessage(), e);
        	}
        }
    };

	private static ScheduledExecutorService connCleaner = Executors.newScheduledThreadPool(1);
	private static HashMap<DeviceType, String> connStrPrefixes = new HashMap<DeviceType, String>();

	static {
		connStrPrefixes.put(DeviceType.SQLITE, "jdbc:synclite_sqlite:");
		connStrPrefixes.put(DeviceType.SQLITE_STORE, "jdbc:synclite_sqlite_store:");
		connStrPrefixes.put(DeviceType.SQLITE_APPENDER, "jdbc:synclite_sqlite_appender:");
		connStrPrefixes.put(DeviceType.DUCKDB, "jdbc:synclite_duckdb:");
		connStrPrefixes.put(DeviceType.DUCKDB_STORE, "jdbc:synclite_duckdb_store:");
		connStrPrefixes.put(DeviceType.DUCKDB_APPENDER, "jdbc:synclite_duckdb_appender:");
		connStrPrefixes.put(DeviceType.DERBY, "jdbc:synclite_derby:");
		connStrPrefixes.put(DeviceType.DERBY_STORE, "jdbc:synclite_derby_store:");
		connStrPrefixes.put(DeviceType.DERBY_APPENDER, "jdbc:synclite_derby_appender:");
		connStrPrefixes.put(DeviceType.H2, "jdbc:synclite_h2:");
		connStrPrefixes.put(DeviceType.H2_STORE, "jdbc:synclite_h2_store:");
		connStrPrefixes.put(DeviceType.H2_APPENDER, "jdbc:synclite_h2_appender:");
		connStrPrefixes.put(DeviceType.HYPERSQL, "jdbc:synclite_hsqldb:");
		connStrPrefixes.put(DeviceType.HYPERSQL_STORE, "jdbc:synclite_hsqldb_store:");
		connStrPrefixes.put(DeviceType.HYPERSQL_APPENDER, "jdbc:synclite_hsqldb_appender:");
		connStrPrefixes.put(DeviceType.STREAMING, "jdbc:synclite_streaming:");		

		connCleaner.scheduleWithFixedDelay(doClean, ConfLoader.getInstance().getIdleConnectionTimeout(), ConfLoader.getInstance().getIdleConnectionTimeout(), TimeUnit.MILLISECONDS);
	}
	
	public static void addDatabase(DB db) {
		databases.put(db.path, db);
	}

	public static void removeDatabase(DB db) {
		databases.remove(db.path);
	}

	public static DB getDatabase(Path dbPath) {
		return databases.get(dbPath);
	}
	
	
	public UUID createConnectionForTxn(String ownerPrincipal) throws SQLException {
		if (ownerPrincipal == null || ownerPrincipal.isBlank()) {
			ownerPrincipal = "anonymous";
		}
		Connection conn = createCachedConnection(url);
		UUID txnHandle = UUID.randomUUID();		
		openConnections.put(txnHandle, new DBConnection(conn, ownerPrincipal));
		if (Main.globalTracer != null && Main.globalTracer.isDebugEnabled()) {
			Main.globalTracer.debug("event=txn_begin txn-handle=" + txnHandle + " owner=" + ownerPrincipal + " db=\"" + this + "\"");
		}
		return txnHandle;
	}

	private DBConnection getAndValidateConnectionForTxn(UUID txnHandle, String requesterPrincipal) throws SQLException {
		DBConnection dbConn = openConnections.get(txnHandle);
		if (dbConn == null) {
			throw new SQLException("No open connection found for txn-handle : " + txnHandle);
		}

		if (requesterPrincipal == null || requesterPrincipal.isBlank()) {
			requesterPrincipal = "anonymous";
		}

		String ownerPrincipal = dbConn.getOwnerPrincipal();
		if (ownerPrincipal == null || ownerPrincipal.isBlank()) {
			ownerPrincipal = "anonymous";
		}

		if (!ownerPrincipal.equals(requesterPrincipal)) {
			if (Main.globalTracer != null) {
				Main.globalTracer.warn("event=txn_ownership_rejected code=ERR_TXN_OWNERSHIP txn-handle=" + txnHandle + " owner=" + ownerPrincipal + " requester=" + requesterPrincipal);
			}
			throw new SQLException("txn-handle is not owned by requester");
		}

		return dbConn;
	}


	public void commitConnectionForTxn(UUID txnHandle, String requesterPrincipal) throws SQLException {
		DBConnection dbConn  = getAndValidateConnectionForTxn(txnHandle, requesterPrincipal);
		if (dbConn.conn.isClosed()) {
			throw new SQLException("Connection for specified txn-handle has been closed, restart the transaction");
		}
		try {
			dbConn.conn.commit();
			if (Main.globalTracer != null && Main.globalTracer.isDebugEnabled()) {
				Main.globalTracer.debug("event=txn_commit txn-handle=" + txnHandle + " requester=" + requesterPrincipal + " db=\"" + this + "\"");
			}
		} finally {
			try {
				dbConn.conn.close();
			} catch (SQLException ignore) {
			}
			openConnections.remove(txnHandle);
		}
	}

	public void rollbackConnectionForTxn(UUID txnHandle, String requesterPrincipal) throws SQLException {
		DBConnection dbConn = getAndValidateConnectionForTxn(txnHandle, requesterPrincipal);
		try {
			if (!dbConn.conn.isClosed()) {
				dbConn.conn.rollback();
				if (Main.globalTracer != null && Main.globalTracer.isDebugEnabled()) {
					Main.globalTracer.debug("event=txn_rollback txn-handle=" + txnHandle + " requester=" + requesterPrincipal + " db=\"" + this + "\"");
				}
			}
		} finally {
			try {
				dbConn.conn.close();
			} catch (SQLException ignore) {
			}
			openConnections.remove(txnHandle);
		}
	}


	public DBConnection getConnectionForTxn(UUID txnHandle, String requesterPrincipal) throws SQLException {
		return getAndValidateConnectionForTxn(txnHandle, requesterPrincipal);
	}

	private Connection createCachedConnection(String url) throws SQLException {
		Properties props = new Properties();
		props.put("config", this.syncliteLoggerConfig);
		props.put("device-name", this.name);
		Connection conn = DriverManager.getConnection(url, props);
		conn.setAutoCommit(false);
		return conn;
	}

	public void close() throws SQLException {
		//Close all open connections		
		if (Main.globalTracer != null) {
			Main.globalTracer.info("event=db_close_start db=\"" + this + "\" open-connections=" + openConnections.size());
		}
		closeResultSetsForDb(this.path);
		for (DBConnection dbConn : openConnections.values()) {
			try {
				dbConn.conn.close();
			} catch (SQLException ignore) {
				if (Main.globalTracer != null && Main.globalTracer.isDebugEnabled()) {
					Main.globalTracer.debug("event=db_close_conn_ignored db=\"" + this + "\" error=" + ignore.getMessage());
				}
			}
		}
		openConnections.clear();
		//Close device
		SyncLite.closeDatabase(this.path);
	}
	
	
	public void init() throws SQLException {
		switch(type) {
		case SQLITE:
			SQLite.initialize(path, syncliteLoggerConfig, name);
			break;
		case SQLITE_STORE:
			SQLiteStore.initialize(path, syncliteLoggerConfig, name);
			break;
		case SQLITE_APPENDER:
			SQLiteAppender.initialize(path, syncliteLoggerConfig, name);
			break;		
		case DUCKDB:
			DuckDB.initialize(path, syncliteLoggerConfig, name);
			break;
		case DUCKDB_STORE:
			DuckDBStore.initialize(path, syncliteLoggerConfig, name);
			break;
		case DUCKDB_APPENDER:
			DuckDBAppender.initialize(path, syncliteLoggerConfig, name);
			break;		
		case H2:
			H2.initialize(path, syncliteLoggerConfig, name);
			break;
		case H2_STORE:
			H2Store.initialize(path, syncliteLoggerConfig, name);
			break;
		case H2_APPENDER:
			H2Appender.initialize(path, syncliteLoggerConfig, name);
			break;		
		case DERBY:
			Derby.initialize(path, syncliteLoggerConfig, name);
			break;
		case DERBY_STORE:
			DerbyStore.initialize(path, syncliteLoggerConfig, name);
			break;
		case DERBY_APPENDER:
			DerbyAppender.initialize(path, syncliteLoggerConfig, name);
			break;		
		case HYPERSQL:
			HyperSQL.initialize(path, syncliteLoggerConfig, name);
			break;
		case HYPERSQL_STORE:
			HyperSQLStore.initialize(path, syncliteLoggerConfig, name);
			break;
		case HYPERSQL_APPENDER:
			HyperSQLAppender.initialize(path, syncliteLoggerConfig, name);
			break;	
		case STREAMING:
			Streaming.initialize(path, syncliteLoggerConfig, name);
			break;
		}
	}

};
