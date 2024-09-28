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
import java.sql.SQLException;
import java.util.HashMap;
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
		private transient boolean inUse;
		private transient long lastUsed;
		
		public DBConnection(Connection c) {
			this.conn = c;
			this.inUse = false;
			this.lastUsed = System.currentTimeMillis();
		}
		
		Connection getConnection() {
			this.inUse = true;
			this.lastUsed = System.currentTimeMillis();
			return conn;
		}

		//Note that we implemented AutoClosable only to be able to record lastUsed time and set inUse to false
		//The actual connection is closed either on commits/rollbacks or by separate periodic connection cleaner 

		@Override
		public void close() throws Exception {
			this.inUse = false;
			this.lastUsed = System.currentTimeMillis();
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
        						dbConn.close();
        					} catch (Exception e) {
        						//Ignore
        					}
        					db.openConnections.remove(txnHandle);
        					++numCleanedConns;
        				} else {
        					numOpenConns++;
        				}
        			}
        			Main.globalTracer.info(db + " : cleaned connections : " + numCleanedConns);
        			Main.globalTracer.info(db + " : current open connections : " + numOpenConns);
        		}
        	} catch (Exception e) {
        		Main.globalTracer.error("Connection cleaner failed with error : " + e.getMessage());
        	}
        }
    };

	private static ScheduledExecutorService connCleaner = Executors.newScheduledThreadPool(1);
	private static HashMap<DeviceType, String> connStrPrefixes = new HashMap<DeviceType, String>();

	static {
		connStrPrefixes.put(DeviceType.SQLITE, "jdbc:synclite_sqlite:");
		connStrPrefixes.put(DeviceType.SQLITE_APPENDER, "jdbc:synclite_sqlite_appender:");
		connStrPrefixes.put(DeviceType.DUCKDB, "jdbc:synclite_duckdb:");
		connStrPrefixes.put(DeviceType.DUCKDB_APPENDER, "jdbc:synclite_duckdb_appender:");
		connStrPrefixes.put(DeviceType.DERBY, "jdbc:synclite_derby:");
		connStrPrefixes.put(DeviceType.DERBY_APPENDER, "jdbc:synclite_derby_appender:");
		connStrPrefixes.put(DeviceType.H2, "jdbc:synclite_h2:");
		connStrPrefixes.put(DeviceType.H2_APPENDER, "jdbc:synclite_h2_appender:");
		connStrPrefixes.put(DeviceType.HYPERSQL, "jdbc:synclite_hsqldb:");
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
	
	
	public UUID createConnectionForTxn() throws SQLException {
		Connection conn = createCachedConnection(url);
		UUID txnHandle = UUID.randomUUID();		
		openConnections.put(txnHandle, new DBConnection(conn));
		return txnHandle;
	}


	public void commitConnectionForTxn(UUID txnHandle) throws SQLException {
		DBConnection dbConn  = openConnections.get(txnHandle);
		if (dbConn == null) {
			throw new SQLException("No open connection found for txn-handle : " + txnHandle);
		}
		if (dbConn.conn.isClosed()) {
			throw new SQLException("Connection for specified txn-handle has been closed, restart the transaction");
		}
		dbConn.conn.commit();
		openConnections.remove(txnHandle);
	}

	public void rollbackConnectionForTxn(UUID txnHandle) throws SQLException {
		DBConnection dbConn = openConnections.get(txnHandle);
		if (dbConn == null) {
			throw new SQLException("No open connection found for txn-handle : " + txnHandle);
		}
		if (!dbConn.conn.isClosed()) {
			dbConn.conn.rollback();
		}
		openConnections.remove(txnHandle);
	}


	public DBConnection getConnectionForTxn(UUID txnHandle) {
		return openConnections.get(txnHandle);
	}

	private Connection createCachedConnection(String url) throws SQLException {
		Properties props = new Properties();
		props.put("config", this.syncliteLoggerConfig);
		props.put("device-name", this.name);
		Connection conn = DriverManager.getConnection(url);
		conn.setAutoCommit(false);
		return conn;
	}

	public void close() throws SQLException {
		//Close all open connections		
		for (DBConnection dbConn : openConnections.values()) {
			dbConn.conn.close();
		}
		//Close device
		SyncLite.closeDatabase(this.path);
	}
	
	
	public void init() throws SQLException, ClassNotFoundException {
		switch(type) {
		case SQLITE:
			Class.forName("io.synclite.logger.SQLite");
			SQLite.initialize(path, syncliteLoggerConfig, name);
			break;
		case SQLITE_APPENDER:
			Class.forName("io.synclite.logger.SQLiteAppender");
			SQLiteAppender.initialize(path, syncliteLoggerConfig, name);
			break;		
		case DUCKDB:
			Class.forName("io.synclite.logger.DuckDB");
			DuckDB.initialize(path, syncliteLoggerConfig, name);
			break;
		case DUCKDB_APPENDER:
			Class.forName("io.synclite.logger.DuckDBAppender");
			DuckDBAppender.initialize(path, syncliteLoggerConfig, name);
			break;		
		case H2:
			Class.forName("io.synclite.logger.H2");
			H2.initialize(path, syncliteLoggerConfig, name);
			break;
		case H2_APPENDER:
			Class.forName("io.synclite.logger.H2Appender");
			H2Appender.initialize(path, syncliteLoggerConfig, name);
			break;		
		case DERBY:
			Class.forName("io.synclite.logger.Derby");
			Derby.initialize(path, syncliteLoggerConfig, name);
			break;
		case DERBY_APPENDER:
			Class.forName("io.synclite.logger.DerbyAppender");
			DerbyAppender.initialize(path, syncliteLoggerConfig, name);
			break;		
		case HYPERSQL:
			Class.forName("io.synclite.logger.HyperSQL");
			HyperSQL.initialize(path, syncliteLoggerConfig, name);
			break;
		case HYPERSQL_APPENDER:
			Class.forName("io.synclite.logger.HyperSQLAppender");
			HyperSQLAppender.initialize(path, syncliteLoggerConfig, name);
			break;	
		case STREAMING:
			Class.forName("io.synclite.logger.Streaming");
			Streaming.initialize(path, syncliteLoggerConfig, name);
			break;
		}
	}

};
