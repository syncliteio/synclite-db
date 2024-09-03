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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import io.synclite.logger.Derby;
import io.synclite.logger.DerbyAppender;
import io.synclite.logger.DuckDB;
import io.synclite.logger.DuckDBAppender;
import io.synclite.logger.H2;
import io.synclite.logger.H2Appender;
import io.synclite.logger.HyperSQL;
import io.synclite.logger.HyperSQLAppender;
import io.synclite.logger.SQLite;
import io.synclite.logger.SQLiteAppender;
import io.synclite.logger.Streaming;
import io.synclite.logger.SyncLite;


public class Main {

	private static Path dbDir;
	private static Path stageDir;
	private static HashMap<DeviceType, String> connStrPrefixes = new HashMap<DeviceType, String>();
	private static ConcurrentHashMap<Path, DB> databases = new ConcurrentHashMap<Path, DB>();
	private static Path dbConfigFilePath = null;
	private static Logger globalTracer;

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
	}

	public static void main(String[] args) {

		//Validate args
		if (args.length > 0) {
			if (args.length == 2) {
				if (!args[0].trim().equals("--config")) {
					usage();
				} else {
					dbConfigFilePath = Path.of(args[1]);
					if (!Files.exists(dbConfigFilePath)) {
						error(new Exception("Invalid configuration file specified : " + dbConfigFilePath));
					}
				}
			} else {
				usage();
			}
		}

		initDB();

		// Register a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				SyncLite.closeAllDatabases();
			} catch (SQLException e) {
			}
			System.out.println("Shutting down gracefully...");
		}));

		try (ZContext context = new ZContext()) {

			ExecutorService threadPool = Executors.newFixedThreadPool(ConfLoader.getInstance().getNumThreads());

	        // Create and bind a REP socket for each worker thread
            for (int i = 0; i < ConfLoader.getInstance().getNumThreads() ; i++) {
                threadPool.submit(() -> {
                    ZMQ.Socket workerSocket = context.createSocket(SocketType.REP);
                    workerSocket.bind(ConfLoader.getInstance().getAddress().toString());

                    while (!Thread.currentThread().isInterrupted()) {
                        // Receive a request
                        String request = workerSocket.recvStr(0);
                        globalTracer.debug("Received request: " + request);

                        // Process the request
                        String response = processRequest(request);

                        // Send the response back to the client
                        workerSocket.send(response, 0);
                    }

                    workerSocket.close();
                });
            }
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
    				SyncLite.closeAllDatabases();
    			} catch (SQLException e) {
    			}

                threadPool.shutdownNow();
                System.out.println("SyncLite DB Server is shutting down.");
                globalTracer.info("SyncLite DB Server is shutting down.");
            }));
		}
	}

	private static void initDB() {
		try {
			initPaths();
			if (dbConfigFilePath == null) {
				createDefaultDBConf();
			}
			ConfLoader.getInstance().loadDBConfigProperties(dbConfigFilePath);
			createDefaultSyncLiteLoggerConf();
			initLogger();
			dumpHeader();
		} catch (Exception e) {
			error(new Exception("Error initializing database : " + e.getMessage(), e));
		}

	}

	private static void initPaths() throws SQLException {
		dbDir = Path.of(System.getProperty("user.home"), "synclite", "job1", "db");        	
		stageDir = Path.of(System.getProperty("user.home"), "synclite", "job1", "stageDir");        	
		try {
			Files.createDirectories(dbDir);
		} catch (IOException e) {
			throw new SQLException("Failed to create default db directory : " + dbDir, e);
		}

		try {
			Files.createDirectories(stageDir);
		} catch (IOException e) {
			throw new SQLException("Failed to create default stage directory : " + stageDir, e);
		}
	}

	private static String processRequest(String request) {
		try {
			JSONObject jsonRequest = new JSONObject(request);
			DB db;
			Path dbPath = null;
			if (jsonRequest.has("db-path")) {
				dbPath = Path.of(jsonRequest.getString("db-path"));
				if (!Files.exists(dbPath.getParent())) {
					return createJsonResponse("Parent directory of specified db-path : " + dbPath + " does not exist", null);
				}
				db = databases.get(dbPath);
			} else {
				return createJsonResponse("device-path must be specified : ", null);				
			}

			if (db == null) {	
				db = new DB();
				db.path = dbPath;

				if (jsonRequest.has("db-type")) {
					String type = jsonRequest.getString("db-type");
					if (type.isBlank()) {
						return createJsonResponse("db-type must be specified : ", null);
					} else {					
						try {
							db.type = DeviceType.valueOf(type);
						} catch (Exception e) {
							return createJsonResponse("Invalid db-type : "  + type + " specified", null);
						}
					}
				} else {
					return createJsonResponse("db-type must be specified : ", null);
				}

				if (jsonRequest.has("db-name")) {
					db.name = jsonRequest.getString("db-name");
				}

				db.syncliteLoggerConfig = dbDir.resolve("synclite_logger.conf");
				if (jsonRequest.has("synclite-logger-config")) {
					String config = jsonRequest.getString("synclite-logger-config");
					if (config.isBlank()) {
						return createJsonResponse("Empty synclite-logger-config specified", null);
					}
					db.syncliteLoggerConfig = Path.of(config);
				} else {
					db.syncliteLoggerConfig = dbDir.resolve("synclite_logger.conf");
				}

				if (!Files.exists(db.syncliteLoggerConfig)) {
					return createJsonResponse("Specified synclite-logger-config does not exist", null);
				}
			}

			String sql;
			if (jsonRequest.has("sql")) {
				sql = jsonRequest.getString("sql");
				if (sql.isBlank()) {
					return createJsonResponse("sql must be specified : ", null);
				} 
			} else {
				return createJsonResponse("sql must be specified : ", null);
			}

			//
			//initialize if the command type is initialize or the device has not been initialized yet after last server restart.
			//
			if (sql.equalsIgnoreCase("initialize")) {				
				try {
					initDevice(db);
					databases.put(db.path, db);					
					return createJsonResponse("Device initialized successfully : ", null);
				} catch (Exception e) {
					return createJsonResponse("Failed to initialize device : " + db + " : " + e.getMessage(), null);
				}
			} 

			// Extract multiple sets of arguments for batch processing
			List<List<Object>> argumentSets = new ArrayList<>();

			if (jsonRequest.has("arguments")) {
				JSONArray argumentSetsArray = jsonRequest.getJSONArray("arguments");

				for (int j = 0; j < argumentSetsArray.length(); j++) {
					JSONArray argumentsArray = argumentSetsArray.getJSONArray(j);
					List<Object> arguments = new ArrayList<>();

					for (int i = 0; i < argumentsArray.length(); i++) {
						Object arg = argumentsArray.get(i);
						arguments.add(arg); // Add more type checks if needed
					}
					argumentSets.add(arguments);
				}
			}

			// Open a connection to the specified SyncLite device
			String prefix = connStrPrefixes.get(db.type);
			Properties props = new Properties();
			props.put("config", db.syncliteLoggerConfig);
			props.put("device-name", db.name);
			try (Connection conn = DriverManager.getConnection(prefix + db.path, props)) {
				return executeSql(conn, sql, argumentSets);
			} catch (SQLException e) {
				return createJsonResponse("DB connection error: " + e.getMessage(), null);
			}
		} catch (JSONException e) {
			return createJsonResponse("Invalid JSON request : " + e.getMessage(), null);
		}
	}

	private static void initDevice(DB device) throws SQLException, ClassNotFoundException {
		switch(device.type) {
		case SQLITE:
			Class.forName("io.synclite.logger.SQLite");
			SQLite.initialize(device.path, device.syncliteLoggerConfig, device.name);
			break;
		case SQLITE_APPENDER:
			Class.forName("io.synclite.logger.SQLiteAppender");
			SQLiteAppender.initialize(device.path, device.syncliteLoggerConfig, device.name);
			break;		
		case DUCKDB:
			Class.forName("io.synclite.logger.DuckDB");
			DuckDB.initialize(device.path, device.syncliteLoggerConfig, device.name);
			break;
		case DUCKDB_APPENDER:
			Class.forName("io.synclite.logger.DuckDBAppender");
			DuckDBAppender.initialize(device.path, device.syncliteLoggerConfig, device.name);
			break;		
		case H2:
			Class.forName("io.synclite.logger.H2");
			H2.initialize(device.path, device.syncliteLoggerConfig, device.name);
			break;
		case H2_APPENDER:
			Class.forName("io.synclite.logger.H2Appender");
			H2Appender.initialize(device.path, device.syncliteLoggerConfig, device.name);
			break;		
		case DERBY:
			Class.forName("io.synclite.logger.Derby");
			Derby.initialize(device.path, device.syncliteLoggerConfig, device.name);
			break;
		case DERBY_APPENDER:
			Class.forName("io.synclite.logger.DerbyAppender");
			DerbyAppender.initialize(device.path, device.syncliteLoggerConfig, device.name);
			break;		
		case HYPERSQL:
			Class.forName("io.synclite.logger.HyperSQL");
			HyperSQL.initialize(device.path, device.syncliteLoggerConfig, device.name);
			break;
		case HYPERSQL_APPENDER:
			Class.forName("io.synclite.logger.HyperSQLAppender");
			HyperSQLAppender.initialize(device.path, device.syncliteLoggerConfig, device.name);
			break;	
		case STREAMING:
			Class.forName("io.synclite.logger.Streaming");
			Streaming.initialize(device.path, device.syncliteLoggerConfig, device.name);
			break;
		}
	}

	private static String executeSql(Connection conn, String sql, List<List<Object>> argumentSets) {
		boolean isQuery = sql.trim().toUpperCase().startsWith("SELECT");
		try {
			if (isQuery) {
				return executeQuery(conn, sql, argumentSets);
			} else if (argumentSets.size() > 0) {
				return executeBatch(conn, sql, argumentSets);
			} else {
				return executeUpdate(conn, sql);
			}
		} catch (SQLException e) {
			return createJsonResponse("SQL Error: " + e.getMessage(), null);
		}
	}

	private static String executeQuery(Connection conn, String sql, List<List<Object>> argumentSets) throws SQLException {
		try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
			// Bind parameters (assuming only one set of arguments for queries)
			if (!argumentSets.isEmpty()) {
				List<Object> arguments = argumentSets.get(0);
				for (int i = 0; i < arguments.size(); i++) {
					pstmt.setObject(i + 1, arguments.get(i));
				}
			}

			try (ResultSet rs = pstmt.executeQuery()) {
				List<Map<String, Object>> results = new ArrayList<>();
				ResultSetMetaData metaData = rs.getMetaData();
				int columnCount = metaData.getColumnCount();

				while (rs.next()) {
					Map<String, Object> row = new HashMap<>();
					for (int i = 1; i <= columnCount; i++) {
						row.put(metaData.getColumnName(i), rs.getObject(i));
					}
					results.add(row);
				}

				return createJsonResponse( results.size() + " rows", results);
			}
		}
	}

	private static String executeBatch(Connection conn, String sql, List<List<Object>> argumentSets) throws SQLException {
		try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
			// Bind parameters for each argument set and add to batch
			for (List<Object> arguments : argumentSets) {
				for (int i = 0; i < arguments.size(); i++) {
					pstmt.setObject(i + 1, arguments.get(i));
				}
				pstmt.addBatch();
			}


			// Execute batch
			int[] updateCounts = pstmt.executeBatch();

			// Convert the integer array to a JSON array
			JSONArray updateCountsJsonArray = new JSONArray();
			for (int count : updateCounts) {
				updateCountsJsonArray.put(count);
			}
			return createJsonResponse("Batch executed successfully, rows affected: " + updateCounts.length, updateCountsJsonArray);
		}
	}

	private static String executeUpdate(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			int rowsAffected = stmt.executeUpdate(sql);
			return createJsonResponse("Update executed successfully, rows affected: " + rowsAffected, null);
		}
	}

	private static String createJsonResponse(String message, Object result) {
		JSONObject jsonResponse = new JSONObject();
		try {
			jsonResponse.put("message", message);
			jsonResponse.put("result", result);
		} catch (JSONException e) {
			jsonResponse.put("message", "Error creating JSON response");
			jsonResponse.put("result", JSONObject.NULL);
		}
		return jsonResponse.toString();
	}

	private static void createDefaultDBConf() throws SQLException {
        String currentDirectory = System.getProperty("user.dir");
		dbConfigFilePath = Path.of(currentDirectory, "synclite_db.conf");
		if (Files.exists(dbConfigFilePath)) {
			return;
		}

		StringBuilder confBuilder = new StringBuilder();
		String newLine = System.getProperty("line.separator");

		confBuilder.append("#==============SyncLite DB Configurations==================");
		confBuilder.append(newLine);
		confBuilder.append("address=").append("tcp://localhost:5555");
		confBuilder.append(newLine);
		confBuilder.append("num-threads=4");
		confBuilder.append(newLine);
		confBuilder.append("trace-level=INFO");
		String confStr = confBuilder.toString();

		try {
			Files.writeString(dbConfigFilePath, confStr);
		} catch (IOException e) {
			throw new SQLException("Failed to create a default SyncLite DB configuration file : " + dbConfigFilePath, e);
		}
	}

	private static void createDefaultSyncLiteLoggerConf() throws SQLException {
		Path confPath = dbDir.resolve("synclite_logger.conf");
		if (Files.exists(confPath)) {
			return;
		}

		StringBuilder confBuilder = new StringBuilder();
		String newLine = System.getProperty("line.separator");

		confBuilder.append("#==============Device Stage Properties==================");
		confBuilder.append(newLine);
		confBuilder.append("local-data-stage-directory=").append(stageDir);
		confBuilder.append(newLine);
		confBuilder.append("#local-data-stage-directory=<path/to/local/stage/directory>");
		confBuilder.append(newLine);
		confBuilder.append("destination-type=FS");
		confBuilder.append(newLine);
		confBuilder.append("#destination-type=<FS|MS_ONEDRIVE|GOOGLE_DRIVE|SFTP|MINIO|KAFKA|S3>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============SFTP Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#sftp:host=<host name of remote host for shipping device log files>");
		confBuilder.append(newLine);
		confBuilder.append("#sftp:user-name=<user name to connect to remote host>");
		confBuilder.append(newLine);
		confBuilder.append("#sftp:password=<password>");
		confBuilder.append(newLine);
		confBuilder.append("#sftp:remote-data-stage-directory=<remote data directory name which will host the device directory>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);	
		confBuilder.append("#==============MinIO  Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#minio:endpoint=<MinIO endpoint to upload devices>");
		confBuilder.append(newLine);
		confBuilder.append("#minio:bucket-name=<MinIO bucket name>");
		confBuilder.append(newLine);
		confBuilder.append("#minio:access-key=<MinIO access key>");
		confBuilder.append(newLine);
		confBuilder.append("#minio:secret-key=<MinIO secret key>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);	
		confBuilder.append("#==============S3 Configuration=====================");
		confBuilder.append(newLine);
		confBuilder.append("#s3:endpoint=https://s3-<region>.amazonaws.com");
		confBuilder.append(newLine);
		confBuilder.append("#s3:bucket-name=<S3 bucket name>");
		confBuilder.append(newLine);
		confBuilder.append("#s3:access-key=<S3 access key>");
		confBuilder.append(newLine);
		confBuilder.append("#s3:secret-key=<S3 secret key>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Kafka Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#kafka:bootstrap.servers=localhost:9092,localhost:9093,localhost:9094");
		confBuilder.append(newLine);
		confBuilder.append("#kafka:<any_other_kafka_producer_property> = <kafka_producer_property_value>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Table filtering Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#include-tables=<comma separate table list>");
		confBuilder.append(newLine);
		confBuilder.append("#exclude-tables=<comma separate table list>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Logger Configuration==================");	
		confBuilder.append("#log-queue-size=2147483647");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-flush-batch-size=1000000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-switch-log-count-threshold=1000000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-switch-duration-threshold-ms=5000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-shipping-frequency-ms=5000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-page-size=4096");
		confBuilder.append(newLine);
		confBuilder.append("#log-max-inlined-arg-count=16");
		confBuilder.append(newLine);
		confBuilder.append("#use-precreated-data-backup=false");
		confBuilder.append(newLine);
		confBuilder.append("#vacuum-data-backup=true");
		confBuilder.append(newLine);
		confBuilder.append("#skip-restart-recovery=false");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Device Configuration==================");
		confBuilder.append(newLine);
		String deviceEncryptionKeyFile = Path.of(System.getProperty("user.home"), ".ssh", "synclite_public_key.der").toString();
		confBuilder.append("#device-encryption-key-file=" + deviceEncryptionKeyFile);
		confBuilder.append(newLine);
		confBuilder.append("#device-name=");
		confBuilder.append(newLine);	

		String confStr = confBuilder.toString();

		try {
			Files.writeString(confPath, confStr);
		} catch (IOException e) {
			throw new SQLException("Failed to create a default SyncLite logger configuration file : " + confPath, e);
		}
	}


	private static final void error(Exception e) {
		System.out.println("ERROR : " + e.getMessage());
		System.exit(1);
	}

	private static final void usage() {
		System.out.println("Usage:"); 
		System.out.println("synclite-db");
		System.out.println("synclite-db --config <path/to/config-file>");
		System.exit(1);
	}

	private static void dumpHeader() {
		// Dump version from version file.
		ClassLoader classLoader = Main.class.getClassLoader();
		String version = "UNKNOWN";
		try (InputStream inputStream = classLoader.getResourceAsStream("synclite.version")) {
			if (inputStream != null) {
				// Read the contents of the file as a string
				Scanner scanner = new Scanner(inputStream, "UTF-8");
				version = scanner.useDelimiter("\\A").next();
			}
		} catch (Exception e) {
			// Skip
		}
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("\n");
		builder.append("===================SyncLite DB " + version + "==========================");
		builder.append("\n");
		builder.append("Starting with configuration");
		builder.append("\n");
		builder.append("address : " + ConfLoader.getInstance().getAddress());
		builder.append("\n");
		builder.append("num-threads : " + ConfLoader.getInstance().getNumThreads());
		builder.append("\n");
		builder.append("trace-level : " + ConfLoader.getInstance().getTraceLevel());
		builder.append("\n");
		builder.append("========================================================================");
		builder.append("\n");		
		
		System.out.print(builder.toString());
		globalTracer.info(builder.toString());
	}


	static private final void initLogger() {
		globalTracer = Logger.getLogger(Main.class);    	
		globalTracer.setLevel(ConfLoader.getInstance().getTraceLevel());
		RollingFileAppender fa = new RollingFileAppender();
		fa.setName("FileLogger");
        String currentDirectory = System.getProperty("user.dir");
		fa.setFile(Path.of(currentDirectory, "synclite_db.trace").toString());
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setMaxBackupIndex(10);
		fa.setMaxFileSize("10MB"); // Set the maximum file size to 10 MB
		fa.setAppend(true);
		fa.activateOptions();
		globalTracer.addAppender(fa);    	
	}

}
