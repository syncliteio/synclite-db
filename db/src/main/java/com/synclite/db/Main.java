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
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
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
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.synclite.db.DB.DBConnection;

import io.synclite.logger.SyncLite;

public class Main {

	private static Path dbDir;
	private static Path stageDir;
	private static Path dbConfigFilePath = null;
	public static Logger globalTracer;

	// Define a handler to respond to requests
	static class SyncLiteHTTPHandler implements HttpHandler {
		@Override
		public void handle(HttpExchange exchange) throws IOException {

			String requestMethod = exchange.getRequestMethod();
			String response = "";

			if ("POST".equals(requestMethod)) {
				InputStream inputStream = exchange.getRequestBody();
				String requestBody = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
				globalTracer.debug("Received request: " + requestBody);

				response = processRequest(requestBody);

				exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.getBytes().length);
				try (OutputStream os = exchange.getResponseBody()) {
					os.write(response.getBytes());
				}
			}  else if ("GET".equals(requestMethod)) {
				response = "Server is up and running!";

				exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.getBytes().length);

				// Send the response message
				try (OutputStream os = exchange.getResponseBody()) {
					os.write(response.getBytes());
				}
			} else {
				// Handle other HTTP methods (e.g., PUT, DELETE)
				String methodNotAllowed = "Method Not Allowed. Use POST.";
				exchange.sendResponseHeaders(405, methodNotAllowed.length());
				OutputStream outputStream = exchange.getResponseBody();
				outputStream.write(methodNotAllowed.getBytes(StandardCharsets.UTF_8));
				outputStream.close();
			}
		}
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

		try {
			// Create an HTTP server and bind it to a port
			HttpServer server = HttpServer.create(new InetSocketAddress(ConfLoader.getInstance().getPort()), 0);

			server.createContext("/", new SyncLiteHTTPHandler());

			ExecutorService threadPool = Executors.newFixedThreadPool(ConfLoader.getInstance().getNumThreads());
			server.setExecutor(threadPool);

			// Start the server
			server.start();

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				try {
					SyncLite.closeAllDatabases();
				} catch (SQLException e) {
				}
				server.stop(1);
				threadPool.shutdownNow();
				System.out.println("SyncLiteDB Server is shutting down.");
				globalTracer.info("SyncLiteDB Server is shutting down.");
			}));
		} catch (Exception e) {
			globalTracer.error("Failed to start server : " + e.getMessage(), e);
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
			DB db = null;
			Path dbPath = null;
			DeviceType dbType = null;
			String dbName = null;
			Path dbSyncLiteLoggerConfig = null;

			if (jsonRequest.has("db-path")) {
				dbPath = Path.of(jsonRequest.getString("db-path"));
				if (!Files.exists(dbPath.getParent())) {
					return createJsonResponse(false, "Parent directory of specified db-path : " + dbPath + " does not exist", null);
				}
				db = DB.getDatabase(dbPath);
			} else {
				return createJsonResponse(false, "db-path must be specified : ", null);				
			}

			if (db == null) {
				if (jsonRequest.has("db-type")) {
					String type = jsonRequest.getString("db-type");
					if (type.isBlank()) {
						return createJsonResponse(false, "db-type must be specified : ", null);
					} else {					
						try {
							dbType = DeviceType.valueOf(type);
						} catch (Exception e) {
							return createJsonResponse(false, "Invalid db-type : "  + type + " specified", null);
						}
					}
				} else {
					return createJsonResponse(false, "db-type must be specified : ", null);
				}

				if (jsonRequest.has("db-name")) {
					dbName = jsonRequest.getString("db-name");
				}

				dbSyncLiteLoggerConfig = dbDir.resolve("synclite_logger.conf");
				if (jsonRequest.has("synclite-logger-config")) {
					String config = jsonRequest.getString("synclite-logger-config");
					if (config.isBlank()) {
						return createJsonResponse(false, "Empty synclite-logger-config specified", null);
					}
					dbSyncLiteLoggerConfig = Path.of(config);
				} else {
					dbSyncLiteLoggerConfig = dbDir.resolve("synclite_logger.conf");
				}

				if (!Files.exists(dbSyncLiteLoggerConfig)) {
					return createJsonResponse(false, "Specified synclite-logger-config does not exist", null);
				}

				db = new DB(dbName, dbType, dbPath, dbSyncLiteLoggerConfig);
			}

			UUID txnHandle = null;
			if (jsonRequest.has("txn-handle")) {
				String txnH = jsonRequest.getString("txn-handle");
				if (!txnH.isBlank()) {
					try {
						txnHandle = UUID.fromString(txnH);
					} catch (Exception e) {
						return createJsonResponse(false, "Invalid txn-handle specified : " + txnH + " : " + e.getMessage(), null);
					}
				} 
			}

			String sql;
			if (jsonRequest.has("sql")) {
				sql = jsonRequest.getString("sql");
				if (sql.isBlank()) {
					return createJsonResponse(false, "sql must be specified : ", null);
				} 
			} else {
				return createJsonResponse(false, "sql must be specified : ", null);
			}

			//
			//initialize if the command type is initialize or the device has not been initialized yet after last server restart.
			//

			String sqlToCheck = sql.split(";")[0].strip().toLowerCase();
			switch (sqlToCheck) {
			case "initialize":
				try {
					db.init();
					DB.addDatabase(db);					
					return createJsonResponse(true, "Database initialized successfully", null);
				} catch (Exception e) {
					return createJsonResponse(false, "Failed to initialize database : " + db + " : " + e.getMessage(), null);
				}

			case "close":
				try {
					db.close();
					DB.removeDatabase(db);					
					return createJsonResponse(true, "Database closed successfully", null);
				} catch (Exception e) {
					return createJsonResponse(false, "Failed to initialize database : " + db + " : " + e.getMessage(), null);
				}

			case "begin" :
				try {
					// Open a connection to the specified SyncLite device
					Properties props = new Properties();
					props.put("config", db.getSyncLiteLoggerConfig());
					props.put("device-name", db.getName());
					txnHandle = db.createConnectionForTxn();
					return createJsonResponseForTxnBegin(true, "Transaction started succcessfully", txnHandle.toString());
				} catch (Exception e) {
					return createJsonResponse(false, "Failed to begin transaction on database : " + db + " : " + e.getMessage(), null);
				}

			case "commit":
				try {
					if (txnHandle != null) {
						db.commitConnectionForTxn(txnHandle);
						return createJsonResponse(true, "Transaction committed successfully: ", null);
					} else {
						return createJsonResponse(false, "txn-handle must be specified : ", null);
					}
				} catch (Exception e) {
					return createJsonResponse(false, "Failed to commit transaction on database : " + db + " : " + e.getMessage(), null);
				}

			case "rollback":	
				try {
					if (txnHandle != null) {
						db.rollbackConnectionForTxn(txnHandle);
						return createJsonResponse(true, "Transaction rolled back successfully: ", null);
					} else {
						return createJsonResponse(false, "txn-handle must be specified : ", null);
					}
				} catch (Exception e) {
					return createJsonResponse(false, "Failed to rollback transaction on database : " + db + " : " + e.getMessage(), null);
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

			//
			//If txn-handle is specified then we need to execute the specified sql using open connection 
			//for the respective transaction.
			//
			if (txnHandle != null) {
				try (DBConnection dbConn = db.getConnectionForTxn(txnHandle)) {
					if (dbConn == null) {
						return createJsonResponse(false, "Connection closed for specified txn-handle : " + txnHandle , null);
					}
					try {
						return executeSql(dbConn.getConnection(), sql, argumentSets);
					} catch (Exception e) {
						return createJsonResponse(false, "Database error: " + e.getMessage(), null);
					}
				}
			} else {
				// Open a connection to the specified SyncLite device
				Properties props = new Properties();
				props.put("config", db.getSyncLiteLoggerConfig());
				props.put("device-name", db.getName());
				try (Connection conn = DriverManager.getConnection(db.getURL(), props)) {
					return executeSql(conn, sql, argumentSets);
				} catch (SQLException e) {
					return createJsonResponse(false, "Database error: " + e.getMessage(), null);
				}
			}
		} catch (Exception e) {
			globalTracer.debug("Error : " + e.getMessage(), e);
			return createJsonResponse(false, "Failed to process request : " + e.getMessage(), null);
		}
	}


	private static String executeSql(Connection conn, String sql, List<List<Object>> argumentSets) throws SQLException {
		boolean isQuery = sql.trim().toUpperCase().startsWith("SELECT");
		if (isQuery) {
			return executeQuery(conn, sql, argumentSets);
		} else if (argumentSets.size() > 0) {
			return executeBatch(conn, sql, argumentSets);
		} else {
			return executeUpdate(conn, sql);
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

				return createJsonResponse(true, results.size() + " rows", results);
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
				JSONObject countObj = new JSONObject();
				countObj.put("count", count);
				updateCountsJsonArray.put(countObj);
			}
			return createJsonResponse(true, "Batch executed successfully, rows affected: " + updateCounts.length, updateCountsJsonArray);
		}
	}

	private static String executeUpdate(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			int rowsAffected = stmt.executeUpdate(sql);
			return createJsonResponse(true, "Update executed successfully, rows affected: " + rowsAffected, null);
		}
	}

	private static String createJsonResponse(Boolean result, String message, Object resultSet) {
		JSONObject jsonResponse = new JSONObject();
		try {
			jsonResponse.put("result", result);
			jsonResponse.put("message", message);
			if (resultSet != null) {
				jsonResponse.put("resultset", resultSet);
			}
		} catch (JSONException e) {
			jsonResponse.put("result", false);
			jsonResponse.put("message", "Error creating JSON response : " + e.getMessage());
		}
		return jsonResponse.toString();
	}

	private static String createJsonResponseForTxnBegin(Boolean result, String message, String txnHandle) {
		JSONObject jsonResponse = new JSONObject();
		try {
			jsonResponse.put("result", result);
			jsonResponse.put("message", message);
			jsonResponse.put("txn-handle", txnHandle);
		} catch (JSONException e) {
			jsonResponse.put("result", false);
			jsonResponse.put("message", "Error creating JSON response");
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

		confBuilder.append("#==============SyncLiteDB Configurations==================");
		confBuilder.append(newLine);
		confBuilder.append("port=").append("5555");
		confBuilder.append(newLine);
		confBuilder.append("num-threads=4");
		confBuilder.append(newLine);
		confBuilder.append("idle-connection-timeout-ms=30000");
		confBuilder.append(newLine);
		confBuilder.append("trace-level=INFO");
		String confStr = confBuilder.toString();

		try {
			Files.writeString(dbConfigFilePath, confStr);
		} catch (IOException e) {
			throw new SQLException("Failed to create a default SyncLiteDB configuration file : " + dbConfigFilePath, e);
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
		if (isWindows()) {
			System.out.println("Usage:"); 
			System.out.println();
			System.out.println("synclite-db.bat");
			System.out.println();
			System.out.println("synclite-db.bat --config <path/to/config-file>");
		} else {
			System.out.println("Usage:"); 
			System.out.println();
			System.out.println("synclite-db.sh");
			System.out.println();
			System.out.println("synclite-db.sh--config <path/to/config-file>");
		}
		System.exit(1);
	}

	private final static boolean isWindows() {
		String osName = System.getProperty("os.name").toLowerCase();
		if (osName.contains("win")) {
			return true;
		} else {
			return false;
		}
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
		builder.append("===================SyncLiteDB " + version + "==========================");
		builder.append("\n");
		builder.append("Starting with configuration");
		builder.append("\n");
		builder.append("port : " + ConfLoader.getInstance().getPort());
		builder.append("\n");
		builder.append("num-threads : " + ConfLoader.getInstance().getNumThreads());
		builder.append("\n");
		builder.append("idle-connection-timeout-ms : " + ConfLoader.getInstance().getIdleConnectionTimeout());
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
