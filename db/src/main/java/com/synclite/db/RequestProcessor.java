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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONObject;

import com.synclite.db.DB.DBConnection;

import io.synclite.logger.DeviceType;

final class RequestProcessor {

	private RequestProcessor() {
	}

	static String processRequest(String request, String requesterPrincipal, String requesterAppId) {
		try {
			JSONObject jsonRequest = new JSONObject(request);
			String protocolVersion = Main.DEFAULT_PROTOCOL_VERSION;
			if (jsonRequest.has("protocol-version")) {
				protocolVersion = String.valueOf(jsonRequest.get("protocol-version")).trim();
				if (!Main.isSupportedProtocolVersion(protocolVersion)) {
					return Main.createJsonResponse(false, "Unsupported protocol-version: " + protocolVersion, null, "ERR_UNSUPPORTED_PROTOCOL_VERSION", Main.DEFAULT_PROTOCOL_VERSION);
				}
			}

			UUID txnHandle = null;
			if (jsonRequest.has("txn-handle")) {
				String txnH = jsonRequest.getString("txn-handle");
				if (!txnH.isBlank()) {
					try {
						txnHandle = UUID.fromString(txnH);
					} catch (Exception e) {
						return Main.createJsonResponse(false, "Invalid txn-handle specified : " + txnH + " : " + e.getMessage(), null, "ERR_INVALID_REQUEST", protocolVersion);
					}
				}
			}

			String requestType = null;
			if (jsonRequest.has("request-type")) {
				requestType = String.valueOf(jsonRequest.get("request-type")).trim().toLowerCase();
			}

			String sql = null;
			if (jsonRequest.has("sql")) {
				sql = jsonRequest.getString("sql");
				if (sql != null) {
					sql = sql.trim();
				}
			}

			String sqlToCheck;
			if (requestType != null && !requestType.isEmpty()) {
				sqlToCheck = requestType;
			} else {
				if (sql == null || sql.isBlank()) {
					return Main.createJsonResponse(false, "sql must be specified : ", null, "ERR_INVALID_REQUEST", protocolVersion);
				}
				sqlToCheck = sql.split(";")[0].strip().toLowerCase();
			}
			String operation = "execute";
			switch (sqlToCheck) {
			case "initialize":
			case "close":
			case "begin":
			case "commit":
			case "rollback":
			case "next":
				operation = sqlToCheck;
				break;
			default:
				if (sql != null && sql.trim().toUpperCase().startsWith("SELECT")) {
					operation = "select";
				}
			}

			if (ConfLoader.getInstance().isAppAuthEnabled() && !ConfLoader.getInstance().isOperationAllowed(requesterAppId, operation)) {
				return Main.createJsonResponse(false, "Operation not allowed for app-id: " + requesterAppId + " : " + operation, null, "ERR_FORBIDDEN_OPERATION", protocolVersion);
			}

			int paginationSize;
			try {
				paginationSize = parseResultsetPaginationSize(jsonRequest, ConfLoader.getInstance().getResultsetPaginationSize()).intValue();
			} catch (Exception e) {
				return Main.createJsonResponse(false, "Invalid resultset-pagination-size: " + e.getMessage(), null, "ERR_INVALID_REQUEST", protocolVersion);
			}

			String dataFormat;
			try {
				dataFormat = parseResultsetDataFormat(jsonRequest);
			} catch (Exception e) {
				return Main.createJsonResponse(false, "Invalid resultset-data-format: " + e.getMessage(), null, "ERR_INVALID_REQUEST", protocolVersion);
			}

			boolean includeMetadata = parseResultsetIncludeMetadata(jsonRequest);

			if ("next".equals(sqlToCheck)) {
				return fetchNextResultSetPage(jsonRequest, requesterPrincipal, paginationSize, protocolVersion, dataFormat, includeMetadata);
			}

			if (sql == null || sql.isBlank()) {
				return Main.createJsonResponse(false, "sql must be specified : ", null, "ERR_INVALID_REQUEST", protocolVersion);
			}

			DB db = null;
			Path dbPath = null;
			DeviceType dbType = null;
			String dbName = null;
			Path dbSyncLiteLoggerConfig = null;

			if (jsonRequest.has("db-path")) {
				dbPath = Path.of(jsonRequest.getString("db-path"));
				if (!Files.exists(dbPath.getParent())) {
					return Main.createJsonResponse(false, "Parent directory of specified db-path : " + dbPath + " does not exist", null, "ERR_INVALID_DB_PATH", protocolVersion);
				}
				db = DB.getDatabase(dbPath);
			} else {
				return Main.createJsonResponse(false, "db-path must be specified : ", null, "ERR_INVALID_REQUEST", protocolVersion);
			}

			if (db == null) {
				if (jsonRequest.has("db-type")) {
					String type = jsonRequest.getString("db-type");
					if (type.isBlank()) {
						return Main.createJsonResponse(false, "db-type must be specified : ", null, "ERR_INVALID_REQUEST", protocolVersion);
					} else {
						try {
							dbType = DeviceType.valueOf(type);
						} catch (Exception e) {
							return Main.createJsonResponse(false, "Invalid db-type : " + type + " specified", null, "ERR_INVALID_REQUEST", protocolVersion);
						}
					}
				} else {
					return Main.createJsonResponse(false, "db-type must be specified : ", null, "ERR_INVALID_REQUEST", protocolVersion);
				}

				if (jsonRequest.has("db-name")) {
					dbName = jsonRequest.getString("db-name");
				}

				dbSyncLiteLoggerConfig = Main.getDbDir().resolve("synclite_logger.conf");
				if (jsonRequest.has("synclite-logger-config")) {
					String config = jsonRequest.getString("synclite-logger-config");
					if (config.isBlank()) {
						return Main.createJsonResponse(false, "Empty synclite-logger-config specified", null, "ERR_INVALID_REQUEST", protocolVersion);
					}
					dbSyncLiteLoggerConfig = Path.of(config);
				} else {
					dbSyncLiteLoggerConfig = Main.getDbDir().resolve("synclite_logger.conf");
				}

				if (!Files.exists(dbSyncLiteLoggerConfig)) {
					return Main.createJsonResponse(false, "Specified synclite-logger-config does not exist", null, "ERR_INVALID_REQUEST", protocolVersion);
				}

				db = new DB(dbName, dbType, dbPath, dbSyncLiteLoggerConfig);
			}

			switch (sqlToCheck) {
			case "initialize":
				try {
					db.init();
					DB.addDatabase(db);
					return Main.createJsonResponse(true, "Database initialized successfully", null, "OK", protocolVersion);
				} catch (Exception e) {
					Main.globalTracer.error("Error: Failed to initialize database : " + db + " : " + e.getMessage(), e);
					return Main.createJsonResponse(false, "Failed to initialize database : " + db + " : " + e.getMessage(), null, "ERR_DATABASE", protocolVersion);
				}

			case "close":
				try {
					db.close();
					DB.removeDatabase(db);
					return Main.createJsonResponse(true, "Database closed successfully", null, "OK", protocolVersion);
				} catch (Exception e) {
					Main.globalTracer.debug("Failed to close database : " + db + " : " + e.getMessage());
					return Main.createJsonResponse(false, "Failed to close database : " + db + " : " + e.getMessage(), null, "ERR_DATABASE", protocolVersion);
				}

			case "begin":
				try {
					txnHandle = db.createConnectionForTxn(requesterPrincipal);
					return Main.createJsonResponseForTxnBegin(true, "Transaction started succcessfully", txnHandle.toString(), "OK", protocolVersion);
				} catch (Exception e) {
					Main.globalTracer.debug("Failed to begin transaction on database : " + db + " : " + e.getMessage());
					return Main.createJsonResponse(false, "Failed to begin transaction on database : " + db + " : " + e.getMessage(), null, "ERR_DATABASE", protocolVersion);
				}

			case "commit":
				try {
					if (txnHandle != null) {
						db.commitConnectionForTxn(txnHandle, requesterPrincipal);
						return Main.createJsonResponse(true, "Transaction committed successfully: ", null, "OK", protocolVersion);
					}
					return Main.createJsonResponse(false, "txn-handle must be specified : ", null, "ERR_INVALID_REQUEST", protocolVersion);
				} catch (Exception e) {
					Main.globalTracer.debug("Failed to commit transaction on database : " + db + " : " + e.getMessage());
					if (e.getMessage() != null && e.getMessage().contains("not owned by requester")) {
						return Main.createJsonResponse(false, "Failed to commit transaction on database : " + db + " : " + e.getMessage(), null, "ERR_TXN_OWNERSHIP", protocolVersion);
					}
					return Main.createJsonResponse(false, "Failed to commit transaction on database : " + db + " : " + e.getMessage(), null, "ERR_DATABASE", protocolVersion);
				}

			case "rollback":
				try {
					if (txnHandle != null) {
						db.rollbackConnectionForTxn(txnHandle, requesterPrincipal);
						return Main.createJsonResponse(true, "Transaction rolled back successfully: ", null, "OK", protocolVersion);
					}
					return Main.createJsonResponse(false, "txn-handle must be specified : ", null, "ERR_INVALID_REQUEST", protocolVersion);
				} catch (Exception e) {
					Main.globalTracer.debug("Failed to rollback transaction on database : " + db + " : " + e.getMessage());
					if (e.getMessage() != null && e.getMessage().contains("not owned by requester")) {
						return Main.createJsonResponse(false, "Failed to rollback transaction on database : " + db + " : " + e.getMessage(), null, "ERR_TXN_OWNERSHIP", protocolVersion);
					}
					return Main.createJsonResponse(false, "Failed to rollback transaction on database : " + db + " : " + e.getMessage(), null, "ERR_DATABASE", protocolVersion);
				}
			}

			List<List<Object>> argumentSets = new ArrayList<List<Object>>();
			if (jsonRequest.has("arguments")) {
				JSONArray argumentSetsArray = jsonRequest.getJSONArray("arguments");
				for (int j = 0; j < argumentSetsArray.length(); j++) {
					JSONArray argumentsArray = argumentSetsArray.getJSONArray(j);
					List<Object> arguments = new ArrayList<Object>();
					for (int i = 0; i < argumentsArray.length(); i++) {
						arguments.add(argumentsArray.get(i));
					}
					argumentSets.add(arguments);
				}
			}

			if (txnHandle != null) {
				try (DBConnection dbConn = db.getConnectionForTxn(txnHandle, requesterPrincipal)) {
					if (dbConn == null) {
						return Main.createJsonResponse(false, "Connection closed for specified txn-handle : " + txnHandle, null, "ERR_INVALID_REQUEST", protocolVersion);
					}
					try {
					return executeSql(dbConn.getConnection(), db, sql, argumentSets, requesterPrincipal, paginationSize, protocolVersion, dataFormat, includeMetadata);
					} catch (Exception e) {
						return Main.createJsonResponse(false, "Database error: " + e.getMessage(), null, "ERR_DATABASE", protocolVersion);
					}
				} catch (SQLException e) {
					if (e.getMessage() != null && e.getMessage().contains("not owned by requester")) {
						return Main.createJsonResponse(false, "Database error: " + e.getMessage(), null, "ERR_TXN_OWNERSHIP", protocolVersion);
					}
					return Main.createJsonResponse(false, "Database error: " + e.getMessage(), null, "ERR_DATABASE", protocolVersion);
				}
			}

			Properties props = new Properties();
			props.put("config", db.getSyncLiteLoggerConfig());
			props.put("device-name", db.getName());
			Connection conn = null;
			try {
				conn = DriverManager.getConnection(db.getURL(), props);
				String response = executeSql(conn, db, sql, argumentSets, requesterPrincipal, paginationSize, protocolVersion, dataFormat, includeMetadata);
				if (!db.isResultsetConnectionRetained(conn)) {
					conn.close();
				}
				return response;
			} catch (SQLException e) {
				if (conn != null) {
					try {
						if (!db.isResultsetConnectionRetained(conn)) {
							conn.close();
						}
					} catch (Exception ignore) {
					}
				}
				return Main.createJsonResponse(false, "Database error: " + e.getMessage(), null, "ERR_DATABASE", protocolVersion);
			}
		} catch (Exception e) {
			Main.globalTracer.error("Error : " + e.getMessage(), e);
			return Main.createJsonResponse(false, "Failed to process request : " + e.getMessage(), null, "ERR_REQUEST_PROCESSING");
		}
	}

	private static String executeSql(Connection conn, DB db, String sql, List<List<Object>> argumentSets, String requesterPrincipal, int paginationSize, String protocolVersion, String dataFormat, boolean includeMetadata) throws SQLException {
		boolean isQuery = sql.trim().toUpperCase().startsWith("SELECT");
		if (isQuery) {
			return executeQuery(conn, db, sql, argumentSets, requesterPrincipal, paginationSize, protocolVersion, dataFormat, includeMetadata);
		}
		if (argumentSets.size() > 0) {
			return executeBatch(conn, sql, argumentSets);
		}
		return executeUpdate(conn, sql);
	}

	private static String executeQuery(Connection conn, DB db, String sql, List<List<Object>> argumentSets, String requesterPrincipal, int paginationSize, String protocolVersion, String dataFormat, boolean includeMetadata) throws SQLException {
		Statement stmt = null;
		ResultSet rs = null;
		try {
			if (argumentSets.isEmpty()) {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
			} else {
				PreparedStatement pstmt = conn.prepareStatement(sql);
				List<Object> arguments = argumentSets.get(0);
				for (int i = 0; i < arguments.size(); i++) {
					pstmt.setObject(i + 1, arguments.get(i));
				}
				stmt = pstmt;
				rs = pstmt.executeQuery();
			}

			DB.ResultSetPage page = db.createPagedResultSetPage(conn, stmt, rs, requesterPrincipal, paginationSize);
			JSONArray resultsetJson = formatRows(page.rows, dataFormat);
			JSONArray metadataJson = includeMetadata ? page.columnMetadata : null;
			return Main.createJsonResponseWithHandle(true, page.rows.size() + " rows", resultsetJson, "OK", protocolVersion, page.resultsetHandle, Boolean.valueOf(page.hasMore), metadataJson);
		} catch (SQLException e) {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception ignore) {
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception ignore) {
				}
			}
			throw e;
		}
	}

	private static String fetchNextResultSetPage(JSONObject jsonRequest, String requesterPrincipal, int requestedPaginationSize, String protocolVersion, String dataFormat, boolean includeMetadata) {
		if (!jsonRequest.has("resultset-handle")) {
			return Main.createJsonResponse(false, "resultset-handle must be specified for next", null, "ERR_INVALID_REQUEST", protocolVersion);
		}

		UUID handle;
		try {
			handle = UUID.fromString(jsonRequest.getString("resultset-handle"));
		} catch (Exception e) {
			return Main.createJsonResponse(false, "Invalid resultset-handle specified", null, "ERR_INVALID_REQUEST", protocolVersion);
		}
		try {
			DB.ResultSetPage page = DB.fetchNextResultSetPage(handle, requesterPrincipal, requestedPaginationSize);
			JSONArray resultsetJson = formatRows(page.rows, dataFormat);
			JSONArray metadataJson = includeMetadata ? page.columnMetadata : null;
			return Main.createJsonResponseWithHandle(true, page.rows.size() + " rows", resultsetJson, "OK", protocolVersion, page.resultsetHandle, Boolean.valueOf(page.hasMore), metadataJson);
		} catch (SQLException e) {
			if (e.getMessage() != null && e.getMessage().contains("not owned by requester")) {
				return Main.createJsonResponse(false, "resultset-handle is not owned by requester", null, "ERR_RESULTSET_OWNERSHIP", protocolVersion);
			}
			if (e.getMessage() != null && e.getMessage().contains("No open resultset found")) {
				return Main.createJsonResponse(false, "No open resultset found for resultset-handle : " + handle, null, "ERR_INVALID_RESULTSET_HANDLE", protocolVersion);
			}
			return Main.createJsonResponse(false, "Failed to fetch next page : " + e.getMessage(), null, "ERR_DATABASE", protocolVersion);
		}
	}

	private static String executeBatch(Connection conn, String sql, List<List<Object>> argumentSets) throws SQLException {
		try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
			for (List<Object> arguments : argumentSets) {
				for (int i = 0; i < arguments.size(); i++) {
					pstmt.setObject(i + 1, arguments.get(i));
				}
				pstmt.addBatch();
			}

			int[] updateCounts = pstmt.executeBatch();
			JSONArray updateCountsJsonArray = new JSONArray();
			for (int count : updateCounts) {
				JSONObject countObj = new JSONObject();
				countObj.put("count", count);
				updateCountsJsonArray.put(countObj);
			}
			return Main.createJsonResponse(true, "Batch executed successfully, rows affected: " + updateCounts.length, updateCountsJsonArray);
		}
	}

	private static String executeUpdate(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			int rowsAffected = stmt.executeUpdate(sql);
			return Main.createJsonResponse(true, "Update executed successfully, rows affected: " + rowsAffected, null);
		}
	}

	private static Integer parseResultsetPaginationSize(JSONObject jsonRequest, int defaultPageSize) {
		if (jsonRequest == null || !jsonRequest.has("resultset-pagination-size")) {
			return Integer.valueOf(defaultPageSize);
		}
		Object raw = jsonRequest.get("resultset-pagination-size");
		int pageSize;
		if (raw instanceof Number) {
			pageSize = ((Number) raw).intValue();
		} else {
			pageSize = Integer.parseInt(String.valueOf(raw).trim());
		}
		if (pageSize <= 0) {
			throw new IllegalArgumentException("resultset-pagination-size must be a positive integer");
		}
		return Integer.valueOf(pageSize);
	}

	private static String parseResultsetDataFormat(JSONObject jsonRequest) {
		if (jsonRequest == null || !jsonRequest.has("resultset-data-format")) {
			return "JSON";
		}
		String fmt = String.valueOf(jsonRequest.get("resultset-data-format")).trim().toUpperCase();
		if (!"JSON".equals(fmt) && !"DB".equals(fmt)) {
			throw new IllegalArgumentException("resultset-data-format must be JSON or DB");
		}
		return fmt;
	}

	private static boolean parseResultsetIncludeMetadata(JSONObject jsonRequest) {
		if (jsonRequest == null || !jsonRequest.has("resultset-include-metadata")) {
			return true;
		}
		String val = String.valueOf(jsonRequest.get("resultset-include-metadata")).trim().toUpperCase();
		return !"OFF".equals(val);
	}

	private static JSONArray formatRows(List<Map<String, Object>> rows, String dataFormat) {
		JSONArray resultsetJson = new JSONArray();
		for (Map<String, Object> row : rows) {
			if ("DB".equals(dataFormat)) {
				JSONArray rowArray = new JSONArray();
				for (Object val : row.values()) {
					rowArray.put(val != null ? val : JSONObject.NULL);
				}
				resultsetJson.put(rowArray);
			} else {
				JSONObject rowObj = new JSONObject();
				for (Map.Entry<String, Object> entry : row.entrySet()) {
					rowObj.put(entry.getKey(), entry.getValue() != null ? entry.getValue() : JSONObject.NULL);
				}
				resultsetJson.put(rowObj);
			}
		}
		return resultsetJson;
	}
}
