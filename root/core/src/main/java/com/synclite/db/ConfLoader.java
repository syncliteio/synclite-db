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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;

public class ConfLoader {

	private Integer numThreads;
	private Integer port;
	private Level traceLevel;
	private Long idleConnectionTimeout;
	private String bindAddress;
	private Long maxRequestSizeBytes;
	private int resultsetPaginationSize;
	private long resultsetHandleTimeoutMs;
	private String authToken;
	private boolean appAuthEnabled;
	private long appAuthTimestampSkewMs;
	private long appAuthNonceTtlMs;
	private int appAuthNonceCacheMaxEntries;
	private HashMap<String, String> authorizedAppSecrets;
	private HashMap<String, Set<String>> authorizedAppAllowedOps;

	private static final Set<String> SUPPORTED_OPERATIONS = new HashSet<String>(
		Arrays.asList("initialize", "close", "begin", "commit", "rollback", "select", "execute", "next")
	);

	public int getNumThreads() {
		return numThreads;
	}

	public int getPort() {
		return port;
	}

	public Level getTraceLevel() {
		return traceLevel;
	}
	
	public long getIdleConnectionTimeout() {
		return idleConnectionTimeout;
	}

	public String getBindAddress() {
		return bindAddress;
	}

	public long getMaxRequestSizeBytes() {
		return maxRequestSizeBytes;
	}

	public int getResultsetPaginationSize() {
		return resultsetPaginationSize;
	}

	public long getResultsetHandleTimeoutMs() {
		return resultsetHandleTimeoutMs;
	}

	public String getAuthToken() {
		if (authToken == null) {
			return "";
		}
		return authToken;
	}

	public boolean isAppAuthEnabled() {
		return appAuthEnabled;
	}

	public long getAppAuthTimestampSkewMs() {
		return appAuthTimestampSkewMs;
	}

	public long getAppAuthNonceTtlMs() {
		return appAuthNonceTtlMs;
	}

	public int getAppAuthNonceCacheMaxEntries() {
		return appAuthNonceCacheMaxEntries;
	}

	public String getAppSecret(String appId) {
		if (appId == null || authorizedAppSecrets == null) {
			return null;
		}
		return authorizedAppSecrets.get(appId.toLowerCase());
	}

	public int getAuthorizedAppCount() {
		if (authorizedAppSecrets == null) {
			return 0;
		}
		return authorizedAppSecrets.size();
	}

	public boolean isOperationAllowed(String appId, String operation) {
		if (!appAuthEnabled) {
			return true;
		}

		if (appId == null || operation == null) {
			return false;
		}

		Set<String> allowedOps = authorizedAppAllowedOps.get(appId.toLowerCase());
		if (allowedOps == null || allowedOps.isEmpty()) {
			return false;
		}

		return allowedOps.contains(operation.toLowerCase());
	}
	
	private static final class InstanceHolder {
		private static ConfLoader INSTANCE = new ConfLoader();
	}

	public static ConfLoader getInstance() {
		return InstanceHolder.INSTANCE;
	}

	private HashMap<String, String> properties;

	private ConfLoader() {

	}

	public void loadDBConfigProperties(Path propsPath) throws SyncLiteException {
		this.properties = loadPropertiesFromFile(propsPath);
		validateAndProcessProperties();    	
	}
	
	public static HashMap<String, String> loadPropertiesFromFile(Path propsPath) throws SyncLiteException {
		BufferedReader reader = null;
		try {
			HashMap<String, String> properties = new HashMap<String, String>();
			reader = new BufferedReader(new FileReader(propsPath.toFile()));
			String line = reader.readLine();
			while (line != null) {
				line = line.trim();
				if (line.trim().isEmpty()) {
					line = reader.readLine();
					continue;
				}
				if (line.startsWith("#")) {
					line = reader.readLine();
					continue;
				}
				String[] tokens = line.split("=", 2);
				if (tokens.length != 2 || tokens[0].trim().isEmpty()) {
					throw new SyncLiteException("Invalid line in configuration file " + propsPath + " : " + line);
				}
				properties.put(tokens[0].trim().toLowerCase(), tokens[1].trim());
				line = reader.readLine();
			}
			return properties;
		} catch (IOException e) {
			throw new SyncLiteException("Failed to load configuration file : " + propsPath + " : ", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					throw new SyncLiteException("Failed to close configuration file : " + propsPath + ": " , e);
				}
			}
		}
	}

	private void validateAndProcessProperties() throws SyncLiteException {
		String propValue = properties.get("port");
		if (propValue != null) {
			try {
	            port = Integer.valueOf(propValue);
	            if (port <= 0) {
	            	throw new SyncLiteException("Please specify a valid positive numeric port in the configuration file");
	            }
			} catch (NumberFormatException e) {
				throw new SyncLiteException("Please specify a valid positive numeric port in the configuration file : " + e.getMessage(), e);
			}
		} else {
			port = 5555;
		}

		propValue = properties.get("num-threads");
		if (propValue != null) {
			try {
				this.numThreads = Integer.valueOf(propValue);
				if (this.numThreads == null) {
					throw new SyncLiteException("Invalid value specified for num-threads in configuration file");
				} else if (this.numThreads <= 0) {
					throw new SyncLiteException("Please specify a positive numeric value for num-threads in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLiteException("Please specify a positive numeric value for num-threads in configuration file");
			}
		} else {
			this.numThreads = Runtime.getRuntime().availableProcessors();
		}

		propValue = properties.get("idle-connection-timeout-ms");
		if (propValue != null) {
			try {
				this.idleConnectionTimeout = Long.valueOf(propValue);
				if (this.idleConnectionTimeout == null) {
					throw new SyncLiteException("Invalid value specified for idle-connection-timeout in configuration file");
				} else if (this.idleConnectionTimeout <= 0) {
					throw new SyncLiteException("Please specify a positive numeric value for idle-connection-timeout in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLiteException("Please specify a positive numeric value for idle-connection-timeout in configuration file");
			}
		} else {
			this.idleConnectionTimeout = 30000L;
		}

		propValue = properties.get("trace-level");
		if (propValue != null) {
			this.traceLevel= Level.toLevel(propValue, Level.INFO);
			if (this.traceLevel == null) {
				throw new SyncLiteException("Invalid value specified for trace-level in configuration file");
			}
		} else {
			traceLevel = Level.INFO;
		}

		propValue = properties.get("bind-address");
		if (propValue != null) {
			this.bindAddress = propValue.trim();
			if (this.bindAddress.isEmpty()) {
				throw new SyncLiteException("Please specify a valid non-empty bind-address in the configuration file");
			}
		} else {
			this.bindAddress = "127.0.0.1";
		}

		propValue = properties.get("max-request-size-bytes");
		if (propValue != null) {
			try {
				this.maxRequestSizeBytes = Long.valueOf(propValue);
				if (this.maxRequestSizeBytes <= 0) {
					throw new SyncLiteException("Please specify a positive numeric value for max-request-size-bytes in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLiteException("Please specify a positive numeric value for max-request-size-bytes in configuration file");
			}
		} else {
			this.maxRequestSizeBytes = 1048576L;
		}

		propValue = properties.get("resultset-pagination-size");
		if (propValue != null) {
			try {
				this.resultsetPaginationSize = Integer.parseInt(propValue.trim());
				if (this.resultsetPaginationSize <= 0) {
					throw new SyncLiteException("Please specify a positive numeric value for resultset-pagination-size in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLiteException("Please specify a positive numeric value for resultset-pagination-size in configuration file");
			}
		} else {
			this.resultsetPaginationSize = 1000;
		}

		propValue = properties.get("resultset-handle-timeout-ms");
		if (propValue != null) {
			try {
				this.resultsetHandleTimeoutMs = Long.parseLong(propValue.trim());
				if (this.resultsetHandleTimeoutMs <= 0) {
					throw new SyncLiteException("Please specify a positive numeric value for resultset-handle-timeout-ms in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLiteException("Please specify a positive numeric value for resultset-handle-timeout-ms in configuration file");
			}
		} else {
			this.resultsetHandleTimeoutMs = 300000L;
		}

		propValue = properties.get("auth-token");
		if (propValue != null) {
			this.authToken = propValue.trim();
		} else {
			this.authToken = "";
		}

		propValue = properties.get("enable-app-auth");
		if (propValue != null) {
			this.appAuthEnabled = Boolean.parseBoolean(propValue.trim());
		} else {
			this.appAuthEnabled = false;
		}

		propValue = properties.get("app-auth-timestamp-skew-ms");
		if (propValue != null) {
			try {
				this.appAuthTimestampSkewMs = Long.parseLong(propValue.trim());
				if (this.appAuthTimestampSkewMs <= 0) {
					throw new SyncLiteException("Please specify a positive numeric value for app-auth-timestamp-skew-ms in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLiteException("Please specify a positive numeric value for app-auth-timestamp-skew-ms in configuration file");
			}
		} else {
			this.appAuthTimestampSkewMs = 300000L;
		}

		propValue = properties.get("app-auth-nonce-ttl-ms");
		if (propValue != null) {
			try {
				this.appAuthNonceTtlMs = Long.parseLong(propValue.trim());
				if (this.appAuthNonceTtlMs <= 0) {
					throw new SyncLiteException("Please specify a positive numeric value for app-auth-nonce-ttl-ms in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLiteException("Please specify a positive numeric value for app-auth-nonce-ttl-ms in configuration file");
			}
		} else {
			this.appAuthNonceTtlMs = 600000L;
		}

		propValue = properties.get("app-auth-nonce-cache-max-entries");
		if (propValue != null) {
			try {
				this.appAuthNonceCacheMaxEntries = Integer.parseInt(propValue.trim());
				if (this.appAuthNonceCacheMaxEntries <= 0) {
					throw new SyncLiteException("Please specify a positive numeric value for app-auth-nonce-cache-max-entries in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLiteException("Please specify a positive numeric value for app-auth-nonce-cache-max-entries in configuration file");
			}
		} else {
			this.appAuthNonceCacheMaxEntries = 10000;
		}

		this.authorizedAppSecrets = new HashMap<String, String>();
		this.authorizedAppAllowedOps = new HashMap<String, Set<String>>();
		if (this.appAuthEnabled) {
			propValue = properties.get("authorized-apps");
			if (propValue == null || propValue.trim().isEmpty()) {
				throw new SyncLiteException("enable-app-auth is true but authorized-apps is not specified in configuration file");
			}

			String[] appIds = propValue.split(",");
			for (String rawAppId : appIds) {
				String appId = rawAppId.trim().toLowerCase();
				if (appId.isEmpty()) {
					continue;
				}
				String appSecretKey = "app." + appId + ".secret";
				String appSecret = properties.get(appSecretKey);
				if (appSecret == null || appSecret.trim().isEmpty()) {
					throw new SyncLiteException("Missing required configuration for authorized app secret: " + appSecretKey);
				}
				this.authorizedAppSecrets.put(appId, appSecret.trim());

				String appAllowedOpsKey = "app." + appId + ".allowed-ops";
				String appAllowedOpsValue = properties.get(appAllowedOpsKey);
				Set<String> appAllowedOps = new HashSet<String>();
				if (appAllowedOpsValue == null || appAllowedOpsValue.trim().isEmpty()) {
					appAllowedOps.addAll(SUPPORTED_OPERATIONS);
				} else {
					String[] operations = appAllowedOpsValue.split(",");
					for (String rawOp : operations) {
						String op = rawOp.trim().toLowerCase();
						if (op.isEmpty()) {
							continue;
						}
						if (!SUPPORTED_OPERATIONS.contains(op)) {
							throw new SyncLiteException("Invalid operation in " + appAllowedOpsKey + " : " + op);
						}
						appAllowedOps.add(op);
					}
				}

				if (appAllowedOps.isEmpty()) {
					throw new SyncLiteException("No valid operations configured for authorized app: " + appId + " in " + appAllowedOpsKey);
				}
				this.authorizedAppAllowedOps.put(appId, appAllowedOps);
			}

			if (this.authorizedAppSecrets.isEmpty()) {
				throw new SyncLiteException("enable-app-auth is true but no valid app identifiers were parsed from authorized-apps");
			}
		}
	}
}
