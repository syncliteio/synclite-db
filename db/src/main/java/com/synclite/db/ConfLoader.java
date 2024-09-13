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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;

import org.apache.log4j.Level;

public class ConfLoader {

	private Integer numThreads;
	private Integer port;
	private Level traceLevel;
	private Long idleConnectionTimeout;

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
				if (tokens.length < 2) {
					if (tokens.length == 1) {
						if (tokens[0].startsWith("=")) {
							throw new SyncLiteException("Invalid line in configuration file " + propsPath + " : " + line);
						}
					} else { 
						throw new SyncLiteException("Invalid line in configuration file " + propsPath + " : " + line);
					}
				}
				properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
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
			this.numThreads = 4;
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
	}
}
