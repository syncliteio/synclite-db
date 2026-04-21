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
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.synclite.db.DB.DBConnection;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

import io.synclite.logger.*;

public class Main {

	static Path dbDir;
	static Path stageDir;
	static Path dbConfigFilePath = null;
	public static Logger globalTracer;
	static final String DEFAULT_PROTOCOL_VERSION = "1";
	private static final String SUPPORTED_PROTOCOL_VERSION_ALIAS = "1.0";

	static {
		//Load all SyncLite DB classes here
		try {
			Class.forName("io.synclite.logger.SQLite");
			Class.forName("io.synclite.logger.SQLiteStore");
			Class.forName("io.synclite.logger.SQLiteAppender");
			Class.forName("io.synclite.logger.DuckDB");
			Class.forName("io.synclite.logger.DuckDBStore");
			Class.forName("io.synclite.logger.DuckDBAppender");
			Class.forName("io.synclite.logger.H2");
			Class.forName("io.synclite.logger.H2Store");
			Class.forName("io.synclite.logger.H2Appender");
			Class.forName("io.synclite.logger.Derby");
			Class.forName("io.synclite.logger.DerbyStore");
			Class.forName("io.synclite.logger.DerbyAppender");
			Class.forName("io.synclite.logger.HyperSQL");
			Class.forName("io.synclite.logger.HyperSQLStore");
			Class.forName("io.synclite.logger.HyperSQLAppender");
			Class.forName("io.synclite.logger.Streaming");
		} catch (Exception e) {
			throw new RuntimeException("Failed to load SyncLite Logger classes : " + e.getMessage(), e);
		}
	}


	static boolean isSupportedProtocolVersion(String protocolVersion) {
		if (protocolVersion == null) {
			return false;
		}

		String pv = protocolVersion.trim();
		return DEFAULT_PROTOCOL_VERSION.equals(pv) || SUPPORTED_PROTOCOL_VERSION_ALIAS.equals(pv);
	}

	static int mapErrorCodeToStatus(String errorCode) {
		if (errorCode == null) {
			return HttpURLConnection.HTTP_BAD_REQUEST;
		}

		switch (errorCode) {
		case "ERR_UNAUTHORIZED":
		case "ERR_UNAUTHORIZED_APP":
			return HttpURLConnection.HTTP_UNAUTHORIZED;
		case "ERR_FORBIDDEN_OPERATION":
		case "ERR_TXN_OWNERSHIP":
		case "ERR_RESULTSET_OWNERSHIP":
			return HttpURLConnection.HTTP_FORBIDDEN;
		case "ERR_REQUEST_TOO_LARGE":
			return 413;
		default:
			return HttpURLConnection.HTTP_BAD_REQUEST;
		}
	}

	static String createJsonResponseWithHandle(Boolean result, String message, Object resultSet, String code, String protocolVersion, UUID resultsetHandle, Boolean hasMore) {
		return createJsonResponseWithHandle(result, message, resultSet, code, protocolVersion, resultsetHandle, hasMore, null);
	}

	static String createJsonResponseWithHandle(Boolean result, String message, Object resultSet, String code, String protocolVersion, UUID resultsetHandle, Boolean hasMore, JSONArray columnMetadata) {
		JSONObject jsonResponse = new JSONObject();
		try {
			jsonResponse.put("result", result);
			jsonResponse.put("message", message);
			jsonResponse.put("code", code);
			jsonResponse.put("protocol-version", protocolVersion);
			if (resultSet != null) {
				jsonResponse.put("resultset", resultSet);
			}
			if (resultsetHandle != null) {
				jsonResponse.put("resultset-handle", resultsetHandle.toString());
			}
			if (hasMore != null) {
				jsonResponse.put("has-more", hasMore.booleanValue());
			}
			if (columnMetadata != null) {
				jsonResponse.put("resultset-metadata", columnMetadata);
			}
		} catch (JSONException e) {
			jsonResponse = new JSONObject();
			jsonResponse.put("result", false);
			jsonResponse.put("message", "Error creating JSON response : " + e.getMessage());
			jsonResponse.put("code", "ERR_RESPONSE_SERIALIZATION");
			jsonResponse.put("protocol-version", DEFAULT_PROTOCOL_VERSION);
		}
		if (globalTracer != null && globalTracer.isDebugEnabled()) {
			globalTracer.debug("Response : " + jsonResponse.toString());
		}
		return jsonResponse.toString();
	}

	static Path getDbDir() {
		return dbDir;
	}

	static String sanitizeLogValue(Object value) {
		if (value == null) {
			return "null";
		}
		String str = String.valueOf(value);
		str = str.replace('\n', ' ').replace('\r', ' ').trim();
		if (str.length() > 512) {
			return str.substring(0, 512) + "...";
		}
		return str;
	}

	static String kv(String key, Object value) {
		return key + "=" + sanitizeLogValue(value);
	}

	static String structuredLog(String event, String... entries) {
		StringBuilder builder = new StringBuilder();
		builder.append(kv("event", event));
		if (entries != null) {
			for (String entry : entries) {
				if (entry == null || entry.isEmpty()) {
					continue;
				}
				builder.append(" ").append(entry);
			}
		}
		return builder.toString();
	}

	public static void main(String[] args) {

		//Validate args
		if (args.length > 0) {
			if (args.length == 2) {
				if (!args[0].trim().equals("--config")) {
					ServerRuntime.usage();
				} else {
					dbConfigFilePath = Path.of(args[1]);
					if (!Files.exists(dbConfigFilePath)) {
						ServerRuntime.error(new Exception("Invalid configuration file specified : " + dbConfigFilePath));
					}
				}
			} else {
				ServerRuntime.usage();
			}
		}

		ServerRuntime.initDB();
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup(ConfLoader.getInstance().getNumThreads());
		try {
			ServerBootstrap bootstrap = new ServerBootstrap();
			bootstrap.group(bossGroup, workerGroup)
				.channel(NioServerSocketChannel.class)
				.childOption(ChannelOption.TCP_NODELAY, true)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) {
						ChannelPipeline p = ch.pipeline();
						p.addLast(new HttpServerCodec());
						p.addLast(new HttpObjectAggregator((int) Math.min(Integer.MAX_VALUE, ConfLoader.getInstance().getMaxRequestSizeBytes())));
						p.addLast(new NettyHttpHandler());
					}
				});

			ChannelFuture bindFuture = bootstrap.bind(ConfLoader.getInstance().getBindAddress(), ConfLoader.getInstance().getPort()).sync();
			globalTracer.info(structuredLog("server_started",
				kv("bind-address", ConfLoader.getInstance().getBindAddress()),
				kv("port", ConfLoader.getInstance().getPort())
			));

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				try {
					SyncLite.closeAllDatabases();
				} catch (SQLException e) {
					if (globalTracer != null) {
						globalTracer.error(structuredLog("shutdown_db_close_error",
							kv("error", e.getMessage())
						), e);
					}
				}
				DB.closeAllOpenResultSets();
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();
				System.out.println("SyncLiteDB Server is shutting down.");
				globalTracer.info(structuredLog("server_shutting_down"));
			}));

			bindFuture.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			globalTracer.error(structuredLog("server_interrupted",
				kv("phase", "netty_lifecycle_wait")
			), e);
		} catch (Exception e) {
			globalTracer.error(structuredLog("server_start_failure",
				kv("error", e.getMessage())
			), e);
		} finally {
			DB.closeAllOpenResultSets();
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
			if (globalTracer != null) {
				globalTracer.info(structuredLog("server_eventloop_shutdown_initiated"));
			}
		}
	}


	static String processRequest(String request, String requesterPrincipal, String requesterAppId) {
		return RequestProcessor.processRequest(request, requesterPrincipal, requesterAppId);
	}

	static String createJsonResponse(Boolean result, String message, Object resultSet) {
		String code = result ? "OK" : "ERR_GENERIC";
		return createJsonResponse(result, message, resultSet, code, DEFAULT_PROTOCOL_VERSION);
	}

	static String createJsonResponse(Boolean result, String message, Object resultSet, String code) {
		return createJsonResponse(result, message, resultSet, code, DEFAULT_PROTOCOL_VERSION);
	}

	static String createJsonResponse(Boolean result, String message, Object resultSet, String code, String protocolVersion) {
		JSONObject jsonResponse = new JSONObject();
		try {
			jsonResponse.put("result", result);
			jsonResponse.put("message", message);
			jsonResponse.put("code", code);
			jsonResponse.put("protocol-version", protocolVersion);
			if (resultSet != null) {
				jsonResponse.put("resultset", resultSet);
			}
		} catch (JSONException e) {
			jsonResponse.put("result", false);
			jsonResponse.put("message", "Error creating JSON response : " + e.getMessage());
			jsonResponse.put("code", "ERR_RESPONSE_SERIALIZATION");
			jsonResponse.put("protocol-version", DEFAULT_PROTOCOL_VERSION);
		}
		globalTracer.debug("Response : " + jsonResponse.toString());
		return jsonResponse.toString();
	}

	private static String createJsonResponseForTxnBegin(Boolean result, String message, String txnHandle) {
		String code = result ? "OK" : "ERR_GENERIC";
		return createJsonResponseForTxnBegin(result, message, txnHandle, code, DEFAULT_PROTOCOL_VERSION);
	}

	static String createJsonResponseForTxnBegin(Boolean result, String message, String txnHandle, String code, String protocolVersion) {
		JSONObject jsonResponse = new JSONObject();
		try {
			jsonResponse.put("result", result);
			jsonResponse.put("message", message);
			jsonResponse.put("code", code);
			jsonResponse.put("protocol-version", protocolVersion);
			jsonResponse.put("txn-handle", txnHandle);
		} catch (JSONException e) {
			jsonResponse.put("result", false);
			jsonResponse.put("message", "Error creating JSON response");
			jsonResponse.put("code", "ERR_RESPONSE_SERIALIZATION");
			jsonResponse.put("protocol-version", DEFAULT_PROTOCOL_VERSION);
		}
		globalTracer.debug("Response : " + jsonResponse.toString());
		return jsonResponse.toString();
	}

}
