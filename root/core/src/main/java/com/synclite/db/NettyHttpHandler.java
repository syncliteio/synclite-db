package com.synclite.db;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONObject;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;

public class NettyHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
	private static final AtomicLong REQUEST_COUNTER = new AtomicLong(0);

	private static String getRequestId(HttpRequest request) {
		String requestId = request.headers().get("X-Request-Id");
		if (requestId == null || requestId.trim().isEmpty()) {
			requestId = "req-" + REQUEST_COUNTER.incrementAndGet();
		}
		return requestId;
	}

	private static String readRequestBodyLimited(FullHttpRequest request, long maxRequestSizeBytes) throws IOException {
		int readableBytes = request.content().readableBytes();
		if (readableBytes > maxRequestSizeBytes) {
			throw new IOException("Request body exceeded configured max-request-size-bytes: " + maxRequestSizeBytes);
		}

		byte[] payload = new byte[readableBytes];
		request.content().getBytes(request.content().readerIndex(), payload);
		return new String(payload, StandardCharsets.UTF_8);
	}

	private static void writeResponse(ChannelHandlerContext ctx, HttpRequest request, HttpResponseStatus status, String contentType, String body, String requestId) {
		ByteBuf payload = Unpooled.copiedBuffer(body, StandardCharsets.UTF_8);
		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, payload);
		response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
		response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, payload.readableBytes());
		response.headers().set("X-Request-Id", requestId);

		if (Main.globalTracer != null && Main.globalTracer.isDebugEnabled()) {
			Main.globalTracer.debug(Main.structuredLog("http_response",
				Main.kv("request-id", requestId),
				Main.kv("status", status.code()),
				Main.kv("content-type", contentType),
				Main.kv("payload-bytes", payload.readableBytes())
			));
		}

		boolean keepAlive = HttpUtil.isKeepAlive(request);
		if (keepAlive) {
			response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
			ctx.writeAndFlush(response);
		} else {
			ctx.writeAndFlush(response).addListener(f -> ctx.close());
		}
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
		String requestId = getRequestId(request);
		if (Main.globalTracer != null && Main.globalTracer.isDebugEnabled()) {
			Main.globalTracer.debug(Main.structuredLog("http_request",
				Main.kv("request-id", requestId),
				Main.kv("method", request.method().name()),
				Main.kv("uri", request.uri()),
				Main.kv("remote", ctx.channel().remoteAddress())
			));
		}

		if (!request.decoderResult().isSuccess()) {
			if (Main.globalTracer != null) {
				Main.globalTracer.warn(Main.structuredLog("http_request_invalid",
					Main.kv("request-id", requestId),
					Main.kv("reason", "invalid payload")
				));
			}
			writeResponse(ctx, request, HttpResponseStatus.BAD_REQUEST, "text/plain; charset=utf-8", "Bad Request", requestId);
			return;
		}

		if (request.method().name().equals("GET")) {
			writeResponse(ctx, request, HttpResponseStatus.OK, "text/plain; charset=utf-8", "Server is up and running!", requestId);
			return;
		}

		if (!request.method().name().equals("POST")) {
			if (Main.globalTracer != null) {
				Main.globalTracer.warn(Main.structuredLog("http_method_not_allowed",
					Main.kv("request-id", requestId),
					Main.kv("method", request.method().name())
				));
			}
			writeResponse(ctx, request, HttpResponseStatus.METHOD_NOT_ALLOWED, "text/plain; charset=utf-8", "Method Not Allowed. Use POST.", requestId);
			return;
		}

		if (!RequestAuth.isAuthorized(request)) {
			String response = Main.createJsonResponse(false, "Unauthorized request", null, "ERR_UNAUTHORIZED");
			if (Main.globalTracer != null) {
				Main.globalTracer.warn(Main.structuredLog("auth_rejected",
					Main.kv("request-id", requestId),
					Main.kv("code", "ERR_UNAUTHORIZED"),
					Main.kv("mode", "global-token")
				));
			}
			writeResponse(ctx, request, HttpResponseStatus.UNAUTHORIZED, "application/json; charset=utf-8", response, requestId);
			return;
		}

		String requestBody;
		try {
			requestBody = readRequestBodyLimited(request, ConfLoader.getInstance().getMaxRequestSizeBytes());
			if (Main.globalTracer != null && Main.globalTracer.isDebugEnabled()) {
				Main.globalTracer.debug(Main.structuredLog("http_request_body",
					Main.kv("request-id", requestId),
					Main.kv("size-bytes", requestBody.getBytes(StandardCharsets.UTF_8).length)
				));
			}
		} catch (IOException e) {
			String response = Main.createJsonResponse(false, e.getMessage(), null, "ERR_REQUEST_TOO_LARGE");
			if (Main.globalTracer != null) {
				Main.globalTracer.warn(Main.structuredLog("http_request_rejected",
					Main.kv("request-id", requestId),
					Main.kv("code", "ERR_REQUEST_TOO_LARGE"),
					Main.kv("reason", e.getMessage())
				));
			}
			writeResponse(ctx, request, HttpResponseStatus.valueOf(413), "application/json; charset=utf-8", response, requestId);
			return;
		}

		String path = new QueryStringDecoder(request.uri()).path();
		if (!RequestAuth.isAppRequestAuthorized(request, path, requestBody)) {
			String response = Main.createJsonResponse(false, "Unauthorized application request", null, "ERR_UNAUTHORIZED_APP");
			if (Main.globalTracer != null) {
				Main.globalTracer.warn(Main.structuredLog("auth_rejected",
					Main.kv("request-id", requestId),
					Main.kv("code", "ERR_UNAUTHORIZED_APP"),
					Main.kv("path", path)
				));
			}
			writeResponse(ctx, request, HttpResponseStatus.UNAUTHORIZED, "application/json; charset=utf-8", response, requestId);
			return;
		}

		String requesterAppId = RequestAuth.getRequesterAppId(request);
		String requesterPrincipal = RequestAuth.getRequesterPrincipal(request);
		String response = Main.processRequest(requestBody, requesterPrincipal, requesterAppId);

		HttpResponseStatus status = HttpResponseStatus.OK;
		try {
			JSONObject responseObj = new JSONObject(response);
			if (responseObj.has("result") && !responseObj.getBoolean("result")) {
				status = HttpResponseStatus.valueOf(Main.mapErrorCodeToStatus(responseObj.optString("code", "ERR_GENERIC")));
			}
		} catch (Exception ignored) {
			status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
		}

		if (Main.globalTracer != null && Main.globalTracer.isDebugEnabled()) {
			Main.globalTracer.debug(Main.structuredLog("http_request_complete",
				Main.kv("request-id", requestId),
				Main.kv("status", status.code()),
				Main.kv("requester", requesterPrincipal)
			));
		}
		writeResponse(ctx, request, status, "application/json; charset=utf-8", response, requestId);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		if (Main.globalTracer != null) {
			Main.globalTracer.error(Main.structuredLog("netty_handler_error",
				Main.kv("remote", ctx.channel().remoteAddress()),
				Main.kv("error", cause.getMessage())
			), cause);
		}
		ctx.close();
	}
}
