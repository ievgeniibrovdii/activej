/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.dataflow.server.http;

import io.activej.common.exception.UncheckedException;
import io.activej.dataflow.BinaryStats;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.http.*;
import io.activej.promise.Async;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.Executor;

import static io.activej.http.HttpHeaderValue.ofContentType;
import static io.activej.http.HttpHeaders.CONTENT_TYPE;
import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpMethod.POST;
import static io.activej.http.HttpResponse.ok200;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

public final class DataflowServerControlServlet implements AsyncServlet {
	private static final HttpHeaderValue CONTENT_TYPE_GRAPHVIZ =
			ofContentType(ContentType.of(MediaTypes.GV, UTF_8));

	private final AsyncServlet servlet;

	private AsyncHttpServer server;

	public DataflowServerControlServlet(DataflowServer server, DataflowClient client, Executor executor) {
		this.servlet = RoutingServlet.create()
				.map(POST, "/api/stop", request -> {
					server.close();
					this.server.close();
					return ok200();
				})
				.map("/api/streams/*", RoutingServlet.create()
						.map(GET, "/", request -> ok200()
								.withJson(server.getUploadStats().entrySet().stream()
										.map(e -> "\"" + e.getKey().getId() + "\":" + e.getValue().getBytes())
										.collect(joining(",", "{", "}")))))
				.map("/api/tasks/*", RoutingServlet.create()
						.map(GET, "/", request -> ok200()
								.withJson("{\"succeeded\":" + server.getSucceededTasks() +
										",\"failed\":" + server.getFailedTasks() +
										",\"cancelled\":" + server.getCanceledTasks() +
										",\"running\":" + server.getRunningTasks().keySet().stream().map(Object::toString).collect(joining(",", "[", "]")) +
										",\"last\":" + server.getLastRanTasks().keySet().stream().map(Object::toString).collect(joining(",", "[", "]")) +
										"}"))
						.map("/:taskID/*", RoutingServlet.create()
								.map(GET, "/streams", request ->
										getTask(server, request)
												.map(task -> {
													Map<StreamId, BinaryStats> uploadStats = server.getUploadStats();
													Map<StreamId, BinaryStats> downloadStats = client.getDownloadStats();
													return ok200()
															.withJson("{\"uploads\":" +
																	task.getUploads().stream()
																			.map(s -> "\"" + s.getId() + "\":" + uploadStats.getOrDefault(s, BinaryStats.ZERO))
																			.collect(joining(",", "{", "}")) +
																	",\"downloads\":" +
																	task.getDownloads().stream()
																			.map(s -> "\"" + s.getId() + "\":" + downloadStats.getOrDefault(s, BinaryStats.ZERO))
																			.collect(joining(",", "{", "}")) + "}");
												}))
								.map(GET, "/graph", request ->
										getTask(server, request)
												.map(task -> ok200()
														.withHeader(CONTENT_TYPE, CONTENT_TYPE_GRAPHVIZ)
														.withBody(task.getGraphViz().getBytes(UTF_8))))
								.map(POST, "/cancel", request ->
										getTask(server, request)
												.map(task -> {
													task.cancel();
													return ok200();
												})))
						.map(POST, "/all/cancel", request -> {
							server.cancelAll();
							return ok200();
						}));
	}

	public void setParent(AsyncHttpServer server) {
		this.server = server;
	}

	private static Promise<Long> getId(HttpRequest request, String paramName) {
		String param = request.getPathParameter(paramName);
		try {
			return Promise.of(Long.parseLong(param));
		} catch (NumberFormatException e) {
			return Promise.ofException(HttpException.ofCode(400, "Bad number " + param));
		}
	}

	private static Promise<Task> getTask(DataflowServer server, HttpRequest request) {
		return getId(request, "taskID")
				.then(id -> {
					Task task = server.getRunningTasks().get(id);
					return task != null ?
							Promise.of(task) :
							Promise.ofException(HttpException.ofCode(404, "No task with id " + id));
				});
	}

	@Override
	@NotNull
	public Async<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		return servlet.serve(request);
	}
}
