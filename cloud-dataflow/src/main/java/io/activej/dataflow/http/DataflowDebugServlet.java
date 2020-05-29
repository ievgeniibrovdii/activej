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

package io.activej.dataflow.http;

import io.activej.common.exception.UncheckedException;
import io.activej.dataflow.graph.Partition;
import io.activej.http.*;
import io.activej.promise.Async;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executor;

import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpResponse.ok200;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class DataflowDebugServlet implements AsyncServlet {
	private final AsyncServlet servlet;

	public DataflowDebugServlet(List<Partition> partitions, AsyncHttpClient httpClient, Executor executor) {
		List<InetSocketAddress> debugAddresses = partitions.stream()
				.map(p -> new InetSocketAddress(p.getAddress().getAddress(), p.getAddress().getPort() + 1))
				.collect(toList());

		this.servlet = RoutingServlet.create()
				.map("/*", StaticServlet.ofClassPath(executor, "debug").withIndexHtml())
				.map("/api/*", RoutingServlet.create()
						.map(GET, "/partitions", request -> ok200()
								.withJson(partitions.stream().map(p -> "\"" + p.getAddress().getAddress().getHostAddress() + ":" + p.getAddress().getPort() + "\"").collect(joining(",", "[", "]"))))
						.map("/partitions/:index/*", request -> {
							String index = request.getPathParameter("index");
							InetSocketAddress partition;
							try {
								partition = debugAddresses.get(Integer.parseInt(index));
							} catch (NumberFormatException | IndexOutOfBoundsException e) {
								return HttpResponse.ofCode(400).withPlainText("Bad index: " + index);
							}
							return httpClient.request(HttpRequest.get("http://" + partition.getAddress().getHostAddress() + ":" + partition.getPort() + "/api/" + request.getRelativePath()))
									.thenEx((httpResponse, throwable) -> {
										if (throwable == null) {
											return Promise.of(httpResponse);
										}
										if (throwable instanceof HttpException) {
											return ((HttpException) throwable).get();
										}
										return Promise.ofException(throwable);
									});
						}));
	}

	@Override
	@NotNull
	public Async<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		return servlet.serve(request);
	}
}
