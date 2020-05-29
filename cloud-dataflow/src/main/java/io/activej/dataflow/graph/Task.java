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

package io.activej.dataflow.graph;

import io.activej.common.ref.RefInt;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.inject.DatasetIdModule;
import io.activej.dataflow.inject.DatasetIdModule.DatasetIds;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.node.NodeDownload;
import io.activej.dataflow.node.NodeUpload;
import io.activej.datastream.AbstractStreamConsumer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.inject.ResourceLocator;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.activej.common.Preconditions.checkNotNull;
import static io.activej.common.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

/**
 * Represents a context of a datagraph system: environment, suppliers and
 * consumers.
 * Provides functionality to alter context and wire components.
 */
public final class Task {
	private final Map<StreamId, StreamSupplier<?>> suppliers = new LinkedHashMap<>();
	private final Map<StreamId, StreamConsumer<?>> consumers = new LinkedHashMap<>();
	private final SettablePromise<Void> executionPromise = new SettablePromise<>();

	private final long taskId;
	private final DataflowServer server;
	private final ResourceLocator environment;
	private final DatasetIds datasetIds;
	private final List<Node> nodes;

	private final Map<Node, Set<StreamId>> streams = new HashMap<>();
	private final Set<StreamId> currentStreams = new HashSet<>();

	private final AtomicBoolean bound = new AtomicBoolean();

	@Nullable
	private Node currentNode;

	private final Set<StreamId> uploads = new HashSet<>();
	private final Set<StreamId> downloads = new HashSet<>();
	private String graphViz;

	public Task(long taskId, DataflowServer server, ResourceLocator environment, List<Node> nodes) {
		this.taskId = taskId;
		this.server = server;
		this.environment = environment;
		this.nodes = nodes;
		this.datasetIds = environment.getInstance(DatasetIds.class);
	}

	public void bind() {
		if (!bound.compareAndSet(false, true)) {
			throw new IllegalStateException("Task was already bound!");
		}
		for (Node node : nodes) {
			currentNode = node;
			node.createAndBind(this);
		}
		currentNode = null;

		generateGraphViz();
	}

	public Object get(String key) {
		return environment.getInstance(datasetIds.getKeyForId(key));
	}

	public <T> T get(Class<T> cls) {
		return environment.getInstance(cls);
	}

	public <T> void bindChannel(StreamId streamId, StreamConsumer<T> consumer) {
		checkState(!consumers.containsKey(streamId), "Already bound");
		checkState(currentNode != null, "Must bind streams only from createAndBind");
		consumers.put(streamId, consumer);
		streams.computeIfAbsent(currentNode, $ -> new HashSet<>()).add(streamId);
	}

	public <T> void export(StreamId streamId, StreamSupplier<T> supplier) {
		checkState(!suppliers.containsKey(streamId), "Already exported");
		checkState(currentNode != null, "Must bind streams only from createAndBind");
		suppliers.put(streamId, supplier);
		streams.computeIfAbsent(currentNode, $ -> new HashSet<>()).add(streamId);
	}

	public long getTaskId() {
		return taskId;
	}

	public Set<StreamId> getUploads() {
		return uploads;
	}

	public Set<StreamId> getDownloads() {
		return downloads;
	}

	@SuppressWarnings("unchecked")
	public Promise<Void> execute() {
		return Promises.all(suppliers.keySet().stream().map(streamId -> {
			try {
				StreamSupplier<Object> supplier = (StreamSupplier<Object>) suppliers.get(streamId);
				StreamConsumer<Object> consumer = (StreamConsumer<Object>) consumers.get(streamId);
				checkNotNull(supplier, "Supplier not found for %s, consumer %s", streamId, consumer);
				checkNotNull(consumer, "Consumer not found for %s, supplier %s", streamId, supplier);
				currentStreams.add(streamId);
				if (consumer instanceof AbstractStreamConsumer && ((AbstractStreamConsumer<Object>) consumer).isStarted()) {
					System.out.println(streamId + ", " + supplier + ", " + consumer);
				}
				return supplier.streamTo(consumer)
						.whenComplete(() -> currentStreams.remove(streamId));
			} catch (Exception e) {
				return Promise.ofException(e);
			}
		}).collect(toList()))
				.whenComplete(executionPromise);
	}

	public void cancel() {
		suppliers.values().forEach(StreamSupplier::close);
		consumers.values().forEach(StreamConsumer::close);
	}

	public Promise<Void> getExecutionPromise() {
		return executionPromise;
	}

	public boolean isExecuted() {
		return executionPromise.isComplete();
	}

	@JmxAttribute
	public List<String> getCurrentStreams() {
		return currentStreams.stream().map(StreamId::toString).collect(toList());
	}

	@JmxAttribute
	public String getGraphViz() {
		return graphViz;
	}

	public void generateGraphViz() {
		Map<StreamId, Node> nodesByInput = new HashMap<>();
		Map<StreamId, Node> nodesByOutput = new HashMap<>();
		Map<StreamId, StreamId> network = new HashMap<>();
		Map<Node, String> ids = new LinkedHashMap<>();

		for (Node node : nodes) {
			if (node instanceof NodeDownload) {
				NodeDownload<?> download = (NodeDownload<?>) node;
				network.put(download.getStreamId(), download.getOutput());
			} else if (!(node instanceof NodeUpload)) {
				node.getInputs().forEach(input -> nodesByInput.put(input, node));
				node.getOutputs().forEach(input -> nodesByOutput.put(input, node));
			}
		}

		RefInt lastId = new RefInt(0);
		StringBuilder sb = new StringBuilder("digraph {\n");
		for (Node node : nodes) {
			if (node instanceof NodeDownload) {
				StreamId input = ((NodeDownload<?>) node).getStreamId();
				if (nodesByOutput.containsKey(input)) {
					continue;
				}
				Node target = nodesByInput.get(((NodeDownload<?>) node).getOutput());
				if (target != null) {
					downloads.add(input);
					String nodeId = "s" + input.getId();
					sb.append("  ").append(nodeId).append(" [id=\"").append(nodeId).append("\", shape=point, xlabel=\"").append(input.getId()).append("\"]\n");
					sb.append("  ").append(nodeId).append(" -> ").append(ids.computeIfAbsent(target, $ -> "n" + lastId.value++)).append(" [style=dashed]\n");
				}
				continue;
			} else if (node instanceof NodeUpload) {
				continue;
			}
			String nodeId = ids.computeIfAbsent(node, $ -> "n" + lastId.value++);
			for (StreamId output : node.getOutputs()) {
				Node target = nodesByInput.get(output);
				if (target != null) {
					sb.append("  ").append(nodeId).append(" -> ").append(ids.computeIfAbsent(target, $ -> "n" + lastId.value++)).append('\n');
					continue;
				}
				Node netTarget = nodesByInput.get(network.get(output));
				if (netTarget != null) {
					sb.append("  ").append(nodeId).append(" -> ").append(ids.computeIfAbsent(netTarget, $ -> "n" + lastId.value++));
				} else {
					uploads.add(output);
					String outputId = "s" + output.getId();
					sb.append("  ").append(outputId).append(" [id=\"").append(outputId).append("\", shape=point, xlabel=\"").append(output.getId()).append("\"]\n");
					sb.append("  ").append(nodeId).append(" -> ").append(outputId);
				}
				sb.append(" [style=dashed]\n");
			}
		}
		sb.append('\n');
		ids.forEach((node, id) -> {
			String name = node.getClass().getSimpleName();
			sb.append("  ").append(id).append(" [label=\"").append(name.startsWith("Node") ? name.substring(4) : name).append("\"]\n");
		});
		graphViz = sb.append('}').toString();
	}
}
