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

package io.activej.dataflow.collector;

import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.NodeUpload;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamMerger;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public class MergeCollector<K, T> {
	private final Dataset<T> input;
	private final DataflowClient client;
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final boolean distinct;

	public MergeCollector(SortedDataset<K, T> input, DataflowClient client, boolean distinct) {
		this(input, client, input.keyFunction(), input.keyComparator(), distinct);
	}

	public MergeCollector(Dataset<T> input, DataflowClient client,
	                      Function<T, K> keyFunction, Comparator<K> keyComparator, boolean distinct) {
		this.input = input;
		this.client = client;
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.distinct = distinct;
	}

	public StreamSupplier<T> compile(DataflowGraph graph) {
		DataflowContext context = DataflowContext.of(graph);
		List<StreamId> inputStreamIds = input.channels(context);

		StreamMerger<K, T> merger = StreamMerger.create(keyFunction, keyComparator, distinct);
		for (StreamId streamId : inputStreamIds) {
			NodeUpload<T> nodeUpload = new NodeUpload<>(context.generateNodeIndex(), input.valueType(), streamId);
			Partition partition = graph.getPartition(streamId);
			graph.addNode(partition, nodeUpload);
			StreamSupplier<T> supplier = StreamSupplier.ofPromise(client.download(partition.getAddress(), streamId, input.valueType()));
			supplier.streamTo(merger.newInput());
		}

		return merger.getOutput();
	}
}
