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

package io.activej.dataflow;

import io.activej.async.process.AsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.Messaging;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.csp.queue.*;
import io.activej.dataflow.command.DataflowCommand;
import io.activej.dataflow.command.DataflowCommandDownload;
import io.activej.dataflow.command.DataflowCommandExecute;
import io.activej.dataflow.command.DataflowResponse;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.node.Node;
import io.activej.datastream.AbstractStreamConsumer;
import io.activej.datastream.AbstractStreamSupplier;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.processor.StreamSupplierTransformer;
import io.activej.eventloop.net.SocketSettings;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Client for datagraph server.
 * Sends JSON commands for performing certain actions on server.
 */
public final class DataflowClient {
	private static final Logger logger = getLogger(DataflowClient.class);

	private final SocketSettings socketSettings = SocketSettings.createDefault();

	private final Executor executor;
	private final Path secondaryPath;

	private final ByteBufsCodec<DataflowResponse, DataflowCommand> codec;
	private final BinarySerializerLocator serializers;

	private final AtomicInteger secondaryId = new AtomicInteger(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));

	private int bufferMinSize, bufferMaxSize;

	public DataflowClient(Executor executor, Path secondaryPath, ByteBufsCodec<DataflowResponse, DataflowCommand> codec, BinarySerializerLocator serializers) {
		this.executor = executor;
		this.secondaryPath = secondaryPath;
		this.codec = codec;
		this.serializers = serializers;
	}

	public DataflowClient withBufferSizes(int bufferMinSize, int bufferMaxSize) {
		this.bufferMinSize = bufferMinSize;
		this.bufferMaxSize = bufferMaxSize;
		return this;
	}

	public <T> Promise<StreamSupplier<T>> download(InetSocketAddress address, StreamId streamId, Class<T> type) {
		return AsyncTcpSocketNio.connect(address, 0, socketSettings)
				.then(socket -> {
					Messaging<DataflowResponse, DataflowCommand> messaging = MessagingWithBinaryStreaming.create(socket, codec);
					DataflowCommandDownload commandDownload = new DataflowCommandDownload(streamId);

					return messaging.send(commandDownload)
							.map($ -> {
								ChannelQueue<ByteBuf> primaryBuffer =
										bufferMinSize == 0 && bufferMaxSize == 0 ?
												new ChannelZeroBuffer<>() :
												new ChannelBuffer<>(bufferMinSize, bufferMaxSize);

								ChannelQueue<ByteBuf> buffer = new ChannelBufferWithFallback<>(
										primaryBuffer,
										() -> ChannelFileBuffer.create(executor, secondaryPath.resolve(secondaryId.getAndIncrement() + ".bin")));

								return messaging.receiveBinaryStream()
										.transformWith(buffer)
										.transformWith(ChannelDeserializer.create(serializers.get(type))
												.withExplicitEndOfStream())
										.transformWith(new StreamTraceCounter<>(streamId, address))
										.withEndOfStream(eos -> eos
												.whenComplete(messaging::close));
							});
				});
	}

	private static class StreamTraceCounter<T> implements StreamSupplierTransformer<T, StreamSupplier<T>> {
		private final StreamId streamId;
		private final InetSocketAddress address;
		private int count = 0;
		private final Input input;
		private final Output output;

		private StreamTraceCounter(StreamId streamId, InetSocketAddress address) {
			this.streamId = streamId;
			this.address = address;
			this.input = new Input();
			this.output = new Output();

			input.getAcknowledgement()
					.whenException(output::closeEx);
			output.getEndOfStream()
					.whenResult(input::acknowledge)
					.whenException(input::closeEx);
		}

		@Override
		public StreamSupplier<T> transform(StreamSupplier<T> supplier) {
			supplier.streamTo(input);
			return output;
		}

		private final class Input extends AbstractStreamConsumer<T> {
			@Override
			protected void onEndOfStream() {
				output.sendEndOfStream();
			}

			@Override
			protected void onComplete() {
				logger.info("Received {} items total from stream {}({})", count, streamId, address);
			}
		}

		private final class Output extends AbstractStreamSupplier<T> {
			@Override
			protected void onResumed() {
				StreamDataAcceptor<T> dataAcceptor = getDataAcceptor();
				assert dataAcceptor != null;
				input.resume(item -> {
					count++;
					if (count == 1 || count % 1_000 == 0) {
						logger.info("Received {} items from stream {}({}): {}", count, streamId, address, item);
					}
					dataAcceptor.accept(item);
				});
			}

			@Override
			protected void onSuspended() {
				input.suspend();
			}
		}
	}

	public class Session implements AsyncCloseable {
		private final InetSocketAddress address;
		private final Messaging<DataflowResponse, DataflowCommand> messaging;

		private Session(InetSocketAddress address, AsyncTcpSocketNio socket) {
			this.address = address;
			this.messaging = MessagingWithBinaryStreaming.create(socket, codec);
		}

		public Promise<Void> execute(Collection<Node> nodes) {
			return messaging.send(new DataflowCommandExecute(new ArrayList<>(nodes)))
					.then(messaging::receive)
					.then(response -> {
						messaging.close();
						String error = response.getError();
						if (error != null) {
							return Promise.ofException(new Exception("Error on remote server " + address + ": " + error));
						}
						return Promise.complete();
					});
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
			messaging.closeEx(e);
		}
	}

	public Promise<Session> connect(InetSocketAddress address) {
		return AsyncTcpSocketNio.connect(address, 0, socketSettings)
				.map(socket -> new Session(address, socket));
	}
}
