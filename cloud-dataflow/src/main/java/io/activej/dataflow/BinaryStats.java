package io.activej.dataflow;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.ChannelTransformer;

public class BinaryStats implements ChannelTransformer<ByteBuf, ByteBuf> {
	public static final BinaryStats ZERO = new BinaryStats() {
		@Override
		public void record(long bytes) {
			throw new UnsupportedOperationException();
		}
	};

	private long bytes = 0;

	public void record(long bytes) {
		this.bytes += bytes;
	}

	@Override
	public ChannelConsumer<ByteBuf> transform(ChannelConsumer<ByteBuf> consumer) {
		return consumer.peek(buf -> record(buf.readRemaining()));
	}

	@Override
	public ChannelSupplier<ByteBuf> transform(ChannelSupplier<ByteBuf> supplier) {
		return supplier.peek(buf -> record(buf.readRemaining()));
	}

	public long getBytes() {
		return bytes;
	}

	@Override
	public String toString() {
		return Long.toString(bytes);
	}
}
