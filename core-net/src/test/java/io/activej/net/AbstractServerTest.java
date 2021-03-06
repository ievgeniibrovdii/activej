package io.activej.net;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.common.ref.RefLong;
import io.activej.eventloop.net.SocketSettings;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;

import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public final class AbstractServerTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testTimeouts() throws IOException {
		String message = "Hello!";
		InetSocketAddress address = new InetSocketAddress("localhost", getFreePort());
		SocketSettings settings = SocketSettings.create().withImplReadTimeout(Duration.ofMillis(100000L)).withImplWriteTimeout(Duration.ofMillis(100000L));

		RefLong delay = new RefLong(5);
		SimpleServer.create(socket -> Promises.<ByteBuf>until(null,
				$ ->
						socket.read()
								.whenResult(buf ->
										getCurrentEventloop().delay(delay.inc(),
												() -> socket.write(buf)
														.whenComplete(() -> {
															if (buf == null) {
																socket.close();
															}
														}))),
				Objects::isNull))
				.withSocketSettings(settings)
				.withListenAddress(address)
				.withAcceptOnce()
				.listen();

		ByteBuf response = await(AsyncTcpSocketNio.connect(address)
				.then(socket ->
						socket.write(ByteBufStrings.wrapAscii(message))
								.then(() -> socket.write(null))
								.then(() -> {
									ByteBufQueue queue = new ByteBufQueue();
									return Promises.<ByteBuf>until(null,
											$2 -> socket.read()
													.then(buf -> {
														if (buf != null) {
															queue.add(buf);
														}
														return Promise.of(buf);
													}),
											Objects::isNull)
											.map($2 -> queue.takeRemaining());
								})
								.whenComplete(socket::close)));

		assertEquals(message, response.asString(UTF_8));
	}
}
