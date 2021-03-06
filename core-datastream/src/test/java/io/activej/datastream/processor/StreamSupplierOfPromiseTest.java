package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.async.process.AsyncCloseable.CLOSE_EXCEPTION;
import static io.activej.datastream.TestStreamTransformers.randomlySuspending;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamSupplierOfPromiseTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testOfPromise() {
		StreamSupplier<Integer> delayedSupplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamSupplier<Integer> supplier = StreamSupplier.ofPromise(Promise.complete().async().map($ -> delayedSupplier));
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEndOfStream(supplier, consumer);
		assertEndOfStream(delayedSupplier);
		assertEquals(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), consumer.getList());
	}

	@Test
	public void testClosedImmediately() {
		StreamSupplier<Integer> delayedSupplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamSupplier<Integer> supplier = StreamSupplier.ofPromise(Promise.complete().async().map($ -> delayedSupplier));
		supplier.close();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Throwable exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(CLOSE_EXCEPTION, exception);
		assertClosedWithError(CLOSE_EXCEPTION, supplier, consumer);
		assertClosedWithError(CLOSE_EXCEPTION, delayedSupplier);
		assertEquals(0, consumer.getList().size());
	}

	@Test
	public void testClosedDelayedSupplier() {
		StreamSupplier<Integer> delayedSupplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		delayedSupplier.close();
		StreamSupplier<Integer> supplier = StreamSupplier.ofPromise(Promise.complete().async().map($ -> delayedSupplier));
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		Throwable exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(CLOSE_EXCEPTION, exception);
		assertClosedWithError(CLOSE_EXCEPTION, supplier, consumer);
		assertClosedWithError(CLOSE_EXCEPTION, delayedSupplier);
		assertEquals(0, consumer.getList().size());
	}

}
