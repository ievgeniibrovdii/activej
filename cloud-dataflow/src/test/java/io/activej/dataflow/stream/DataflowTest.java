package io.activej.dataflow.stream;

import io.activej.codec.StructuredCodec;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.collector.Collector;
import io.activej.dataflow.command.DataflowCommand;
import io.activej.dataflow.command.DataflowResponse;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.dataset.impl.DatasetConsumerOfId;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.http.DataflowDebugServlet;
import io.activej.dataflow.inject.BinarySerializerModule;
import io.activej.dataflow.inject.CodecsModule.Subtypes;
import io.activej.dataflow.inject.DataflowModule;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.dataflow.server.http.DataflowServerControlServlet;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpClient;
import io.activej.http.AsyncHttpServer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.codec.StructuredCodec.ofObject;
import static io.activej.common.collection.CollectionUtils.concat;
import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertComplete;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static java.util.Comparator.comparing;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class DataflowTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private ExecutorService executor;

	@Before
	public void setUp() {
		executor = Executors.newSingleThreadExecutor();
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	@Test
	public void testForward() throws Exception {

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(new Partition(address1), new Partition(address2)))
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(1),
						new TestItem(3),
						new TestItem(5)))
				.bind(datasetId("result")).toInstance(result1)
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(2),
						new TestItem(4),
						new TestItem(6)
				))
				.bind(datasetId("result")).toInstance(result2)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		Dataset<TestItem> items = datasetOfId("items", TestItem.class);
		DatasetConsumerOfId<TestItem> consumerNode = consumerOfId(items, "result");
		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(asList(new TestItem(1), new TestItem(3), new TestItem(5)), result1.getList());
		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), result2.getList());
	}

	@Test
	public void testRepartitionAndSort() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(new Partition(address1), new Partition(address2))).build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3),
						new TestItem(4),
						new TestItem(5),
						new TestItem(6)))
				.bind(datasetId("result")).toInstance(result1)
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(1),
						new TestItem(6)))
				.bind(datasetId("result")).toInstance(result2)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		SortedDataset<Long, TestItem> items = repartitionSort(sortedDatasetOfId("items",
				TestItem.class, Long.class, new TestKeyFunction(), new TestComparator()));
		DatasetConsumerOfId<TestItem> consumerNode = consumerOfId(items, "result");
		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				})));

		List<TestItem> results = new ArrayList<>();
		results.addAll(result1.getList());
		results.addAll(result2.getList());
		results.sort(Comparator.comparingLong(item -> item.value));

		assertEquals(asList(
				new TestItem(1),
				new TestItem(1),
				new TestItem(2),
				new TestItem(3),
				new TestItem(4),
				new TestItem(5),
				new TestItem(6),
				new TestItem(6)), results);
	}

	@Test
	public void testRepartitionWithFurtherSort() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();
		InetSocketAddress address3 = getFreeListenAddress();

		Partition partition1 = new Partition(address1);
		Partition partition2 = new Partition(address2);
		Partition partition3 = new Partition(address3);

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(partition1, partition2, partition3))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result3 = StreamConsumerToList.create();

		List<TestItem> list1 = asList(
				new TestItem(15),
				new TestItem(12),
				new TestItem(13),
				new TestItem(17),
				new TestItem(11),
				new TestItem(13));
		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(list1)
				.bind(datasetId("result")).toInstance(result1)
				.build();

		List<TestItem> list2 = asList(
				new TestItem(21),
				new TestItem(26));
		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(list2)
				.bind(datasetId("result")).toInstance(result2)
				.build();

		List<TestItem> list3 = asList(
				new TestItem(33),
				new TestItem(35),
				new TestItem(31),
				new TestItem(38),
				new TestItem(36));
		Module serverModule3 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(list3)
				.bind(datasetId("result")).toInstance(result3)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);
		DataflowServer server3 = Injector.of(serverModule3).getInstance(DataflowServer.class).withListenAddress(address3);

		server1.listen();
		server2.listen();
		server3.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		Dataset<TestItem> items = localSort(
				repartition(
						datasetOfId("items", TestItem.class),
						new TestKeyFunction(),
						asList(partition2, partition3)
				),
				Long.class,
				new TestKeyFunction(),
				new TestComparator()
		);
		Dataset<TestItem> consumerNode = consumerOfId(items, "result");
		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
					server3.close();
				})));

		Set<TestItem> expectedOnServers2And3 = new HashSet<>();
		expectedOnServers2And3.addAll(list1);
		expectedOnServers2And3.addAll(list2);
		expectedOnServers2And3.addAll(list3);

		assertTrue(isSorted(result2.getList(), comparing(testItem -> testItem.value)));
		assertTrue(isSorted(result3.getList(), comparing(testItem -> testItem.value)));

		Set<TestItem> actualOnServers2And3 = new HashSet<>(concat(result2.getList(), result3.getList()));
		assertEquals(expectedOnServers2And3, actualOnServers2And3);
		assertTrue(result1.getList().isEmpty());
	}

	@Test
	public void testFilter() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(new Partition(address1), new Partition(address2)))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(6),
						new TestItem(4),
						new TestItem(2),
						new TestItem(3),
						new TestItem(1)))
				.bind(datasetId("result")).toInstance(result1)
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(7),
						new TestItem(7),
						new TestItem(8),
						new TestItem(2),
						new TestItem(5)))
				.bind(datasetId("result")).toInstance(result2)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		Dataset<TestItem> filterDataset = filter(datasetOfId("items", TestItem.class), new TestPredicate());
		LocallySortedDataset<Long, TestItem> sortedDataset = localSort(filterDataset, long.class, new TestKeyFunction(), new TestComparator());
		DatasetConsumerOfId<TestItem> consumerNode = consumerOfId(sortedDataset, "result");
		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), result1.getList());
		assertEquals(asList(new TestItem(2), new TestItem(8)), result2.getList());
	}

	@Test
	public void testCollector() throws Exception {
		StreamConsumerToList<TestItem> resultConsumer = StreamConsumerToList.create();

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(new Partition(address1), new Partition(address2)))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3),
						new TestItem(4),
						new TestItem(5)))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(6),
						new TestItem(7),
						new TestItem(8),
						new TestItem(9),
						new TestItem(10)))
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		Injector clientInjector = Injector.of(common);
		DataflowClient client = clientInjector.getInstance(DataflowClient.class);
		DataflowGraph graph = clientInjector.getInstance(DataflowGraph.class);

		Dataset<TestItem> filterDataset = filter(datasetOfId("items", TestItem.class), new TestPredicate());
		LocallySortedDataset<Long, TestItem> sortedDataset = localSort(filterDataset, long.class, new TestKeyFunction(), new TestComparator());

		Collector<TestItem> collector = new Collector<>(sortedDataset, client);
		StreamSupplier<TestItem> resultSupplier = collector.compile(graph);

		resultSupplier.streamTo(resultConsumer).whenComplete(assertComplete());

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6), new TestItem(8), new TestItem(10)), resultConsumer.getList());
	}

	public static final class TestItem {
		@Serialize(order = 0)
		public final long value;

		public TestItem(@Deserialize("value") long value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return "TestItem{value=" + value + '}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TestItem other = (TestItem) o;
			return value == other.value;
		}

		@Override
		public int hashCode() {
			return (int) (value ^ (value >>> 32));
		}
	}

	public static class TestComparator implements Comparator<Long> {
		@Override
		public int compare(Long o1, Long o2) {
			return o1.compareTo(o2);
		}
	}

	public static class TestKeyFunction implements Function<TestItem, Long> {
		@Override
		public Long apply(TestItem item) {
			return item.value;
		}
	}

	private static class TestPredicate implements Predicate<TestItem> {
		@Override
		public boolean test(TestItem input) {
			return input.value % 2 == 0;
		}
	}

	static ModuleBuilder createCommon(Executor executor, Path secondaryPath, List<Partition> graphPartitions) {
		return ModuleBuilder.create()
				.install(DataflowModule.create())
				.bind(Executor.class).toInstance(executor)
				.bind(Eventloop.class).toInstance(Eventloop.getCurrentEventloop())
				.scan(new Object() {

					@Provides
					DataflowServer server(Eventloop eventloop, ByteBufsCodec<DataflowCommand, DataflowResponse> codec, BinarySerializerModule.BinarySerializerLocator serializers, Injector environment) {
						return new DataflowServer(eventloop, codec, serializers, environment);
					}

					@Provides
					DataflowClient client(Executor executor, ByteBufsCodec<DataflowResponse, DataflowCommand> codec, BinarySerializerModule.BinarySerializerLocator serializers) {
						return new DataflowClient(executor, secondaryPath, codec, serializers);
					}

					@Provides
					DataflowGraph graph(DataflowClient client, @Subtypes StructuredCodec<Node> nodeCodec) {
						return new DataflowGraph(client, graphPartitions, nodeCodec);
					}

					@Provides
					@Named("server")
					AsyncHttpServer debugServer(Eventloop eventloop, Executor executor, DataflowServer server, DataflowClient client) {
						DataflowServerControlServlet servlet = new DataflowServerControlServlet(server, client, executor);
						AsyncHttpServer debugServer = AsyncHttpServer.create(eventloop, servlet);
						servlet.setParent(debugServer);
						return debugServer;
					}

					@Provides
					AsyncHttpClient httpClient(Eventloop eventloop) {
						return AsyncHttpClient.create(eventloop);
					}

					@Provides
					AsyncHttpServer debugServer(Eventloop eventloop, AsyncHttpClient client, Executor executor) {
						return AsyncHttpServer.create(eventloop, new DataflowDebugServlet(graphPartitions, client, executor));
					}
				})
				.bind(new Key<StructuredCodec<TestComparator>>() {}).toInstance(ofObject(TestComparator::new))
				.bind(new Key<StructuredCodec<TestKeyFunction>>() {}).toInstance(ofObject(TestKeyFunction::new))
				.bind(new Key<StructuredCodec<TestPredicate>>() {}).toInstance(ofObject(TestPredicate::new));
	}

	static InetSocketAddress getFreeListenAddress() {
		try {
			return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), getFreePort());
		} catch (UnknownHostException ignored) {
			throw new AssertionError();
		}
	}

	private static <T> boolean isSorted(Collection<T> collection, Comparator<T> comparator) {
		if (collection.size() < 2) return true;
		Iterator<T> iterator = collection.iterator();
		T current = iterator.next();
		while (iterator.hasNext()) {
			T next = iterator.next();
			if (comparator.compare(current, next) > 0) {
				return false;
			}
			current = next;
		}
		return true;
	}
}
