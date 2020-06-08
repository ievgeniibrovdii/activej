package io.activej.dataflow.stream;

import io.activej.codec.StructuredCodec;
import io.activej.common.tuple.Tuple2;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.dataset.impl.DatasetConsumerOfId;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamJoin.InnerJoiner;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.http.AsyncHttpServer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static io.activej.codec.StructuredCodecs.object;
import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.dataflow.stream.DataflowTest.createCommon;
import static io.activej.dataflow.stream.DataflowTest.getFreeListenAddress;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertComplete;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class PageRankTest {
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

	public static class Page {
		@Serialize(order = 0)
		public final long pageId;
		@Serialize(order = 1)
		public final long[] links;

		public Page(@Deserialize("pageId") long pageId, @Deserialize("links") long[] links) {
			this.pageId = pageId;
			this.links = links;
		}

		public void disperse(Rank rank, StreamDataAcceptor<Rank> cb) {
			for (long link : links) {
				Rank newRank = new Rank(link, rank.value / links.length);
				cb.accept(newRank);
			}
		}

		@Override
		public String toString() {
			return "Page{pageId=" + pageId + ", links=" + Arrays.toString(links) + '}';
		}
	}

	public static class PageKeyFunction implements Function<Page, Long> {
		@Override
		public Long apply(Page page) {
			return page.pageId;
		}
	}

	public static class Rank {
		@Serialize(order = 0)
		public final long pageId;
		@Serialize(order = 1)
		public final double value;

		public Rank(@Deserialize("pageId") long pageId, @Deserialize("value") double value) {
			this.pageId = pageId;
			this.value = value;
		}

		@Override
		public String toString() {
			return "Rank{pageId=" + pageId + ", value=" + value + '}';
		}

		@SuppressWarnings({"SimplifiableIfStatement", "EqualsWhichDoesntCheckParameterClass"})
		@Override
		public boolean equals(Object o) {
			Rank rank = (Rank) o;
			if (pageId != rank.pageId) return false;
			return Math.abs(rank.value - value) < 0.001;
		}
	}

	public static class RankKeyFunction implements Function<Rank, Long> {
		@Override
		public Long apply(Rank rank) {
			return rank.pageId;
		}
	}

	public static class RankAccumulator {
		@Serialize(order = 0)
		public long pageId;
		@Serialize(order = 1)
		public double accumulatedRank;

		@SuppressWarnings("unused")
		public RankAccumulator() {
		}

		public RankAccumulator(long pageId) {
			this.pageId = pageId;
		}

		@Override
		public String toString() {
			return "RankAccumulator{pageId=" + pageId + ", accumulatedRank=" + accumulatedRank + '}';
		}
	}

	public static class RankAccumulatorKeyFunction implements Function<RankAccumulator, Long> {
		@Override
		public Long apply(RankAccumulator rankAccumulator) {
			return rankAccumulator.pageId;
		}
	}

	private static class RankAccumulatorReducer extends ReducerToResult<Long, Rank, Rank, RankAccumulator> {
		@Override
		public RankAccumulator createAccumulator(Long pageId) {
			return new RankAccumulator(pageId);
		}

		@Override
		public RankAccumulator accumulate(RankAccumulator accumulator, Rank value) {
			accumulator.accumulatedRank += value.value;
			return accumulator;
		}

		@Override
		public RankAccumulator combine(RankAccumulator accumulator, RankAccumulator anotherAccumulator) {
			accumulator.accumulatedRank += anotherAccumulator.accumulatedRank;
			return accumulator;
		}

		@Override
		public Rank produceResult(RankAccumulator accumulator) {
			return new Rank(accumulator.pageId, accumulator.accumulatedRank);
		}
	}

	public static class LongComparator implements Comparator<Long> {
		@Override
		public int compare(Long l1, Long l2) {
			return l1.compareTo(l2);
		}
	}

	public static class PageToRankFunction implements Function<Page, Rank> {
		@Override
		public Rank apply(Page page) {
			return new Rank(page.pageId, 1.0);
		}
	}

	public static class PageRankJoiner extends InnerJoiner<Long, Page, Rank, Rank> {
		@Override
		public void onInnerJoin(Long key, Page page, Rank rank, StreamDataAcceptor<Rank> output) {
			page.disperse(rank, output);
		}
	}

	private static SortedDataset<Long, Rank> pageRankIteration(SortedDataset<Long, Page> pages, SortedDataset<Long, Rank> ranks) {
		Dataset<Rank> updates = join(pages, ranks, new PageRankJoiner(), Rank.class, new RankKeyFunction());

		Dataset<Rank> newRanks = sortReduceRepartitionReduce(updates, new RankAccumulatorReducer(),
				Long.class, new RankKeyFunction(), new LongComparator(),
				RankAccumulator.class, new RankAccumulatorKeyFunction(),
				Rank.class);

		return castToSorted(newRanks, Long.class, new RankKeyFunction(), new LongComparator());
	}

	private static SortedDataset<Long, Rank> pageRank(SortedDataset<Long, Page> pages) {
		SortedDataset<Long, Rank> ranks = castToSorted(map(pages, new PageToRankFunction(), Rank.class),
				Long.class, new RankKeyFunction(), new LongComparator());

		for (int i = 0; i < 10; i++) {
			ranks = pageRankIteration(pages, ranks);
		}

		return ranks;
	}

	private Module createModule(Partition... partitions) throws Exception {
		return createCommon(executor, temporaryFolder.newFolder().toPath(), asList(partitions))
				.bind(new Key<StructuredCodec<PageKeyFunction>>() {}).toInstance(object(PageKeyFunction::new))
				.bind(new Key<StructuredCodec<RankKeyFunction>>() {}).toInstance(object(RankKeyFunction::new))
				.bind(new Key<StructuredCodec<RankAccumulatorKeyFunction>>() {}).toInstance(object(RankAccumulatorKeyFunction::new))
				.bind(new Key<StructuredCodec<LongComparator>>() {}).toInstance(object(LongComparator::new))
				.bind(new Key<StructuredCodec<PageToRankFunction>>() {}).toInstance(object(PageToRankFunction::new))
				.bind(new Key<StructuredCodec<RankAccumulatorReducer>>() {}).toInstance(object(RankAccumulatorReducer::new))
				.bind(new Key<StructuredCodec<PageRankJoiner>>() {}).toInstance(object(PageRankJoiner::new))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();
	}

	private static final InetSocketAddress address1 = new InetSocketAddress(3535);
	private static final InetSocketAddress address2 = new InetSocketAddress(3540);

	public Tuple2<StreamConsumerToList<Rank>, Runnable> launchServer(InetSocketAddress address, List<Page> pages) throws Exception {
		StreamConsumerToList<Rank> result = StreamConsumerToList.create();
		Injector env = Injector.of(ModuleBuilder.create()
				.install(createModule())
				.bind(datasetId("items")).toInstance(pages)
				.bind(datasetId("result")).toInstance(result)
				.build());
		DataflowServer server = env.getInstance(DataflowServer.class).withListenAddress(address);
		AsyncHttpServer debugServer = env.getInstance(Key.of(AsyncHttpServer.class).named("server")).withListenPort(address.getPort() + 1);
		debugServer.listen();
		server.listen();
		return new Tuple2<>(result, () -> {
			debugServer.close();
			server.close();
		});
	}

	private static List<Page> generatePages(int number) {
		List<Page> pages = new ArrayList<>(number);
		for (int i = 0; i < number; i++) {
			long[] links = new long[ThreadLocalRandom.current().nextInt(number / 3)];
			for (int j = 0; j < links.length; j++) {
				links[j] = ThreadLocalRandom.current().nextInt(number);
			}
			pages.add(new Page(i, links));
		}
		return pages;
	}

	@Test
	@Ignore
	public void launchServers() throws Exception {
		launchServer(address1, generatePages(1000));
		launchServer(address2, generatePages( 900));
		await();
	}

	@Test
	@Ignore
	public void postPageRankTask() throws Exception {
		SortedDataset<Long, Page> sorted = sortedDatasetOfId("items", Page.class, Long.class, new PageKeyFunction(), new LongComparator());
		SortedDataset<Long, Page> repartitioned = repartitionSort(sorted);
		SortedDataset<Long, Rank> pageRanks = pageRank(repartitioned);

		Injector env = Injector.of(createModule(new Partition(address1), new Partition(address2)));
		DataflowGraph graph = env.getInstance(DataflowGraph.class);
		consumerOfId(pageRanks, "result").channels(DataflowContext.of(graph));

		await(graph.execute());
	}

	@Test
	@Ignore
	public void runDebugServer() throws Exception {
		Injector env = Injector.of(createModule(new Partition(address1), new Partition(address2)));
		env.getInstance(AsyncHttpServer.class).withListenPort(8080).listen();
		await();
	}

	@Test
	public void test() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createModule(new Partition(PageRankTest.address1), new Partition(address2));

		Tuple2<StreamConsumerToList<Rank>, Runnable> serverTuple1 = launchServer(PageRankTest.address1, asList(
				new Page(1, new long[]{1, 2, 3}),
				new Page(3, new long[]{1})));
		Tuple2<StreamConsumerToList<Rank>, Runnable> serverTuple2 = launchServer(address2, singletonList(new Page(2, new long[]{1})));

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		SortedDataset<Long, Page> pages = repartitionSort(sortedDatasetOfId("items",
				Page.class, Long.class, new PageKeyFunction(), new LongComparator()));

		SortedDataset<Long, Rank> pageRanks = pageRank(pages);

		DatasetConsumerOfId<Rank> consumerNode = consumerOfId(pageRanks, "result");

		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					serverTuple1.getValue2().run();
					serverTuple2.getValue2().run();
				})));

		List<Rank> result = new ArrayList<>();
		result.addAll(serverTuple1.getValue1().getList());
		result.addAll(serverTuple2.getValue1().getList());
		result.sort(Comparator.comparingLong(rank -> rank.pageId));

		assertEquals(asList(new Rank(1, 1.7861), new Rank(2, 0.6069), new Rank(3, 0.6069)), result);
	}
}
