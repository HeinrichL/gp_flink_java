import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;

public class GermanCitiesAnt {
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment
				// .createRemoteEnvironment("slave1", 7661);
				.getExecutionEnvironment();

		DataSet<Vertex<Long, String>> vertices = env.fromElements(new Vertex<Long, String>(0L, "hamburg"),
				new Vertex<Long, String>(1L, "berlin"), new Vertex<Long, String>(2L, "bremen"),
				new Vertex<Long, String>(3L, "erfurt"), new Vertex<Long, String>(4L, "köln"),
				new Vertex<Long, String>(5L, "stuttgart"), new Vertex<Long, String>(6L, "münchen"));

		DataSet<Edge<Long, Long>> edges = env.fromElements(new Edge<Long, Long>(0L, 1L, 300L),
				new Edge<Long, Long>(0L, 2L, 200L), new Edge<Long, Long>(1L, 3L, 400L),
				new Edge<Long, Long>(2L, 4L, 400L), new Edge<Long, Long>(2L, 3L, 400L),
				new Edge<Long, Long>(3L, 4L, 500L), new Edge<Long, Long>(3L, 5L, 500L),
				new Edge<Long, Long>(3L, 6L, 500L), new Edge<Long, Long>(4L, 5L, 400L),
				new Edge<Long, Long>(5L, 6L, 300L));

		Graph<Long, String, Long> graph = Graph.fromDataSet(vertices, edges, env);

		Graph<Long, String, Long> parsed = Graph
				.fromCsvReader("C:\\Users\\Heinrich\\Desktop\\vertices-german.txt",
						"C:\\Users\\Heinrich\\Desktop\\edges-german.txt", env)
				.types(Long.class, String.class, Long.class);

		System.out.println(graph.getVertices().collect());
		System.out.println(graph.getEdges().collect());
		System.out.println(parsed.getVertices().collect());
		System.out.println(parsed.getEdges().collect());

		Graph<Long, Tuple2<String, Long>, Tuple2<Long, Double>> withPheromon = parsed
				.mapVertices(new MapFunction<Vertex<Long,String>, Tuple2<String, Long>>() {

					@Override
					public Tuple2<String, Long> map(Vertex<Long, String> arg0) throws Exception {
						Tuple2<String, Long> res = new Tuple2<String, Long>();
						res.f0 = arg0.f1;
						res.f1 = Long.MAX_VALUE;
						return res;
					}
				})
				.mapEdges(new MapFunction<Edge<Long, Long>, Tuple2<Long, Double>>() {

					@Override
					public Tuple2<Long, Double> map(Edge<Long, Long> arg0) throws Exception {
						Tuple2<Long, Double> res = new Tuple2<Long, Double>();
						res.f0 = arg0.f2;
						res.f1 = 0.0;
						return res;
					}
				}).getUndirected();
		
		System.out.println(withPheromon.getVertices().collect());
		System.out.println(withPheromon.getEdges().collect());
		
//		Graph filtered = withPheromon.filterOnEdges(new FilterFunction<Edge<Long, Tuple2<Long, Double>>>() {
//
//			@Override
//			public boolean filter(Edge<Long, Tuple2<Long, Double>> arg0) throws Exception {
//				return arg0.f2.f0 > 300;
//			}
//		});
		
//		for(int i = 0; i < 100; i++){
//			for(Vertex v : withPheromon.getVertices().i)
//		}
		
		//withPheromon.map

		// Execute the vertex-centric iteration
		Graph result = withPheromon.runVertexCentricIteration(new SSSPComputeFunction(),
				new SSSPCombiner(), 200);

		System.out.println(result.getVertices().collect());
		System.out.println(result.getEdges().collect());

	}
}


