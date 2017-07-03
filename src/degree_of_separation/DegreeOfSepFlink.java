package degree_of_separation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

public class DegreeOfSepFlink {
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment
				// .createRemoteEnvironment("slave1", 7661);
				.getExecutionEnvironment();

		Graph<Long, NullValue, NullValue> parsed = Graph
				.fromCsvReader("C:\\Users\\Heinrich\\Desktop\\twitter_combined_csv.txt", env).keyType(Long.class);
		// .types(Long.class, String.class, Long.class);

//		System.out.println(parsed.getVertices().collect());
//		System.out.println(parsed.getEdges().collect());

		long srcId = 214328887L;// parsed.getVertexIds().

		// Graph<VertexID, VertexValue, EdgeValue>
		Graph initGraph = parsed.mapVertices(new MapFunction<Vertex<Long, NullValue>, Integer>() {

			@Override
			public Integer map(Vertex<Long, NullValue> arg0) throws Exception {
				if (arg0.f0 != srcId) {
					return Integer.MAX_VALUE;
				} else {
					return 0;
				}
			}
		}).getUndirected();

		// System.out.println(initGraph.getVertices().collect());
		// System.out.println(initGraph.getEdges().collect());

		Graph result = initGraph.runGatherSumApplyIteration(new Gather(), new Sum(), new Apply(), 20);
		// initGraph.runVertexCentricIteration(new
		// DegreeOfSeparationComputeFunction(srcId),
		// new DegreeOfSeparationCombiner(), 200);

		DataSet<Tuple2<Integer, Integer>> tuples = result.getVertices().map(new MapFunction<Vertex<Long, Integer>, Tuple2<Integer, Integer>>() {

			@Override
			public Tuple2<Integer, Integer> map(Vertex<Long, Integer> arg0) throws Exception {
				return new Tuple2(arg0.f1, 1);
			}

		});
		
		DataSet<Tuple2<Integer, Integer>> reduced = tuples.groupBy(0).sum(1);

		reduced.print();
		
		// System.out.println(result.getEdges().collect());
	}
}
