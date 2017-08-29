package ant;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.StringValue;

public class CityData {
	
	public static Graph<Long, String, Integer> initGraph(ExecutionEnvironment env){
		DataSet<Vertex<Long, String>> vertices = env.fromElements(new Vertex<Long, String>(0L, "hamburg"),
				new Vertex<Long, String>(1L, "berlin"), new Vertex<Long, String>(2L, "bremen"),
				new Vertex<Long, String>(3L, "erfurt"), new Vertex<Long, String>(4L, "köln"),
				new Vertex<Long, String>(5L, "stuttgart"), new Vertex<Long, String>(6L, "münchen"));

		DataSet<Edge<Long, Integer>> edges = env.fromElements(new Edge<Long, Integer>(0L, 1L, 300),
				new Edge<Long, Integer>(0L, 2L, 200), new Edge<Long, Integer>(1L, 3L, 400),
				new Edge<Long, Integer>(2L, 4L, 400), new Edge<Long, Integer>(2L, 3L, 400),
				new Edge<Long, Integer>(3L, 4L, 500), new Edge<Long, Integer>(3L, 5L, 500),
				new Edge<Long, Integer>(3L, 6L, 500), new Edge<Long, Integer>(4L, 5L, 400),
				new Edge<Long, Integer>(5L, 6L, 300));

		Graph<Long, String, Integer> graph = Graph.fromDataSet(vertices, edges, env);
		return graph;
	}
	
	public static Graph<Long, String, Integer> testGraph(ExecutionEnvironment env, String graphFile){

		Graph<Long, String, Integer> graph = Graph.fromCsvReader(graphFile, 
				new MapFunction<Long, String>() {

					@Override
					public String map(Long value) throws Exception {
						return value.toString();
					}
				},
				env)
				.types(Long.class, String.class, Integer.class);
		return graph;
	}

}
