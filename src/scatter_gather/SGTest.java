package scatter_gather;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class SGTest {
	public static void main(String[] args) throws Exception{
		// define the maximum number of iterations
		int maxIterations = 100;
		
		// set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment
				// .createRemoteEnvironment("slave1", 7661);
				.getExecutionEnvironment();

		RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();
		
		Graph graph = new RMatGraph<JDKRandomGenerator>(env, rnd, 70L, 9000L).generate();
		
		Graph<LongValue, Double, Double> initGraph = graph.mapVertices(new MapFunction<Vertex<LongValue, NullValue>, Double>() {

			@Override
			public Double map(Vertex<LongValue, NullValue> arg0) throws Exception {
					return 100.0;
			}
		}).mapEdges(new MapFunction<Edge<LongValue, NullValue>, Double>() {

			@Override
			public Double map(Edge<LongValue, NullValue> value) throws Exception {
				return 100.0;
			}
		})
				.getUndirected();

		// Execute the scatter-gather iteration
		Graph<LongValue, Double, Double> result = initGraph.runScatterGatherIteration(
					new MinDistanceMessenger(), new VertexDistanceUpdater(), maxIterations * 70);

		// Extract the vertices as the result
		DataSet<Vertex<LongValue, Double>> singleSourceShortestPaths = result.getVertices();

		singleSourceShortestPaths.print();

		// - - -  UDFs - - - //

		// scatter: messaging
		
	}
}



