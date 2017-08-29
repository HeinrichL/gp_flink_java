package ant;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;

public class TSPAnt {
	static final int Q = 500;
	static final int alpha = -1;
	static final int beta = 5;
	static final double evaporation = 0.5;
	static final double antFactor = 0.8;

	static final double randomCityProp = 0.01;

	// @SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		// set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		String hdfs = args[0];
		String graphFile = args[1];
		int numIter = Integer.parseInt(args[2]);

		Graph<Long, String, Integer> initGraph = CityData.testGraph(env, hdfs + "/" + graphFile);

		int numCities = (int) initGraph.numberOfVertices();
		int numAnts = (int) (numCities * antFactor);

		Random r = new Random();
		int numIterations = numIter;

		DataSet<Tuple2<Long, List<Ant>>> ants = env.generateSequence(0, numAnts - 1)
				.map(new MapFunction<Long, Tuple2<Long, List<Ant>>>() {

					private static final long serialVersionUID = 7945306553428126095L;

					@Override
					public Tuple2<Long, List<Ant>> map(Long id) throws Exception {
						long start = (long) r.nextInt(numCities);
						List<Ant> ant = new ArrayList<Ant>();
						ant.add(new Ant(id, start));
						return new Tuple2<Long, List<Ant>>(start, ant);
					}
				}).groupBy(0).reduce(new ReduceFunction<Tuple2<Long, List<Ant>>>() {

					@Override
					public Tuple2<Long, List<Ant>> reduce(Tuple2<Long, List<Ant>> value1,
							Tuple2<Long, List<Ant>> value2) throws Exception {

						value1.f1.addAll(value2.f1);
						return value1;
					}
				});

		Graph<Long, Tuple2<List<Ant>, Boolean>, Tuple2<Integer, Double>> graphWithAnts = initGraph
				.mapVertices(new MapFunction<Vertex<Long, String>, Tuple2<List<Ant>, Boolean>>() {

					@Override
					public Tuple2<List<Ant>, Boolean> map(Vertex<Long, String> value) throws Exception {
						return new Tuple2<List<Ant>, Boolean>(new ArrayList<Ant>(), false);
					}
				}).mapEdges(new MapFunction<Edge<Long,Integer>, Tuple2<Integer, Double>>() {

					@Override
					public Tuple2<Integer, Double> map(Edge<Long, Integer> value) throws Exception {
						return new Tuple2<Integer, Double>(value.f2, 1.0);
					}
				})
				.joinWithVertices(ants, new VertexJoinFunction<Tuple2<List<Ant>, Boolean>, List<Ant>>() {

					@Override
					public Tuple2<List<Ant>, Boolean> vertexJoin(Tuple2<List<Ant>, Boolean> vertexValue,
							List<Ant> inputValue) throws Exception {

						vertexValue.f0.addAll(inputValue);

						return vertexValue;
					}

				});

		graphWithAnts.getVertices().print();

		// graph.getEdges().writeAsCsv(hdfs + "/output");

		System.out.println(env.getExecutionPlan());
		env.execute();

	}
}
