package ant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

public class GermanCitiesAnt {

	static final int Q = 500;
	static final int alpha = -1;
	static final int beta = 5;
	static final double evaporation = 0.5;
	static final double antFactor = 0.8;

	static final double randomCityProp = 0.01;

	//@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment
				// .createRemoteEnvironment("slave1", 7661);
				.getExecutionEnvironment();

		// Graph<Long, String, Long> parsed = Graph
		// .fromCsvReader("C:\\Users\\Heinrich\\Desktop\\vertices-german.txt",
		// "C:\\Users\\Heinrich\\Desktop\\edges-german.txt", env)
		// .types(Long.class, String.class, Long.class);
		// System.out.println(parsed.getVertices().collect());
		// System.out.println(parsed.getEdges().collect());
		
		String hdfs = args[0];
		String graphFile = args[1];
		int numIter = Integer.parseInt(args[2]);

		Graph<Long, String, Integer> initGraph = CityData.testGraph(env, hdfs + "/" + graphFile);

		initGraph.getEdges().print();

		int numCities = (int) initGraph.numberOfVertices();
		int numAnts = (int) (numCities * antFactor);

		Graph<Long, String, Tuple3<Integer, Double, Integer[]>> graph = initGraph
				.mapEdges(new MapFunction<Edge<Long, Integer>, Tuple3<Integer, Double, Integer[]>>() {

					@Override
					public Tuple3<Integer, Double, Integer[]> map(Edge<Long, Integer> arg0) throws Exception {
						Integer[] vector = new Integer[numAnts];
						Arrays.fill(vector, 0);
						return new Tuple3(arg0.f2, 1.0, vector);
					}
				});

		Random r = new Random();
		int numIterations = numIter;
		
		DataSet<Tuple2<Long, Ant>> ants = env.generateSequence(0, numAnts - 1)
				.map(new MapFunction<Long, Tuple2<Long, Ant>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 7945306553428126095L;

					@Override
					public Tuple2<Long, Ant> map(Long id) throws Exception {
						long start = (long) r.nextInt(numCities);
						return new Tuple2<Long, Ant>(start, new Ant(id, start));
					}
				});

		

		for (int i = 0; i < numIterations; i++) {
			for (int j = 0; j < numCities; j++) {
			
			Graph local = graph;

			DataSet<Tuple2<Long, List<Edge<Long, Tuple3<Integer, Double, Integer[]>>>>> vIncidentEdges = local
					.groupReduceOnEdges(
							new EdgesFunctionWithVertexValue<Long, String, Tuple3<Integer, Double, Integer[]>, Tuple2<Long, List<Edge<Long, Tuple3<Integer, Double, Integer[]>>>>>() {

								@Override
								public void iterateEdges(Vertex<Long, String> vertex,
										Iterable<Edge<Long, Tuple3<Integer, Double, Integer[]>>> edges,
										Collector<Tuple2<Long, List<Edge<Long, Tuple3<Integer, Double, Integer[]>>>>> out)
										throws Exception {

									List<Edge<Long, Tuple3<Integer, Double, Integer[]>>> res = new ArrayList<Edge<Long, Tuple3<Integer, Double, Integer[]>>>();

									for (Edge<Long, Tuple3<Integer, Double, Integer[]>> e : edges) {
										res.add(e);
									}

									out.collect(new Tuple2(vertex.f0, res));

								}

							}, EdgeDirection.ALL);
			
			//vIncidentEdges.print();
			//ants.print();
			
			DataSet<Tuple2<Tuple2<Long, Edge<Long, Tuple3<Integer, Double, Integer[]>>>, Tuple2<Long, Ant>>> vAntsWithSelectedEdge = vIncidentEdges
					.join(ants).where(0).equalTo(0)
					.map(new MapFunction<Tuple2<Tuple2<Long, List<Edge<Long, Tuple3<Integer, Double, Integer[]>>>>, Tuple2<Long, Ant>>, Tuple2<Tuple2<Long, Edge<Long, Tuple3<Integer, Double, Integer[]>>>, Tuple2<Long, Ant>>>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = -4207862817056906122L;

						@Override
						public Tuple2<Tuple2<Long, Edge<Long, Tuple3<Integer, Double, Integer[]>>>, Tuple2<Long, Ant>> map(
								Tuple2<Tuple2<Long, List<Edge<Long, Tuple3<Integer, Double, Integer[]>>>>, Tuple2<Long, Ant>> arg0)
								throws Exception {

							Ant a = arg0.f1.f1;

							List<Edge<Long, Tuple3<Integer, Double, Integer[]>>> incidentEdges = arg0.f0.f1;
							List<Long> visitedCities = a.getTour();

							double prob = 0.00;

							long currentCity = a.getCurrentVertex();
							long startCity = a.getStartVertex();

							// distance, pheromone, prob
							List<Edge<Long, Tuple3<Integer, Double, Integer[]>>> allowedEdges = new ArrayList<Edge<Long, Tuple3<Integer, Double, Integer[]>>>();

							// calculate sum of probabilities
							for (Edge<Long, Tuple3<Integer, Double, Integer[]>> e : incidentEdges) {
								long otherVertex;
								if (e.f0 == currentCity) {
									otherVertex = e.f1;
								} else {
									otherVertex = e.f0;
								}
								if (!visitedCities.contains(otherVertex)
										|| visitedCities.size() == numCities && otherVertex == startCity) {
									prob += Math.pow(e.f2.f0, alpha) * Math.pow(e.f2.f1, beta);

									allowedEdges.add(e);
								}
							}

							double bestProb = 0.00;
							Edge<Long, Tuple3<Integer, Double, Integer[]>> bestEdge = null;

							// select edge with highest prob
							for (Edge<Long, Tuple3<Integer, Double, Integer[]>> allowedEdge : allowedEdges) {
								double relProb = Math.pow(allowedEdge.f2.f0, alpha) * Math.pow(allowedEdge.f2.f1, beta)
										/ prob;

								if (bestEdge == null || relProb > bestProb) {
									bestEdge = allowedEdge;
									bestProb = relProb;
								}
							}

							long dst;

							if (bestEdge.f0 == a.getCurrentVertex())
								dst = bestEdge.f1;
							else
								dst = bestEdge.f0;

							a.addCityToTour(dst);
							a.setCurrentVertex(dst);
							a.addDistance(bestEdge.f2.f0);

							return new Tuple2<Tuple2<Long, Edge<Long, Tuple3<Integer, Double, Integer[]>>>, Tuple2<Long, Ant>>(
									new Tuple2<Long, Edge<Long, Tuple3<Integer, Double, Integer[]>>>(arg0.f0.f0,
											bestEdge),
									new Tuple2(arg0.f1.f0, a));
						}
					});

			// vAntsWithSelectedEdge.print();

			// group ants by traversed edge
			DataSet selectedEdge = vAntsWithSelectedEdge.map(
					new MapFunction<Tuple2<Tuple2<Long, Edge<Long, Tuple3<Integer, Double, Integer[]>>>, Tuple2<Long, Ant>>, Tuple3<Long, Long, List<Ant>>>() {

						@Override
						public Tuple3<Long, Long, List<Ant>> map(
								Tuple2<Tuple2<Long, Edge<Long, Tuple3<Integer, Double, Integer[]>>>, Tuple2<Long, Ant>> value)
								throws Exception {

							return new Tuple3(value.f0.f1.f0, value.f0.f1.f1,
									new ArrayList<Ant>(Arrays.asList(value.f1.f1)));
						}
					}).groupBy(0, 1).reduce(new ReduceFunction<Tuple3<Long, Long, List<Ant>>>() {

						@Override
						public Tuple3<Long, Long, List<Ant>> reduce(Tuple3<Long, Long, List<Ant>> value1,
								Tuple3<Long, Long, List<Ant>> value2) throws Exception {

							value1.f2.addAll(value2.f2);
							return new Tuple3(value1.f0, value1.f1, value1.f2);

						}
					});

			// selectedEdge.print();
			
			graph = local.joinWithEdges(selectedEdge,
					new EdgeJoinFunction<Tuple3<Integer, Double, Integer[]>, List<Ant>>() {

						@Override
						public Tuple3<Integer, Double, Integer[]> edgeJoin(Tuple3<Integer, Double, Integer[]> edgeValue,
								List<Ant> inputValue) throws Exception {

							Integer[] vector = edgeValue.f2;

							for (Ant a : inputValue)
								vector[(int) a.getID()] = 1;

							return new Tuple3(edgeValue.f0, edgeValue.f1, vector);
						}
					});

			// graph.getEdges().print();

			ants = vAntsWithSelectedEdge.map(
					new MapFunction<Tuple2<Tuple2<Long, Edge<Long, Tuple3<Integer, Double, Integer[]>>>, Tuple2<Long, Ant>>, Tuple2<Long, Ant>>() {

						@Override
						public Tuple2<Long, Ant> map(
								Tuple2<Tuple2<Long, Edge<Long, Tuple3<Integer, Double, Integer[]>>>, Tuple2<Long, Ant>> value)
								throws Exception {

							return new Tuple2(value.f1.f1.getCurrentVertex(), value.f1.f1);
						}
					});

			// vIncidentEdgesWithProbabilities.print();
			}
		}

		//graph.getEdges().print();
		//ants.print();
		
		
		
		ants.writeAsCsv(hdfs + "/outputg");
		graph.getEdges().writeAsCsv(hdfs + "/output");
		
		System.out.println(env.getExecutionPlan());
		env.execute();

		// joinedWithAnts.print();// .collect().forEach(ss ->
		// System.out.println(ss));

	}
}
