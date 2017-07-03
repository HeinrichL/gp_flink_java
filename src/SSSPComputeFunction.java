import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;

public final class SSSPComputeFunction extends ComputeFunction<Long, Tuple2<String, Long>, Tuple2<Long, Double>, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void compute(Vertex<Long, Tuple2<String, Long>> vertex, MessageIterator<Long> messages) {

		Long minDistance = (vertex.getId().equals(0L)) ? 0L : Long.MAX_VALUE;

		for (Long msg : messages) {
			minDistance = Math.min(minDistance, msg);
		}

		if (minDistance < vertex.getValue().f1) {
			setNewVertexValue(new Tuple2<String, Long>(vertex.f1.f0, minDistance));
			for (Edge<Long, Tuple2<Long, Double>> e : getEdges()) {

				e.f2.f1 = 200.0;

				sendMessageTo(e.getTarget(), minDistance + e.getValue().f0);
			}
		}
	}
}