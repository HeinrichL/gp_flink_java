package scatter_gather;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.LongValue;

public final class MinDistanceMessenger extends ScatterFunction<LongValue, Double, Double, Double> {

	public void sendMessages(Vertex<LongValue, Double> vertex) {
		for (Edge<LongValue, Double> edge : getEdges()) {
			sendMessageTo(edge.getTarget(), vertex.getValue() + 1);
		}
	}
}
