package scatter_gather;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.types.LongValue;

//gather: vertex update
public final class VertexDistanceUpdater extends GatherFunction<LongValue, Double, Double> {

	public void updateVertex(Vertex<LongValue, Double> vertex, MessageIterator<Double> inMessages) {
			setNewVertexValue(vertex.getValue() + 1);
	}
}