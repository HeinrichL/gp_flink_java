package degree_of_separation;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;

public class DegreeOfSeparationComputeFunction extends ComputeFunction<Long, Integer, Long, Integer>{

	private long srcId;
	
	public DegreeOfSeparationComputeFunction(long src){
		srcId = src;
	}
	
	@Override
	public void compute(Vertex<Long, Integer> vertex, MessageIterator<Integer> messages) throws Exception {
		
		int minDegree = (vertex.getId() == srcId) ? 0 : Integer.MAX_VALUE;

		for (Integer msg : messages) {
			minDegree = Math.min(minDegree, msg);
		}

		if (minDegree != Integer.MAX_VALUE && minDegree < vertex.getValue()) {
			setNewVertexValue(minDegree);
			for (Edge<Long, Long> e : getEdges()) {
				sendMessageTo(e.getTarget(), minDegree + 1);
			}
		}
	}
}
