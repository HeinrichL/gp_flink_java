package ant;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

public class TSPAntScatter extends
		ScatterFunction<Long, Tuple2<List<Ant>, Boolean>, Tuple3<List<Ant>, Tuple2<Long, Double>, Boolean>, Tuple2<Integer, Double>> {

	@Override
	public void sendMessages(Vertex<Long, Tuple2<List<Ant>, Boolean>> vertex) throws Exception {
		boolean sendAnts = vertex.f1.f1;

		if (sendAnts) {
			for (Ant a : vertex.f1.f0) {
				// sendMessageTo(a.getCurrentVertex(), new Tuple3());
			}
		} else {
			for(Edge<Long, Tuple2<Integer, Double>> e : getEdges()){
				long dest = vertex.f0 == e.getSource() ? e.getTarget() : e.getSource();
				sendMessageTo(dest, new Tuple3<List<Ant>, Tuple2<Long, Double>, Boolean>(new ArrayList<>(), 
						new Tuple2<Long, Double>()));
			}
		}

	}

}
