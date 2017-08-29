package ant;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

public class TSPAntGather extends GatherFunction<Long, Tuple2<List<Ant>, Boolean>, Tuple3<List<Ant>, Tuple2<Long, Double>, Boolean>>{

	@Override
	public void updateVertex(Vertex<Long, Tuple2<List<Ant>, Boolean>> vertex,
			MessageIterator<Tuple3<List<Ant>, Tuple2<Long, Double>, Boolean>> inMessages) throws Exception {
		
		
		
	}

}
