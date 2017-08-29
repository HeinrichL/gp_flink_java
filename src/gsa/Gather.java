package gsa;

import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.types.NullValue;

public class Gather extends GatherFunction<Integer, NullValue, Integer>{

	@Override
	public Integer gather(Neighbor<Integer, NullValue> neighbor) {
		//if(neighbor.getNeighborValue() != Integer.MAX_VALUE){
			return neighbor.getNeighborValue() + 1;
		//}
		//return Integer.MAX_VALUE;
	}
}
