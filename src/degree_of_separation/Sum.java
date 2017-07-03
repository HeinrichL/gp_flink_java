package degree_of_separation;

import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.types.NullValue;

public class Sum extends SumFunction<Integer, NullValue, Integer>{

	@Override
	public Integer sum(Integer arg0, Integer arg1) {
		return Math.min(arg0, arg1);
	}

}
