package degree_of_separation;

import org.apache.flink.graph.gsa.ApplyFunction;

public class Apply extends ApplyFunction<Integer, Integer, Integer> {

//	private long srcId;
//
//	public Apply(long src){
//		srcId = src;
//	}

	@Override
	public void apply(Integer newValue, Integer currentValue) {
		if(newValue < currentValue){
			setResult(newValue);
		}
	}

}
