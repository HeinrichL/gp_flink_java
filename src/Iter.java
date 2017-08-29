import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class Iter {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Create initial IterativeDataSet
		IterativeDataSet<Integer> initial = env.fromElements(0,1,2,3,4,5,6,7,8,9,100,101,102,103,104,105,106,107,108,109).iterate(10000);

		DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
		    @Override
		    public Integer map(Integer i) throws Exception {

		        return i + 1;
		    }
		});

		// Iteratively transform the IterativeDataSet
		DataSet<Integer> count = initial.closeWith(iteration);

		count.map(new MapFunction<Integer, Double>() {
		    @Override
		    public Double map(Integer count) throws Exception {
		        return count / (double) 10000 * 4;
		    }
		}).print();
		
		count.writeAsText("ooo" + System.currentTimeMillis()                                                                                                                                                                                                                );

		env.execute("Iterative Pi Example");

	}

}
