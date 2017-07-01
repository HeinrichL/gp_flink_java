import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.java.*;
import org.apache.flink.api.java.aggregation.Aggregations;

public class FirstExample {
	public static void main(String[] args){
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		try {
			DataSet<String> input =
			    env.readTextFile("hdfs://192.168.2.121/spark/input.txt");
			
			DataSet<Tuple2<String, Integer>> counts = input
					.flatMap(new LineSplitter())
					.groupBy(0)
					.aggregate(Aggregations.SUM, 1);
			
			counts.print();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
