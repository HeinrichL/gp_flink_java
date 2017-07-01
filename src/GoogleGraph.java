import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

public class GoogleGraph {
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment
				//.createRemoteEnvironment("slave1", 7661);
		.getExecutionEnvironment();

		// get input data
		Graph<Long, NullValue, NullValue> inputGraph = Graph
				.fromCsvReader("hdfs://192.168.2.121/flink/web-Google.txt", env)
				.keyType(Long.class);

		System.out.println(inputGraph.getVertices().count());
		System.out.println(inputGraph.getEdges().count());
		// execute program
		env.execute("WordCount Example");
	}
}
