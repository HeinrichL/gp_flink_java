package degree_of_separation;

import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;

public class DegreeOfSeparationCombiner extends MessageCombiner<Integer, Integer> {

	public void combineMessages(MessageIterator<Integer> messages) {

		int minMessage = Integer.MAX_VALUE;
		for (Integer msg : messages) {
			minMessage = Math.min(minMessage, msg);
		}
		sendCombinedMessage(minMessage);
	}
}
