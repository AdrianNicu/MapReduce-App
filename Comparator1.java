import java.util.Comparator;


public class Comparator1 implements Comparator<Pair<String,Double>> {

	
	public int compare(Pair<String,Double> p1, Pair<String,Double> p2) {
		if (p1.second > p2.second) return -1024;
		if (p1.second < p2.second) return 1024;
		if (ReplicatedWorkers.fNames.indexOf(p1.first) > ReplicatedWorkers.fNames.indexOf(p1.second)) return 1024;
		if (ReplicatedWorkers.fNames.indexOf(p1.first) < ReplicatedWorkers.fNames.indexOf(p1.second)) return -1024;
		return 0;
	}
}
