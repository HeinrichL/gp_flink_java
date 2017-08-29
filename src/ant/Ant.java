package ant;

import java.util.ArrayList;
import java.util.List;

public class Ant {
	
	private long ID;
	private long currentVertex;
	private long startVertex;
	private List<Long> tour;
	private int distance;
	private List<Long> bestTour;
	private int bestDistance;
	
	public Ant(long ID, long start){
		this.ID = ID;
		currentVertex = start;
		startVertex = start;
		
		tour = new ArrayList<Long>();
		tour.add(start);
		distance = 0;
		
		bestTour = new ArrayList<Long>();
		bestDistance = 0;
	}
	
	public long getID() {
		return ID;
	}
	public void setID(long iD) {
		ID = iD;
	}
	
	public long getStartVertex() {
		return startVertex;
	}
	public void setStartVertex(long startVertex) {
		this.startVertex = startVertex;
	}
	
	public long getCurrentVertex() {
		return currentVertex;
	}
	public void setCurrentVertex(long currentVertex) {
		this.currentVertex = currentVertex;
	}
	public List<Long> getTour() {
		return tour;
	}
	public void setTour(List<Long> tour) {
		this.tour = tour;
	}
	
	public void addCityToTour(long city){
		tour.add(city);
	}
	
	public int getDistance() {
		return distance;
	}
	public void setDistance(int distance) {
		this.distance = distance;
	}
	
	public void addDistance(int dist){
		this.distance += dist;
	}
	
	public List<Long> getBestTour() {
		return bestTour;
	}
	public void setBestTour(List<Long> bestTour) {
		this.bestTour = bestTour;
	}
	public int getBestDistance() {
		return bestDistance;
	}
	public void setBestDistance(int bestDistance) {
		this.bestDistance = bestDistance;
	}
	
	@Override
	public String toString(){
		return "{ ID:" + ID + ", curr: " + currentVertex + ", start: " + startVertex  + ", tour: (" + tour + "), length:" + distance + ")}";
	}
}
