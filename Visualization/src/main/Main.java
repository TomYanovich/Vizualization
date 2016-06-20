package main;
import java.io.IOException;
import java.util.List;

public class Main {
	
	private static final int NUM_OF_EPISODES = 3;
	
	public static void main(String[] args) throws IOException {
		
		List<String> longestEpisodes = WordMatrix.getKlongestEpisodes(NUM_OF_EPISODES);
		for (String episode_id : longestEpisodes){
			WordMatrix.init(episode_id).fill().exportTSV();
		}
	}
}
