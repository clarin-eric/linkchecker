package at.oeaw.acdh.stormchecker;

import org.apache.storm.LocalCluster;

public class LinkcheckerTest {

	public static void main(String[] args) throws Exception {
		
		LocalCluster.withLocalModeOverride(() -> {
			org.apache.storm.flux.Flux.main(args); 
			return null;
		}, 100l);
	}

}
