package at.oeaw.acdh.linkchecker;

import org.apache.storm.LocalCluster;
import org.apache.storm.flux.FluxBuilder;
import org.apache.storm.flux.model.ExecutionContext;
import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;

public class LinkcheckerTest {

	public static void main(final String[] args) throws Exception {
		
		TopologyDef def = FluxParser.parseFile("crawler.flux", false, true, null, false);
		
		
		  try (LocalCluster cluster = new LocalCluster()) {
			  cluster.submitTopology(def.getName(), def.getConfig(), FluxBuilder.buildTopology(new ExecutionContext(def, FluxBuilder.buildConfig(def))));
			  Thread.sleep(10000);
			  
			  cluster.deactivate(def.getName());
			  cluster.close();
		  }
		 
		 
	}

}
