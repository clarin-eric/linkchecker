package eu.clarin.linkchecker;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.storm.LocalCluster;
import org.apache.storm.flux.FluxBuilder;
import org.apache.storm.flux.model.ExecutionContext;
import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.Utils;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class LinkcheckerTestApp {

   public static void main(final String[] args) throws Exception {
      
      ClientAndServer cas = startClientAndServer(8181);
      
      new MockServerClient("localhost", 8181)
         .when(
            request()
         ).respond(
            response().withStatusCode(200)   
         );
      

      TopologyDef def = FluxParser.parseResource("/linkchecker-test.flux", false, true, null, true);

      try (LocalCluster cluster = new LocalCluster()) {
         
         cluster.submitTopology(def.getName(), def.getConfig(),
               FluxBuilder.buildTopology(new ExecutionContext(def, FluxBuilder.buildConfig(def))));

         Utils.sleep(1800000);
         

         // kill the topology
         final KillOptions killOptions = new KillOptions();
         killOptions.set_wait_secs(0);
         cluster.killTopologyWithOpts(def.getName(), killOptions);
         while (topologyExists(cluster, def.getName())) {
            System.out.println("waiting for topology");
            Utils.sleep(1000);
         }
         
         cluster.shutdown();
         
      }
       
      cas.close();
      System.exit(0);
   }

   private static final boolean topologyExists(LocalCluster cluster, final String topologyName) throws TException {

      // list all the topologies on the local cluster
      final List<TopologySummary> topologies = cluster.getClusterInfo().get_topologies();

      // search for a topology with the topologyName
      if (null != topologies && !topologies.isEmpty()) {
         final List<TopologySummary> collect = topologies.stream().filter(p -> p.get_name().equals(topologyName))
               .collect(Collectors.toList());
         if (null != collect && !collect.isEmpty()) {
            return true;
         }
      }
      return false;
   }
}
