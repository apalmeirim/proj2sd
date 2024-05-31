package tukano.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tukano.api.java.RepShorts;
import tukano.api.java.Shorts;
import tukano.impl.java.servers.ShortsReplication;
import tukano.impl.rest.servers.utils.CustomLoggingFilter;
import tukano.impl.rest.servers.utils.GenericExceptionMapper;
import utils.Args;


public class RestShortsRepServer extends AbstractRestServer {
    public static final int PORT = 9092;

    private static Logger Log = Logger.getLogger(RestShortsRepServer.class.getName());

    RestShortsRepServer() {
        super( Log, Shorts.NAME, PORT);
    }


    @Override
    void registerResources(ResourceConfig config) {
        config.registerInstances(new RestShortsRepResource());
    }

    public static void main(String[] args) throws Exception {
        Args.use(args);
        new RestShortsRepServer().start();
    }
}