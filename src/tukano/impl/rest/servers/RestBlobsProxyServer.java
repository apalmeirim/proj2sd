package tukano.impl.rest.servers;

import org.glassfish.jersey.server.ResourceConfig;
import tukano.api.java.Blobs;
import tukano.impl.rest.servers.utils.CustomLoggingFilter;
import tukano.impl.rest.servers.utils.GenericExceptionMapper;
import utils.Args;

import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

public class RestBlobsProxyServer extends AbstractRestServer {
        public static final int PORT = 6789;

        private static Logger Log = Logger.getLogger(tukano.impl.rest.servers.RestBlobsProxyServer.class.getName());

        RestBlobsProxyServer(int port) {
            super( Log, Blobs.NAME, port);
        }


        @Override
        void registerResources(ResourceConfig config) {
            config.register( RestBlobsProxyResource.class );
            config.register(new GenericExceptionMapper());
            config.register(new CustomLoggingFilter());
        }

        public static void main(String[] args) throws NoSuchAlgorithmException {
            Args.use(args);
            new tukano.impl.rest.servers.RestBlobsProxyServer(Args.valueOf("-port", PORT)).start();
        }
}
