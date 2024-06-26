package tukano.impl.grpc.servers;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.logging.Logger;

import tukano.api.java.Users;
import utils.Args;

public class GrpcUsersServer extends AbstractGrpcServer {
public static final int PORT = 13456;
	
	private static Logger Log = Logger.getLogger(GrpcUsersServer.class.getName());

	public GrpcUsersServer()
			throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
		super( Log, Users.NAME, PORT, new GrpcUsersServerStub());
	}
	
	public static void main(String[] args) {
		try {
			Args.use(args);
			new GrpcUsersServer().start();
		} catch (KeyStoreException | IOException | NoSuchAlgorithmException |
				 CertificateException | UnrecoverableKeyException e) {
			e.printStackTrace();
		}
	}	
}
