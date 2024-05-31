package tukano.impl.java.servers;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;
import org.pac4j.scribe.builder.api.DropboxApi20;
import tukano.api.java.Result;
import tukano.impl.api.java.ExtendedBlobs;
import tukano.impl.java.clients.Clients;
import utils.Hash;
import utils.Hex;

import java.util.logging.Logger;
import java.io.File;
import java.util.function.Consumer;

import static java.lang.String.format;
import static tukano.api.java.Result.ErrorCode.*;
import static tukano.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.java.Result.error;
import static tukano.api.java.Result.ok;

public class JavaBlobsProxy implements ExtendedBlobs {

    private static Logger Log = Logger.getLogger(JavaBlobs.class.getName());

    private static final String apiKey = "ieu8jc9h6zy9cwz";
    private static final String apiSecret = "v1q9s8vdhm9r3j7";
    private static final String accessTokenStr = "sl.B2BiIlIVmr2ssaipqd0b84cPBFdjTp42dzJCKlgOW_3Bs1GPocyICQ5CT8bl0OtV-tnRuR-9SdmIifoEZQntOubFVyuQOPphIR7JrQQZ_MvSvv6-eM78WbDFsszr3FByKNHnhHL4T1jA";

    private static final String UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload";
    private static final String DOWNLOAD_URL = "https://content.dropboxapi.com/2/files/download";
    private static final String DELETE_URL ="https://api.dropboxapi.com/2/files/delete_v2";

    private final Gson json;
    private final OAuth20Service service;
    private final OAuth2AccessToken accessToken;

    public JavaBlobsProxy(){
        json = new Gson();
        accessToken = new OAuth2AccessToken(accessTokenStr);
        service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
    }

    @Override
    public Result<Void> upload(String blobId, byte[] bytes) {
        Log.info(() -> format("upload : blobId = %s, sha256 = %s\n", blobId, Hex.of(Hash.sha256(bytes))));

        if (!validBlobId(blobId))
            return error(FORBIDDEN);

        var uploadBlob = new OAuthRequest(Verb.POST, UPLOAD_URL);
        uploadBlob.addHeader("Content-Type", "application/octet-stream");
        uploadBlob.addHeader("Dropbox-API-Arg", "{\"path\":\"/" + blobId + "\",\"mode\": \"overwrite\",\"autorename\": false,\"mute\": false,\"strict_conflict\": false}");
        uploadBlob.setPayload(bytes);
        service.signRequest(accessToken, uploadBlob);
        try {
            Response response = service.execute(uploadBlob);
            if (response.getCode() == 200) {
                return ok();
            } else {
                Logger.getLogger(JavaBlobsProxy.class.getName());
                return error(INTERNAL_ERROR);
            }
        } catch (Exception e) {
            Log.severe(e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<byte[]> download(String blobId) {
        Log.info(() -> format("download : blobId = %s\n", blobId));

        var downloadBlob = new OAuthRequest(Verb.POST, DOWNLOAD_URL);
        downloadBlob.addHeader("Dropbox-API-Arg", String.format("{\"path\":\"/%s\"}",blobId));
        downloadBlob.addHeader("Content-Type", "application/octet-stream");
        service.signRequest(accessToken, downloadBlob);

        try {
            Response response = service.execute(downloadBlob);
            if (response.getCode() == 200) {
                return ok(response.getStream().readAllBytes());
            } else {
                Logger.getLogger(JavaBlobsProxy.class.getName());
                return error(INTERNAL_ERROR);
            }
        }catch (Exception e){
            Log.severe(e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> downloadToSink(String blobId, Consumer<byte[]> sink) {
        return null;
    }

    @Override
    public Result<Void> delete(String blobId, String token) {
        Log.info(() -> format("delete : blobId = %s, token=%s\n", blobId, token));

        var deleteBlob = new OAuthRequest(Verb.POST, DELETE_URL);

        deleteBlob.addHeader("Content-Type", "application/octet-stream");
        deleteBlob.addHeader("Dropbox-API-Arg", String.format("{\"path\":\"/%s\"}", blobId));
        service.signRequest(accessToken, deleteBlob);

        try {
            Response response = service.execute(deleteBlob);
            if (response.getCode() == 200) {
                return ok();
            } else {
                Logger.getLogger(JavaBlobsProxy.class.getName());
                return error(INTERNAL_ERROR);
            }
        } catch (Exception e) {
            Log.severe(e.getMessage());
            return error(INTERNAL_ERROR);
        }

    }

    @Override
    public Result<Void> deleteAllBlobs(String userId, String token) {
        return null;
    }

    private boolean validBlobId(String blobId) {
        return Clients.ShortsClients.get().getShort(blobId).isOK();
    }

    private File toFilePath(String blobId) {
        return null;
    }
}
