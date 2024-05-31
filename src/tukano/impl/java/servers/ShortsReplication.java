package tukano.impl.java.servers;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import jakarta.inject.Singleton;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import tukano.api.Short;
import tukano.api.User;
import tukano.api.java.Blobs;
import tukano.api.java.RepShorts;
import tukano.api.java.Result;
import tukano.impl.discovery.Discovery;
import tukano.impl.java.servers.data.Following;
import tukano.impl.java.servers.data.Likes;
import tukano.impl.rest.servers.kafka.KafkaPublisher;
import tukano.impl.rest.servers.kafka.KafkaSubscriber;
import tukano.impl.rest.servers.kafka.RecordProcessor;
import tukano.impl.rest.servers.kafka.SyncPoint;
import utils.DB;
import utils.Token;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static tukano.api.java.Result.*;
import static tukano.api.java.Result.ErrorCode.*;
import static tukano.impl.java.clients.Clients.BlobsClients;
import static tukano.impl.java.clients.Clients.UsersClients;
import static utils.DB.getOne;

@Singleton
public class ShortsReplication extends Thread implements RecordProcessor, RepShorts {
    private static Logger Log = Logger.getLogger(ShortsReplication.class.getName());

    private static final String BLOB_COUNT = "*";
    static final String FROM_BEGINNING = "earliest";
    static final String TOPIC = "single_partition_topic";
    static final String KAFKA_BROKERS = "kafka:9092";

    private static final long USER_CACHE_EXPIRATION = 3000;
    private static final long SHORTS_CACHE_EXPIRATION = 3000;
    private static final long BLOBS_USAGE_CACHE_EXPIRATION = 10000;

    private static final String CREATE_SHORT = "createShort";
    private static final String DELETE_SHORT = "deleteShort";
    private static final String FOLLOW = "follow";
    private static final String LIKE = "like";
    private static final String DELETE_ALL_SHORTS = "deleteAllShorts";

    final KafkaPublisher sender;
    final KafkaSubscriber receiver;
    final SyncPoint<String> sync;
    AtomicLong counter = new AtomicLong( totalShortsInDatabase() );
    @Getter
    Long version;
    Discovery discovery = Discovery.getInstance();

    public ShortsReplication() {
        this.sender = KafkaPublisher.createPublisher(KAFKA_BROKERS);
        this.sync = SyncPoint.getInstance();
        this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), FROM_BEGINNING);
        this.receiver.start(false, this);
    }


    static record Credentials(String userId, String pwd) {
        static Credentials from(String userId, String pwd) {
            return new Credentials(userId, pwd);
        }
    }

    protected final LoadingCache<Credentials, Result<User>> usersCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(USER_CACHE_EXPIRATION)).removalListener((e) -> {
            }).build(new CacheLoader<>() {
                @Override
                public Result<User> load(Credentials u) throws Exception {
                    var res = UsersClients.get().getUser(u.userId(), u.pwd());
                    if (res.error() == TIMEOUT)
                        return error(BAD_REQUEST);
                    return res;
                }
            });

    protected final LoadingCache<String, Result<Short>> shortsCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(SHORTS_CACHE_EXPIRATION)).removalListener((e) -> {
            }).build(new CacheLoader<>() {
                @Override
                public Result<Short> load(String shortId) throws Exception {
                    var query = format("SELECT count(*) FROM Likes l WHERE l.shortId = '%s'", shortId);
                    var likes = DB.sql(query, Long.class);
                    return errorOrValue( getOne(shortId, Short.class), shrt -> shrt.copyWith( likes.get(0) ) );
                }
            });

    protected final LoadingCache<String, Map<String,Long>> blobCountCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(BLOBS_USAGE_CACHE_EXPIRATION)).removalListener((e) -> {
            }).build(new CacheLoader<>() {
                @Override
                public Map<String,Long> load(String __) throws Exception {
                    final var QUERY = "SELECT REGEXP_SUBSTRING(s.blobUrl, '^(\\w+:\\/\\/)?([^\\/]+)\\/([^\\/]+)') AS baseURI, count('*') AS usage From Short s GROUP BY baseURI";
                    var hits = DB.sql(QUERY, BlobServerCount.class);

                    var candidates = hits.stream().collect( Collectors.toMap( BlobServerCount::baseURI, BlobServerCount::count));

                    for( var uri : BlobsClients.all() )
                        candidates.putIfAbsent( uri.toString(), 0L);

                    return candidates;

                }
            });

    @Override
    public void onReceive(ConsumerRecord<String, String> r) {
        version = r.offset();
        var method = r.key();
        var value = r.value().split(",");

        switch (method) {
            case CREATE_SHORT:
                _createShort(value[0], value[1], value[2], Long.parseLong(value[3]));
                break;
            case DELETE_SHORT:
                _deleteShort(value[0]);
                break;
            case FOLLOW:
                _follow(value[0], value[1], value[2]);
                break;
            case LIKE:
                _like(value[0], value[1], value[2], value[3]);
                break;
            case DELETE_ALL_SHORTS:
                _deleteAllShorts(value[0], value[1]);
                break;
        }
        sync.setResult(version, r.value());
    }


    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Result<Short> createShort(Long version, String userId, String password) {

        if(userId == null || password == null)
            return Result.error(BAD_REQUEST);

        if(!okUser(userId).isOK())
            return Result.error(NOT_FOUND);

        if(!okUser(userId, password).isOK())
            return Result.error(FORBIDDEN);

        var shortId = format("%s-%d", userId, counter.incrementAndGet());

        var servers = getLeastLoadedBlobServerURI().split("\\|");

        StringBuilder blobUrl = new StringBuilder(format("%s/%s/%s", servers[0], Blobs.NAME, shortId));

        for(int i = 1; i < servers.length; i++) {
            blobUrl.append("|");
            blobUrl.append(format("%s/%s/%s", servers[i], Blobs.NAME, shortId));
        }
        var shrt = new Short(shortId, userId, blobUrl.toString());

        String args = String.format("%s,%s,%s,%s", userId, blobUrl, shrt.getShortId(), shrt.getTimestamp());

        sender.publish(TOPIC, CREATE_SHORT, args);

        return Result.ok(shrt);

    }


    private Short _createShort(String userId, String blobUrl, String shortId, Long timestamp) {

        var shrt = new Short(shortId, userId, blobUrl);

        shrt.setTimestamp(timestamp);

        return DB.insertOne(shrt).value();

    }


    @Override
    public Result<Void> deleteShort(Long version, String shortId, String password) {

        Short s = getShort(version, shortId).value();

        if(!shortFromCache(shortId).isOK())
            return Result.error(NOT_FOUND);

        if(!okUser(s.getOwnerId(), password).isOK())
            return Result.error(FORBIDDEN);


        String args = format("%s,", shortId);
        sender.publish(TOPIC, DELETE_SHORT, args);

        return Result.ok();

    }


    private void _deleteShort(String shortId) {

        Short s = shortFromCache(shortId).value();

        DB.transaction(hibernate -> {
           shortsCache.invalidate(s.getShortId());
           hibernate.remove(s);
           var query = format("SELECT * FROM Likes l WHERE l.shortId = '%s'", s.getShortId());
           hibernate.createNativeQuery( query, Likes.class).list().forEach( hibernate::remove);
           BlobsClients.get().delete(s.getBlobUrl(), Token.get());
        });

    }

    @Override
    public Result<Short> getShort(Long version, String shortId) {

        if(shortId == null) {
            return Result.error(BAD_REQUEST);
        }

        if(!shortFromCache(shortId).isOK())
            return Result.error(NOT_FOUND);

        if(version != null) {
            sync.waitForResult(version);
        }

        alterBlobURL(discovery.knownUrisOf(Blobs.NAME, 1), shortId);

        return shortFromCache(shortId);

    }


    private void alterBlobURL(URI[] onlineServers, String shortId) {
        if (onlineServers.length > 0) {

            StringBuilder st = new StringBuilder(format("%s/%s/%s", onlineServers[0].toString(), Blobs.NAME, shortId));

            for (int i = 1; i < onlineServers.length; i++) {
                if(!(onlineServers[i] == null)) {
                    st.append("|");
                    st.append(format("%s/%s/%s", onlineServers[i].toString(), Blobs.NAME, shortId));
                }
            }

            if(shortFromCache(shortId).isOK()) {
                Short s = shortFromCache(shortId).value();
                s.setBlobUrl(st.toString());
            }
        }
    }

    @Override
    public Result<List<String>> getShorts(Long version, String userId) {

        var query = format("SELECT s.shortId FROM Short s WHERE s.ownerId = '%s'", userId);
        return errorOrValue( okUser(userId), DB.sql( query, String.class));

    }

    @Override
    public Result<Void> follow(Long version, String userId1, String userId2, boolean isFollowing, String password) {

        if(!(okUser(userId1).isOK() && okUser(userId2).isOK()))
            return Result.error(NOT_FOUND);

        if(!okUser(userId1, password).isOK())
            return Result.error(FORBIDDEN);

        var query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId2);

        var res = DB.sql(query, String.class);

        if(isFollowing && res.contains(userId1) || !isFollowing && !res.contains(userId1))
            return Result.error(CONFLICT);

        String args = format("%s,%s,%s", userId1, userId2, isFollowing);
        sender.publish(TOPIC, FOLLOW, args);

        return ok();

    }

    private void _follow(String u1, String u2, String isF) {

        var f = new Following(u1, u2);

        var r = Boolean.valueOf(isF) ? DB.insertOne(f) : DB.deleteOne(f);

    }


    @Override
    public Result<List<String>> followers(Long version, String userId, String password) {

        if(!okUser(userId).isOK())
            return Result.error(NOT_FOUND);

        if(!okUser(userId, password).isOK())
            return Result.error(FORBIDDEN);

        if(version != null)
            sync.waitForResult(version);

        var query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);

        return Result.ok(DB.sql(query, String.class));

    }

    @Override
    public Result<Void> like(Long version, String shortId, String userId, boolean isLiked, String password) {

        var shrt = getShort(version, shortId);

        if(!shrt.isOK())
            return Result.error(NOT_FOUND);

        if(version != null)
            sync.waitForResult(version);

        var query = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);
        List<String> result = DB.sql(query, String.class);

        if(result.contains(userId) && isLiked)
            return Result.error(CONFLICT);

        if(!(result.contains(userId) || isLiked))
            return Result.error(NOT_FOUND);

        String args = format("%s,%s,%s,%s", userId, shortId, shrt.value().getOwnerId(), isLiked);
        sender.publish(TOPIC, LIKE, args);

        return Result.ok();

    }

    private void _like(String userId, String shortId, String ownerId, String liked) {

        var l = new Likes(userId, shortId, ownerId);

        var r = Boolean.valueOf(liked) ? DB.insertOne(l) : DB.deleteOne(l);

    }

    @Override
    public Result<List<String>> likes(Long version, String shortId, String password) {

        var s = getShort(version, shortId);

        if(!s.isOK())
            return Result.error(NOT_FOUND);

        if(!okUser(s.value().getOwnerId(), password).isOK())
            return Result.error(FORBIDDEN);

        if(version != null) {
            sync.waitForResult(version);
        }

        var query = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);

        return Result.ok(DB.sql(query, String.class));

    }

    @Override
    public Result<List<String>> getFeed(Long version, String userId, String password) {

        if(!okUser(userId, password).isOK())
            return Result.error(FORBIDDEN);

        if(version != null)
            sync.waitForResult(version);

        final var QUERY_FMT = """
				SELECT s.shortId, s.timestamp FROM Short s WHERE	s.ownerId = '%s'				
				UNION			
				SELECT s.shortId, s.timestamp FROM Short s, Following f 
					WHERE 
						f.followee = s.ownerId AND f.follower = '%s' 
				ORDER BY s.timestamp DESC""";

        return Result.ok(DB.sql(format(QUERY_FMT, userId, userId), String.class));

    }

    @Override
    public Result<Void> deleteAllShorts(Long version, String userId, String password, String token) {

        Log.info(() -> format("deleteAllShorts : userId = %s, password = %s, token = %s\n", userId, password, token));

        if( ! Token.matches( token ) )
            return error(FORBIDDEN);

        String args = format("%s,%s", userId, password);
        sender.publish(TOPIC, DELETE_ALL_SHORTS, args);

        _deleteAllShorts(userId, password);

        return Result.ok();

    }

    private void _deleteAllShorts(String userId, String password) {

        Log.info(format("lolololSCP"));

        DB.transaction( (hibernate) -> {

            usersCache.invalidate( new Credentials(userId, password) );

            //delete shorts
            var query1 = format("SELECT * FROM Short s WHERE s.ownerId = '%s'", userId);
            hibernate.createNativeQuery(query1, Short.class).list().forEach( s -> {
                _deleteShort(s.getShortId());
            });

            //delete follows
            var query2 = format("SELECT * FROM Following f WHERE f.follower = '%s' OR f.followee = '%s'", userId, userId);
            hibernate.createNativeQuery(query2, Following.class).list().forEach( hibernate::remove );

            //delete likes
            var query3 = format("SELECT * FROM Likes l WHERE l.ownerId = '%s' OR l.userId = '%s'", userId, userId);
            hibernate.createNativeQuery(query3, Likes.class).list().forEach( l -> {
                _deleteShort(l.getShortId());
            });
        });
    }


    private long totalShortsInDatabase() {
        var hits = DB.sql("SELECT count('*') FROM Short", Long.class);
        return 1L + (hits.isEmpty() ? 0L : hits.get(0));
    }

    private List<Object> args(Object... args) {
        List<Object> argsList = Arrays.asList(args);
        return argsList;
    }

    protected Result<Short> shortFromCache( String shortId ) {
        try {
            return shortsCache.get(shortId);
        } catch (ExecutionException e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }

    protected Result<User> okUser( String userId, String pwd) {
        try {
            return usersCache.get( new Credentials(userId, pwd));
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(INTERNAL_ERROR);
        }
    }

    private Result<Void> okUser( String userId ) {
        var res = okUser( userId, "");
        if( res.error() == FORBIDDEN )
            return ok();
        else
            return error( res.error() );
    }

    private String getLeastLoadedBlobServerURI() {
        try {
            var servers = blobCountCache.get(BLOB_COUNT);

            return	servers.entrySet()
                    .stream()
                    .sorted((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.joining("|"));
        } catch( Exception x ) {
            x.printStackTrace();
        }
        return "?";
    }

    static record BlobServerCount(String baseURI, Long count) {};

}

