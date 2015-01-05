

import java.util.List;
import java.util.Map;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonObject;



public interface CouchBaseAccess {
    public final static String DEFAULT_BUCKET_NAME = "voip";

    public final static String TALBE_KEY = "type";
    
    public Bucket getBucket(String name);

    public <T> T getSimpleObject(String id, Class<T> clazz);

    public boolean replaceSimpleObject(String id, Object object);

    boolean replaceSimpleMap(String id, Map<String, Object> map);

    JsonObject getVoipjsonByKey(String id);

    <T> List<T> getListSimpleObject(String type, Class<T> clazz);

}
