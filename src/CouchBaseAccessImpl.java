import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryRow;

public class CouchBaseAccessImpl implements CouchBaseAccess, InitializingBean {

    private final Log logger = LogFactory.getLog(getClass());
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static MappingJsonFactory jsonFactory = new MappingJsonFactory();

    private String servers;
    private List<String> serversList;
    private static Cluster cluster;
    private static Bucket cacheBucket;
    private long recentReconnectTime;
    private int reconnectWaitTime = 5000;

    public void setServers(String servers) {
        this.servers = servers;
    }

    public void setReconnectWaitTime(int reconnectWaitTime) {
        this.reconnectWaitTime = reconnectWaitTime;
    }

    @Override
    public JsonObject getVoipjsonByKey(String id) {
        JsonDocument jd = null;
        try {
            Bucket bucket = getBucket(DEFAULT_BUCKET_NAME);
            if (bucket != null) jd = bucket.get(id);
        } catch (Exception e) {
            logger.error("getVoipjsonByKey:" + id + " - " + DEFAULT_BUCKET_NAME, e);
            initCluster();
        }
        if (jd == null) return null;

        return jd.content();
    }

    @Override
    public <T> T getSimpleObject(String id, Class<T> clazz) {
        JsonDocument jd = null;
        try {
            Bucket bucket = getBucket(DEFAULT_BUCKET_NAME);
            if (bucket != null) jd = bucket.get(id);
        } catch (Exception e) {
            logger.error("getSimpleObject:" + id + " - " + clazz + " - " + DEFAULT_BUCKET_NAME, e);
            initCluster();
        }
        if (jd == null) return null;

        Map<String, Object> map = getClassAttributeMap(clazz, jd.content());

        T t = json2Object(map2Json(map), clazz);
        return t;
    }

    public static <T> T json2Object(String json, Class<T> clazz) {
        if (StringUtils.isBlank(json)) return null;
        T t = null;
        try {
            t = (T) objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return t;
    }

    public static String map2Json(Map<String, ? extends Object> map) {
        String result = null;
        if (map != null) {
            try {
                StringWriter writer = new StringWriter();
                objectMapper.writeValue(writer, map);
                result = writer.toString();
            } catch (Exception e) {
            }
        }
        return result;
    }

    @Override
    public <T> List<T> getListSimpleObject(String type, Class<T> clazz) {
        List<T> tlist = null;
        try {
            Bucket bucket = getBucket(DEFAULT_BUCKET_NAME);

            long s = System.currentTimeMillis();
            QueryResult query = bucket.query("select * from voip where type = '" + type + "'");
            if (logger.isDebugEnabled()) {
                logger.debug("getListSimpleObject " + type + " from couchbase  use time = " + (System.currentTimeMillis() - s) + "ms "
                             + query.success());
            }
            if (query.success()) {
                List<QueryRow> list = query.allRows();
                tlist = new ArrayList<T>();
                for (QueryRow queryRow : list) {
                    try {
                        if (logger.isDebugEnabled()) {
                            logger.debug("getListSimpleObject load " + type + " row :" + queryRow.toString());
                        }
                        Map<String, Object> ip = getClassAttributeMap(clazz, queryRow.value());
                        T t = json2Object(map2Json(ip), clazz);
                        tlist.add(t);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            logger.error("getListSimpleObject:" + type + " - " + clazz + " - " + DEFAULT_BUCKET_NAME, e);
            initCluster();
        }
        return tlist;
    }

    private <T> Map<String, Object> getClassAttributeMap(Class<T> clazz, JsonObject jd) {
        Map<String, Object> map = jd.toMap();

        Field[] fields = clazz.getDeclaredFields();
        Set<String> fieldSet = new HashSet<String>();
        for (Field s : fields) {
            fieldSet.add(s.getName());
        }
        Iterator<String> it = map.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            if (!fieldSet.contains(key)) {
                it.remove();
            }
        }
        return map;
    }

    @Override
    public boolean replaceSimpleObject(String id, Object object) {
        JsonObject jo = object2JsonObject(object);

        JsonDocument doc = JsonDocument.create(id, jo);
        try {
            Bucket bucket = getBucket(DEFAULT_BUCKET_NAME);
            if (bucket != null) {
                JsonDocument response = bucket.upsert(doc);
                if (logger.isDebugEnabled()) {
                    logger.debug("replaceSimpleObject response :" + response.toString());
                }
                return true;
            }
        } catch (Exception e) {
            logger.error("replaceSimpleObject error:" + id + " - " + object.toString() + " - " + DEFAULT_BUCKET_NAME, e);
            initCluster();
        }

        return false;
    }

    @Override
    public boolean replaceSimpleMap(String id, Map<String, Object> map) {
        JsonObject jo = map2JsonObject(map);

        JsonDocument doc = JsonDocument.create(id, jo);
        try {
            Bucket bucket = getBucket(DEFAULT_BUCKET_NAME);
            if (bucket != null) {
                JsonDocument response = bucket.upsert(doc);
                if (logger.isDebugEnabled()) {
                    logger.debug("replaceSimpleObject response :" + response.toString());
                }
                return true;
            }
        } catch (Exception e) {
            logger.error("replaceSimpleObject error:" + id + " - " + jo.toString() + " - " + DEFAULT_BUCKET_NAME, e);
            initCluster();
        }

        return false;
    }

    @Override
    public Bucket getBucket(String name) {
        // Bucket bucket = bucketCache.get(name);
        // if (null == bucket) {
        try {
            if (cacheBucket == null) {
                cacheBucket = cluster.openBucket(name);
            }
            return cacheBucket;
        } catch (Exception e) {
            logger.error("open Bucket:" + name, e);
            initCluster();
            try {
                if (cacheBucket == null) {
                    cacheBucket = cluster.openBucket(name);
                }
                return cacheBucket;
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return null;
    }

    private void initCluster() {
        long delayTime = System.currentTimeMillis() - recentReconnectTime;
        if (delayTime < reconnectWaitTime) {
            logger.info("re initCluster NOT TIME:" + delayTime + "ms wait time=" + reconnectWaitTime);
            return;
        }

        if (cacheBucket != null) {
            cacheBucket.close();
            cacheBucket = null;
        }
        if (cluster != null) {
            cluster.disconnect();
            cluster = null;
        }
        cluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.builder().queryEnabled(true).build(), serversList);
        cacheBucket = cluster.openBucket("voip");

        recentReconnectTime = System.currentTimeMillis();
        logger.info("re init Cluster complete");
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        Assert.hasText(servers, "couchbase servers must not be empty!");
        String[] serverArr = servers.split(",");
        serversList = Arrays.asList(serverArr);
        cluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.builder().queryEnabled(true).build(), serversList);
        recentReconnectTime = System.currentTimeMillis();
        logger.info("init Cluster complete");
    }

    private static JsonObject object2JsonObject(Object object) {
        Map<String, Object> map = object2map(object);
        return map2JsonObject(map);
    }

    public static Map<String, Object> json2map(String json) {
        Map<String, Object> mapper = null;
        try {
            mapper = objectMapper.readValue(json, Map.class);
        } catch (Exception e) {
            return null;
        }
        return mapper;
    }

    public static Map<String, Object> object2map(Object object) {
        Map<String, Object> mapper = null;
        try {
            mapper = json2map(object2json(object));
        } catch (Exception e) {
        }

        Class<?> clazz = object.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Set<String> fieldSet = new HashSet<String>();
        for (Field s : fields) {
            fieldSet.add(s.getName());
        }

        JSONObject jo;
        try {
            Iterator it = mapper.keySet().iterator();
            while (it.hasNext()) {
                String key = (String) it.next();
                if (!fieldSet.contains(key)) {
                    it.remove();
                }
            }
            return mapper;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mapper;
    }

    public static String object2json(Object object) {
        Class<?> clazz = object.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Set<String> fieldSet = new HashSet<String>();
        for (Field s : fields) {
            fieldSet.add(s.getName());
        }

        StringWriter writer = new StringWriter();
        JsonGenerator jsonGenerator = null;
        try {
            jsonGenerator = jsonFactory.createJsonGenerator(writer);
        } catch (IOException e) {
            throw new RuntimeException("json factory init error", e);
        }
        try {
            objectMapper.writeValue(jsonGenerator, object);
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException("object wapped by json error", e);
        }
        String js = writer.getBuffer().toString();
        JSONObject jo;
        try {
            jo = new JSONObject(js);
            Iterator it = jo.keys();
            while (it.hasNext()) {
                String key = (String) it.next();
                if (!fieldSet.contains(key)) {
                    it.remove();
                }
            }
            return jo.toString();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return js;

    }

    private static JsonObject map2JsonObject(Map<String, Object> map) {
        JsonObject recordjo = JsonObject.create();

        for (String k : map.keySet()) {
            Object v = map.get(k);
            if (v == null) {
                v = "";
            }
            recordjo.put(k, v);
        }
        return recordjo;
    }
}
