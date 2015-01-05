import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class CouchBaseTest {

    static String[] args = new String[] { "/src/applicationContext.xml" };

    private static final ApplicationContext context = new FileSystemXmlApplicationContext(args);
    public static final ScheduledThreadPoolExecutor scheduledExecutor = new ScheduledThreadPoolExecutor(5);

    public static void main(String[] args) throws InterruptedException {
        final CouchBaseAccess couchBaseAccess;

        couchBaseAccess = (CouchBaseAccess) context.getBean("couchBaseAccess");
        System.out.println(couchBaseAccess);
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {

            public void run() {
                // Data Buckets:voip
                // key and value following:
                // v_version_android1.1_tldb
                // {
                // "index": 0,
                // "value": "3",
                // "voption": "tldb",
                // "type": "v_version",
                // "version": "android1.1"
                // }

                List<VersionDO> list = couchBaseAccess.getListSimpleObject("v_version", VersionDO.class);
                System.out.println("size:" + list.size());
                // TODO save to local cache
            }
        }, 1L, 1L, TimeUnit.SECONDS);
        Thread.sleep(1000000);
    }
}
