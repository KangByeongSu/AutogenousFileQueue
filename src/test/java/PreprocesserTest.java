import com.skplanet.filequeue.PreprocessFileQueue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import com.skplanet.properties.FileQueueProperties;

/**
 * Created by byeongsukang on 2016. 6. 6..
 */
public class PreprocesserTest {
    PreprocessFileQueue pre;

    @Before
    public void init() {
        pre = new PreprocessFileQueue();

    }

    @Test
    public void testSplitter() {
       // int num = pre.findLastFileNumber(FileQueueProperties.FILE_PATH+FileQueueProperties.PARENT_PATH+FileQueueProperties.CONSUME_LOG_PATH);
        System.out.println("getHead()"+pre.getHead());
        System.out.println("getTail()"+pre.getTail());
    }

    @Ignore
    @Test
    public void testAppender() throws Exception{
        pre.findLatestHeader("/tmp/autogenous_path/data/page-1.dat");
    }

}
