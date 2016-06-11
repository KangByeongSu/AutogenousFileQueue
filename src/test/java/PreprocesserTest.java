import com.skplanet.filequeue.PreprocessFileQueue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import properties.FileQueueProperties;

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
        int num = pre.findLatestFileNumber(FileQueueProperties.FILE_PATH+FileQueueProperties.PARENT_PATH+FileQueueProperties.CONSUME_LOG_PATH);
        pre.getHead();
    }

    @Ignore
    @Test
    public void testAppender() throws Exception{
        pre.findLastHeader("/tmp/autogenous_path/data/page-1.dat");
    }

}
