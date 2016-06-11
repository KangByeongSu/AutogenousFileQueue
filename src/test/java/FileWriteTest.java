import com.skplanet.element.Header;
import com.skplanet.filequeue.PreprocessFileQueue;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by byeongsukang on 2016. 6. 11..
 */
public class FileWriteTest {
    PreprocessFileQueue pre;

    @Before
    public void init() {
        pre = new PreprocessFileQueue();

    }

    @Test
    public void testWriteBoth() throws FileNotFoundException, IOException {
        testWrite("/tmp/autogenous_path/data/page-1.dat", "{\"service_id\":\"rakeapi\",\"log\":\"강버ㅕㅇ수병수짱 병수병수 ㅋㅋㅋ\"}");
        testWrite("/tmp/autogenous_path/consume_log/consume-1.index", "null");
    }

    public void testWrite(String path, String data) throws FileNotFoundException, IOException {
        RandomAccessFile raf = new RandomAccessFile(path, "rw");
        FileChannel fileChannel = raf.getChannel();
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.put(new byte[]{/*head*/0x68, 0x65, 0x61, 0x64,/*ElementCount*/ 0x00, 0x00, 0x00, 0x01,/*startOffset*/ 0x00, 0x00,  0x00, 0x00,/*endOffset*/ 0x00, 0x00, 0x00, 0x60});
        //   ByteBuffer buf = ByteBuffer.wrap(new byte[]{'h','e','a','d',(byte)0x00,(byte)0x00,(byte)0x00,(byte)0xff,(byte)0x00,(byte)0x00,(byte)0xff,(byte)0xff});
        buf.flip();
        //  raf.seek(0);
        fileChannel.write(buf);

        if (!("null".equals(data))) {
            System.out.println(data);
            buf = ByteBuffer.allocate(data.getBytes().length);
            buf.put(data.getBytes());
            buf.flip();
            fileChannel.write(buf);
        }


        Header header = pre.getHeader("/tmp/autogenous_path/data/page-1.dat", 0);
        System.out.println("header : " + header.getHead());

        System.out.println("start offset : " + header.getStartOffset());

        System.out.println("end offset : " + header.getEndOffset());
        System.out.println("Element Count : " + header.getElementCount());
    }

}