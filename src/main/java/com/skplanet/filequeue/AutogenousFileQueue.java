package com.skplanet.filequeue;

import com.skplanet.element.Header;
import com.skplanet.properties.FileQueueProperties;
import com.skplanet.utils.Calculator;
import com.skplanet.utils.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by byeongsukang on 2016. 6. 6..
 */
public class AutogenousFileQueue implements FileQueue {
    private static AutogenousFileQueue autogenousFileQueue;
    private final static int HEAD_LENGTH = 16;
    private Header writeHeader;
    private Header readHeader;
    private int headOffset;
    private int tailOffset;
    private int elementCount;
    private int currentDataPageIndex;
    private int currentConsumeLogPageIndex;
    private RandomAccessFile dataFileRaf;
    private RandomAccessFile consumeLogFileRaf;
    private FileChannel dataFileChannel;
    private FileChannel consumeLogFileChannel;
    private ByteBuffer ioByteBuffer;
    private ByteBuffer consumeByteBuffer;

    public AutogenousFileQueue() {
        try {
            PreprocessFileQueue pre = new PreprocessFileQueue();
            this.writeHeader = pre.getHead();
            this.headOffset = writeHeader.getEndOffset(); //받은곳부터 쓰면된다 오버라이트 확인
            this.elementCount = writeHeader.getElementCount();
            this.readHeader = pre.getTail(); // 받은 곳부터 헤더 뽑아서 읽으면 된다
            this.tailOffset = readHeader.getEndOffset();
            String filePath = FileQueueProperties.FILE_PATH;
            String parentPath = filePath + FileQueueProperties.PARENT_PATH;
            String dataPath = parentPath + FileQueueProperties.DATA_PATH;
            String consumeLogPath = parentPath + FileQueueProperties.CONSUME_LOG_PATH;
            currentDataPageIndex = pre.findLastFileNumber(dataPath);
            currentConsumeLogPageIndex = pre.findLastFileNumber(consumeLogPath);
            dataFileRaf = new RandomAccessFile(dataPath + FileQueueProperties.DATA_FILE_NAME + currentDataPageIndex + FileQueueProperties.DATA_FILE_SUFFIX, "rw");
            consumeLogFileRaf = new RandomAccessFile(consumeLogPath + FileQueueProperties.CONSUME_FILE_NAME + currentConsumeLogPageIndex + FileQueueProperties.CONSUME_FILE_SUFFIX, "rw");
            dataFileChannel = dataFileRaf.getChannel();
            consumeLogFileChannel = consumeLogFileRaf.getChannel();
            ioByteBuffer = ByteBuffer.allocateDirect(1024);
            consumeByteBuffer = ByteBuffer.allocateDirect(16);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        //헤더 읽기
        // head, tail복구
        //properties path
        //NIO
    }

    AutogenousFileQueue(Properties properties) {

    }


    public static synchronized AutogenousFileQueue getInstance() {

        if (autogenousFileQueue == null) {
            autogenousFileQueue = new AutogenousFileQueue();
        }
        return autogenousFileQueue;
    }


    public byte[] getByteLog(int elementNum, int start, int end, String log) {

        byte[] fullData = new byte[1024];
        byte[] head = {/*head*/0x68, 0x65, 0x61, 0x64};
        byte[] elementCounts = Calculator.convertBytes(elementNum);
        byte[] startOffset = Calculator.convertBytes(start);
        byte[] endOffset = Calculator.convertBytes(end);
        byte[] data = log.getBytes();
        System.arraycopy(head, 0, fullData, 0, head.length);
        System.arraycopy(elementCounts, 0, fullData, 4, elementCounts.length);
        System.arraycopy(startOffset, 0, fullData, 8, startOffset.length);
        System.arraycopy(endOffset, 0, fullData, 12, endOffset.length);
        System.arraycopy(data, 0, fullData, 16, data.length);
        return fullData;
    }
    public byte[] getByteLog(Header header) {

        byte[] headerData = new byte[16];
        byte[] head = {/*head*/0x68, 0x65, 0x61, 0x64};
        byte[] elementCounts = Calculator.convertBytes(header.getElementCount());
        byte[] startOffset = Calculator.convertBytes(header.getStartOffset());
        byte[] endOffset = Calculator.convertBytes(header.getEndOffset());
        System.arraycopy(head, 0, headerData, 0, head.length);
        System.arraycopy(elementCounts, 0, headerData, 4, elementCounts.length);
        System.arraycopy(startOffset, 0, headerData, 8, startOffset.length);
        System.arraycopy(endOffset, 0, headerData, 12, endOffset.length);
        return headerData;
    }

    /**
     * 맥스 사이즈 잡고 맥스사이즈 넘어가면 파일 인덱스 하나 늘려서 재 생성후 계속 집어넣는거로 바꿔야한다
     * @param log
     */
    public void add(String log) {
        try {
            int startOffset = headOffset;
            int endOffset = startOffset + HEAD_LENGTH + log.length();
            System.out.println("write elementCount : "+elementCount);
            byte[] data = getByteLog(++elementCount, startOffset, endOffset, log);
            String byteToString = new String(data, 0, data.length);
            System.out.println("write log : "+byteToString);
            ioByteBuffer.clear();
            ioByteBuffer.put(data);
            ioByteBuffer.flip();
            ioByteBuffer.limit(16 + log.length());  //1024로 할당했는데 어디까지 읽을지. 그럼 최대 길이가 몇인지를 정해줘야하는데..
            dataFileChannel.position(startOffset);
            dataFileChannel.write(ioByteBuffer);
            headOffset = endOffset; //lock걸아야겠죠  ??
            //offset, count모두 수정
            //그냥 쓰면 된다

        } catch (IOException e) {

        }
    }

    public void offer() {

    }

    public void remove() {
        try {
            //data파일에서 다음 읽어야할 친구의 헤더이다.
            Header header = Utils.getHeader(dataFileChannel, tailOffset);
            int startOffset = header.getStartOffset()+16;
            int endOffset = header.getEndOffset();
            System.out.println("start : "+startOffset+" end : "+endOffset);
            if(elementCount < 0 || endOffset>headOffset || endOffset==0){
                System.out.println("Queue is Empty");
                System.out.println("elementCOunt : "+elementCount);
                System.out.println("header : "+header.getElementCount());

                return;
            }
            ioByteBuffer.clear();
            //풀사이즈랑 나누기 연산해서 몇번째 인덱스의 데이터 파일에서 읽어야할지 정해야한다.
            dataFileChannel.position(startOffset);
            ioByteBuffer.limit(endOffset-startOffset);

            byte[] temp = new byte[endOffset-startOffset];
            dataFileChannel.read(ioByteBuffer);
            ioByteBuffer.rewind();
            ioByteBuffer.get(temp);

            String a =new String(temp,0,temp.length);
            System.out.println("read result : "+a);

            consumeByteBuffer.clear();
            consumeByteBuffer.put(getByteLog(header));
            consumeByteBuffer.flip();
            consumeLogFileChannel.position(consumeLogFileChannel.size());
            consumeLogFileChannel.write(consumeByteBuffer);

            elementCount--;
            tailOffset=endOffset;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void poll() {

    }

    public void element() {

    }

    public void peek() {

    }

    public void close() {
        try {
            dataFileRaf.close();
            consumeLogFileRaf.close();
            dataFileChannel.close();
            consumeLogFileChannel.close();
            autogenousFileQueue=null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
