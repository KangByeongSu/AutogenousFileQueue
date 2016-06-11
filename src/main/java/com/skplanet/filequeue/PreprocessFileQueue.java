package com.skplanet.filequeue;

import com.skplanet.element.Header;
import com.skplanet.properties.FileQueueProperties;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

/**
 * Created by byeongsukang on 2016. 6. 6..
 */
public class PreprocessFileQueue {
    private Logger logger = Logger.getLogger("AutogenousFileQueue");
    private String filePath;
    private String parentPath;
    private String dataPath;
    private String consumeLogPath;

    public PreprocessFileQueue() {
        filePath = FileQueueProperties.FILE_PATH;
        parentPath = filePath + FileQueueProperties.PARENT_PATH;
        dataPath = parentPath + FileQueueProperties.DATA_PATH;
        consumeLogPath = parentPath + FileQueueProperties.CONSUME_LOG_PATH;
        File parentDirectory = new File(parentPath);
        File dataDirectory = new File(dataPath);
        File consumeLogDirectory = new File(consumeLogPath);
        if (!checkFileIsExist(parentDirectory)) {
            createFile(parentDirectory);
        }
        if (!checkFileIsExist(dataDirectory)) {
            createFile(dataDirectory);
        }
        if (!checkFileIsExist(consumeLogDirectory)) {
            createFile(consumeLogDirectory);
        }

    }

    private boolean checkFileIsExist(File file) {
        if (file.exists()) {
            logger.info("File is exist. Path is : " + file.getAbsolutePath());
            return true;
        } else {
            logger.info("File is Empty. Path is : " + file.getAbsolutePath());
            return false;
        }
    }

    private void createFile(File parentDirectory) {

        try {
            boolean isSuccess = parentDirectory.mkdir();
            if (isSuccess) {
                logger.info("File created by : " + parentDirectory.getAbsolutePath());
            } else {
                throw new IOException();
            }
        } catch (IOException e) {
            logger.info("File creation Error ." + parentDirectory.getAbsolutePath() + " is invalid path.");
        }
        //file생성 있으면 읽고 없으면 생성
    }

    //한글 읽을때 깨져서 읽는다. 3바이트에 1글자이므로  이거 실제 전송시에는 어떻게 해결할지 고민해봐야한다
    public long findLatestHeader(String path) throws FileNotFoundException, IOException {
        RandomAccessFile raf = new RandomAccessFile(path, "rw");
        FileChannel fileChannel = raf.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        long length = raf.length();
        int readData = 0;
        while (1751474532 != readData) {//여기서 돌려줘야한다
            raf.seek(--length);
            fileChannel.read(byteBuffer);
            byteBuffer.rewind();
            readData = byteBuffer.getInt();
            byteBuffer.clear();
        }
        raf.close();
        fileChannel.close();
        return length;
    }

    public Header getHeader(String filePath, long offset) throws FileNotFoundException, IOException {

        RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
        FileChannel fileChannel = raf.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(16);

        raf.seek(offset);
        fileChannel.read(byteBuffer);
        byteBuffer.rewind();
        int header = byteBuffer.getInt();
        int elementCount = byteBuffer.getInt();
        int startOffset = byteBuffer.getInt();
        int endOffset = byteBuffer.getInt();

        raf.close();
        fileChannel.close();
        return new Header(header, elementCount, startOffset, endOffset);
    }

    public boolean verifyWriter(String dataFilePath, long endOffset) throws FileNotFoundException, IOException {
        Header header = getHeader(dataFilePath, endOffset);
        RandomAccessFile raf = new RandomAccessFile(dataFilePath, "rw");
        FileChannel fileChannel = raf.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);

        raf.seek(header.getEndOffset()-1);
        System.out.println("endoffset "+header.getEndOffset());
        fileChannel.read(byteBuffer);
        byteBuffer.rewind();
        byte temp = byteBuffer.get();
        raf.close();
        fileChannel.close();
        /* 0 == null, and 125 == '}' */
        System.out.println("value of endOffset "+temp);
        if (temp != 0) {
            return true;
        } else {
            return false;
        }
    }

    private String getConsummeLogFilePath(int index) {
        return consumeLogPath + FileQueueProperties.CONSUME_FILE_NAME + index + FileQueueProperties.CONSUME_FILE_SUFFIX;
    }

    private String getDataFilePath(int index) {
        return dataPath + FileQueueProperties.DATA_FILE_NAME + index + FileQueueProperties.DATA_FILE_SUFFIX;
    }

    //File 최대 사이즈를 unsigned int가 허용가능한 범위로 해야 에러가안난다 (int) 파싱할 경우 에러날 수 있다
    public int getHead() {
        try {
            int num = findLastFileNumber(dataPath);
            String dataFilePath = getDataFilePath(num);
            if (num == 0) {
                // initial setting. create File and return start position 0.
                File initialConsumeLogFile = new File(getDataFilePath(1));
                initialConsumeLogFile.createNewFile();
                return 0;
            }
            long lastOffset = findLatestHeader(dataFilePath);
            Header header = getHeader(dataFilePath, lastOffset);
            if (verifyWriter(dataFilePath, lastOffset)) {
                return header.getEndOffset();
            } else {
                return (int) lastOffset; //얘를 (int)로 바꾸는게아니라 비트연산으로 int로 바꾸자 utils
                //endOffset부터 다시 head찾는 verify후  overwrite
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    /*
        return ((buffer[offset] & 0xff) << 24)
                + ((buffer[offset + 1] & 0xff) << 16)
                + ((buffer[offset + 2] & 0xff) << 8)
                + (buffer[offset + 3] & 0xff);*/
    //나중에 InputStream , OutputStream과  RandomAccess속도 비교
    public int getTail() {
        try {

            int num = findLastFileNumber(consumeLogPath);
            String consummeLogFilePath = getConsummeLogFilePath(num);
            RandomAccessFile raf = new RandomAccessFile(consummeLogFilePath, "rw");
            FileChannel fileChannel = raf.getChannel();
            long fileLength=raf.length();
            long CONSUMELOG_FIXEX_SIZE = 16;
            if (num == 0) {
                // initial setting. create File and return start position 0.
                File initialConsumeLogFile = new File(getConsummeLogFilePath(1));
                initialConsumeLogFile.createNewFile();
                raf.close();
                fileChannel.close();
                return 0;
            }
            //깨진지 여부는 굳이 알 필요가 없다  나누기 연산을 하면  깨졌으면 이전것을 자동으로 찾아간다 거기다 다시 Length를 곱해주면 된다

            ByteBuffer byteBuffer = ByteBuffer.allocate(16);
            raf.seek((fileLength/CONSUMELOG_FIXEX_SIZE) * CONSUMELOG_FIXEX_SIZE);
            fileChannel.read(byteBuffer);
            Header header = new Header(byteBuffer.getInt(),byteBuffer.getInt(),byteBuffer.getInt(),byteBuffer.getInt());

            System.out.println("head : "+header.getHead());
            System.out.println("number : "+header.getElementCount());
            System.out.println("start : "+header.getStartOffset());
            System.out.println("end : "+header.getEndOffset());

            raf.close();
            fileChannel.close();
            return header.getStartOffset();



            //1. 컨슘 로그 파일 연다  OK
            //2. 컨슘 로그의 마지막 라인을 읽는다 ( 고정 크기 / 파일 크기 ) 하면 총 몇개인지 나온다 거기에 고정크기 * 갯수 해서 마지막라인을 읽는다
            //3. 해당 라인에 매칭되는 데이터파일에 HEAD식별자가 있는지 확인한다.*/
            //4. 확인되면 컨슘 시작한다
            //5. 확인이 안되면 이전 line으로 간다.
            //6. overwrite or append로 진행
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 1;
    }

    private void recoverIndex() {

    }


    //컨슘로그 지울꺼면. 새로운 파일 만들어지고 거기에 한줄 일떄   이전꺼 지우면  한줄에서 꺠지면 이전꺼 못찾아온다
    public int findLastFileNumber(String path) {
        try {
            File dir = new File(path);
            File files[] = new File[0];
            int latest = 0;
            if (dir.isDirectory()) {
                files = dir.listFiles();
            }
            if (files.length == 0) {
                logger.info(path + " directory is NULL");
                return 0;
            }
            for (File file : files) {
                if (file.isFile()) {
                    String name = file.getName();
                    String prefix[] = name.split("\\.");
                    String number[] = prefix[0].split("\\-");
                    int num = Integer.parseInt(number[1].trim());
                    if (latest < num) {
                        latest = num;
                    }
                }
            }
            return latest;
        } catch (NumberFormatException e) {
            logger.info("Parse Integer Exception. File Format is page-NUM.dat. Please Check your ConsumerLog Directory");
            return 0;
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.info("ArrayIndexOutOfBounds Exception. File Format is page-NUM.dat. Please Check your ConsumerLog Directory");
            return 0;
        }
    }

    private void close() {

    }
}
