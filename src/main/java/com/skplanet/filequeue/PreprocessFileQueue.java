package com.skplanet.filequeue;

import com.skplanet.element.Header;
import com.skplanet.properties.FileQueueProperties;
import com.skplanet.utils.Utils;

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
    //    private final static int HEAD_DATA=1751474532;
    private final static int HEAD_DATA = ByteBuffer.wrap(new byte[]{/*head*/0x68, 0x65, 0x61, 0x64}).getInt();

    private final static long CONSUMELOG_FIXEX_SIZE = 16;
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
    public long findLatestHeader(FileChannel fileChannel) throws FileNotFoundException, IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        long length = fileChannel.size();
        int readData = 0;
        while (HEAD_DATA != readData) {//여기서 돌려줘야한다
            if (length == 0) break;
            fileChannel.position(--length);
            fileChannel.read(byteBuffer);
            byteBuffer.rewind();
            readData = byteBuffer.getInt();
            byteBuffer.clear();
        }
        return length;
    }


    /**
     * Write index verification
     */
    public boolean verifyWriter(FileChannel fileChannel, long endOffset) throws FileNotFoundException, IOException {
        Header header = Utils.getHeader(fileChannel, endOffset);
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);

        if (header.getEndOffset() == 0) {
            return true;
        }
        fileChannel.position(header.getEndOffset() - 1);
        System.out.println("endoffset " + header.getEndOffset());
        fileChannel.read(byteBuffer);
        byteBuffer.rewind();
        byte temp = byteBuffer.get();
        /* 0 == null, and 125 == '}' */
        System.out.println("value of endOffset " + temp);
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

    /**
     * get write offset after revision
     *
     * @return
     */
    public Header getHead() {
        try {
            int num = findLastFileNumber(dataPath);

            if (num == 0) {
                // initial setting. create File and return start position 0.
                File initialConsumeLogFile = new File(getDataFilePath(1));
                initialConsumeLogFile.createNewFile();
                return new Header(HEAD_DATA, 0, 0, 0);
            }
            String dataFilePath = getDataFilePath(num);
            RandomAccessFile raf = new RandomAccessFile(dataFilePath, "rw");
            FileChannel fileChannel = raf.getChannel();
            long lastOffset = findLatestHeader(fileChannel);
            Header header = Utils.getHeader(fileChannel, lastOffset);
            if (!verifyWriter(fileChannel, lastOffset)) {
                header.setEndOffset((int) lastOffset); //얘를 (int)로 바꾸는게아니라 비트연산으로 int로 바꾸자 utils
                //endOffset부터 다시 head찾는 verify후  overwrite
            }

            raf.close();
            fileChannel.close();
            return header;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * get read offset after revision
     *
     * @return
     */
    public Header getTail() {
        try {

            int num = findLastFileNumber(consumeLogPath);

            if (num == 0) {
                // initial setting. create File and return start position 0.
                File initialConsumeLogFile = new File(getConsummeLogFilePath(1));
                initialConsumeLogFile.createNewFile();
                return new Header(HEAD_DATA, 0, 0, 0);
            }
            String consummeLogFilePath = getConsummeLogFilePath(num);
            RandomAccessFile raf = new RandomAccessFile(consummeLogFilePath, "rw");
            FileChannel fileChannel = raf.getChannel();
            long fileLength = raf.length();
            //깨진지 여부는 굳이 알 필요가 없다  나누기 연산을 하면  깨졌으면 이전것을 자동으로 찾아간다 거기다 다시 Length를 곱해주면 된다
            ByteBuffer byteBuffer = ByteBuffer.allocate(16);

            if (fileLength == 0){
                return new Header(HEAD_DATA, 0, 0, 0);
            }
                fileChannel.position(((fileLength / CONSUMELOG_FIXEX_SIZE) * CONSUMELOG_FIXEX_SIZE) - 16);
            //이 다음놈의 data header를 가져온다
            //이전놈의 consumerlog header를 가져온다.
            fileChannel.read(byteBuffer);
            byteBuffer.rewind();
            Header header = new Header(byteBuffer.getInt(), byteBuffer.getInt(), byteBuffer.getInt(), byteBuffer.getInt());

            //얘가 가지고 있는 엔드 오프셋에서 헤더를뽑아서 거기의 엔드 오프셋이 널인지를 체크해야합니다
            /**
             * 어떻게 할까 얘는.. 일단
             */
/*
            RandomAccessFile dataRaf = new RandomAccessFile(getDataFilePath(findLastFileNumber(dataPath)), "rw");
            FileChannel dataFileChannel = dataRaf.getChannel();
            if(!verifyWriter(dataFileChannel,Utils.getHeader(dataFileChannel,header.getEndOffset()).getEndOffset())){
                //어떻게 처리해야하지
            }
            //데이터 null인지 체크할까?*/

            System.out.println("head : " + header.getHead());
            System.out.println("number : " + header.getElementCount());
            System.out.println("start : " + header.getStartOffset());
            System.out.println("end : " + header.getEndOffset());

            raf.close();
            fileChannel.close();


            return header;

//start end 데이터파일에 존재ㅐ하는지 확인한번 하고 리턴해야한다

            //1. 컨슘 로그 파일 연다  OK
            //2. 컨슘 로그의 마지막 라인을 읽는다 ( 고정 크기 / 파일 크기 ) 하면 총 몇개인지 나온다 거기에 고정크기 * 갯수 해서 마지막라인을 읽는다
            //3. 해당 라인에 매칭되는 데이터파일에 HEAD식별자가 있는지 확인한다.*/
            //4. 확인되면 컨슘 시작한다
            //5. 확인이 안되면 이전 line으로 간다.
            //6. overwrite or append로 진행
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    //컨슘로그 지울꺼면. 새로운 파일 만들어지고 거기에 한줄 일떄   이전꺼 지우면  한줄에서 꺠지면 이전꺼 못찾아온다

    /**
     * get Last File Number in directory
     *
     * @param path
     * @return
     */
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
