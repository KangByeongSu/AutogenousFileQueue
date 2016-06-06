package com.skplanet.filequeue;

import properties.FileQueueProperties;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.ParseException;
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
    private RandomAccessFile raf;

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

    private int findLastHeader() {
        //while돌려서 head나올떄까지..
        return 1;
    }

    private boolean verifyWriter(int endOffset) {
        if (true/*file(endoffset)==null || ~~~*/) {
            return true;
        } else {
            return false;
        }
    }

    protected int getHead() {

        int endOffset = findLastHeader();
        if (verifyWriter(endOffset)) {

        } else {
            //endOffset부터 overwrite
        }

        //1. 데이터 디렉토리의 가장 끝 파일을 읽는다.
        //2. 마지막 줄을 가져온다  마지막줄의 첫 헤더를 어떻게 가져올까요
        return 1;
    }

    //나중에 InputStream , OutputStream과  RandomAccess속도 비교
    public int getTail() {

        try {
            int num = findLatestFileNumber(consumeLogPath);
            System.out.println("Tail num : " + num);
            if (num == 0) {
                // initial setting. create File and return start position 0.
                File initialConsumeLogFile = new File(consumeLogPath + File.separator + FileQueueProperties.CONSUME_FILE_NAME + 1 + FileQueueProperties.CONSUME_FILE_SUFFIX);

                initialConsumeLogFile.createNewFile();
                return 0;
            }


            String latestConsumeLogPath = consumeLogPath + File.separator + FileQueueProperties.CONSUME_FILE_NAME + num + FileQueueProperties.CONSUME_FILE_SUFFIX;
            System.out.println("path : " + latestConsumeLogPath);

            RandomAccessFile raf = new RandomAccessFile(latestConsumeLogPath, "rw");

            FileChannel fileChannel = raf.getChannel();

            ByteBuffer buf = ByteBuffer.allocate(48);

            int bytesRead = fileChannel.read(buf);
            System.out.println("dddd : " + bytesRead);
            //ByteBuffer buf2 = ByteBuffer.allocate(1024);

            //    buf2.clear();


            //1. 컨슘 로그 파일 연다  OK
            //2. 컨슘 로그의 마지막 라인을 읽는다 ( 고정 크기 / 파일 크기 ) 하면 총 몇개인지 나온다 거기에 고정크기 * 갯수 해서 마지막라인을 읽는다
            //3. 해당 라인에 매칭되는 데이터파일에 HEAD식별자가 있는지 확인한다.
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
    public int findLatestFileNumber(String path) {
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

    private void findTailFile() {

    }

    private void findHeadFile() {

    }

    private void close(){

    }
}
