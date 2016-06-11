package com.skplanet.filequeue;

import com.oracle.javafx.jmx.json.JSONDocument;
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
    public long findLastHeader(String path) throws FileNotFoundException, IOException {
        //try {
        System.out.println("path : " + path);
        RandomAccessFile raf = new RandomAccessFile(path, "rw");
        FileChannel fileChannel = raf.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        String bytebfferToString = "";
        long length = raf.length();
        while (!("head".equals(bytebfferToString))) {//여기서 돌려줘야한다
            System.out.println("length :" + length);
            raf.seek(--length);
            fileChannel.read(byteBuffer);
            bytebfferToString = new String(byteBuffer.array(), 0, byteBuffer.position());
            System.out.println("The Byte is : " + bytebfferToString);
            byteBuffer.clear();
        }
        System.out.println("Length is : " + length);

        return length;

        //  System.out.println("read result is " +bytebfferToString);
//            String stringToInsert = "{This is a string to insert into a file.}";
//            byte[] answerByteArray = stringToInsert.getBytes();
//            ByteBuffer byteBuffer = ByteBuffer.wrap(answerByteArray);

//            fileChannel.write(byteBuffer);

//지금 궁금한점.. 올로케이트 갯수만큼만 딱한번 읽나

//            int bytesRead = fileChannel.read(buf);
//            System.out.println("dddd : " + bytesRead);
        //ByteBuffer buf2 = ByteBuffer.allocate(1024);

        //    buf2.clear();


        //1. 컨슘 로그 파일 연다  OK
        //2. 컨슘 로그의 마지막 라인을 읽는다 ( 고정 크기 / 파일 크기 ) 하면 총 몇개인지 나온다 거기에 고정크기 * 갯수 해서 마지막라인을 읽는다
        //3. 해당 라인에 매칭되는 데이터파일에 HEAD식별자가 있는지 확인한다.
        /*}catch(FileNotFoundException e){

            return 0;
        }catch(IOException e){
            return 0;
        }*/
    }

    public boolean verifyWriter(String dataFilePath, long endOffset) throws FileNotFoundException, IOException {
        RandomAccessFile raf = new RandomAccessFile(dataFilePath, "rw");
        FileChannel fileChannel = raf.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(16);

        raf.seek(endOffset);
        fileChannel.read(byteBuffer);

        System.out.println("bbget : " + byteBuffer.get(0));
        byteBuffer.rewind();
        //head부터 4바이트 =개수 4바이트 =start 4바이트 =end
        int header = byteBuffer.getInt();
        int elementCount = byteBuffer.getInt();
        int startIndex = byteBuffer.getInt();
        int endIndex = byteBuffer.getInt();
        System.out.println("end index : "+endIndex);

        byteBuffer.clear();
        ByteBuffer nbyteBuffer = ByteBuffer.allocate(1);
        raf.seek(endIndex);
        fileChannel.read(nbyteBuffer);
        nbyteBuffer.rewind();
        System.out.println("end buffer " +nbyteBuffer.get());
        if (true/*file(endoffset)==null || ~~~*/) {
            return true;
        } else {
            return false;
        }
    }

    private String getConsummeLogFilePath(int index) {
        return consumeLogPath + FileQueueProperties.DATA_FILE_NAME + index + FileQueueProperties.DATA_FILE_SUFFIX;
    }

    private String getDataFilePath(int index) {
        return dataPath + FileQueueProperties.DATA_FILE_NAME + index + FileQueueProperties.DATA_FILE_SUFFIX;
    }

    public int getHead() {
        try {
            String dataFilePath = getDataFilePath(findLatestFileNumber(dataPath));
            if (verifyWriter(dataFilePath, findLastHeader(dataFilePath))) {


            } else {
                //endOffset부터 다시 head찾는 verify후  overwrite
            }

            //1. 데이터 디렉토리의 가장 끝 파일을 읽는다.
            //2. 마지막 줄을 가져온다  마지막줄의 첫 헤더를 어떻게 가져올까요
            return 1;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
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


           /* String latestConsumeLogPath = consumeLogPath + File.separator + FileQueueProperties.CONSUME_FILE_NAME + num + FileQueueProperties.CONSUME_FILE_SUFFIX;
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

    private void close() {

    }
}
