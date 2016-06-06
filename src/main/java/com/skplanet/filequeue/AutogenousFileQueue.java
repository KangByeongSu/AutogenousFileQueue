package com.skplanet.filequeue;

import java.io.RandomAccessFile;
import java.util.Properties;

/**
 * Created by byeongsukang on 2016. 6. 6..
 */
public class AutogenousFileQueue implements FileQueue {
    private static AutogenousFileQueue autogenousFileQueue;
    private int headOffset;
    private int tailOffset;


    public AutogenousFileQueue() {
        PreprocessFileQueue pre = new PreprocessFileQueue();
        headOffset = pre.getHead();
        tailOffset = pre.getTail();
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



    public void add() {

    }

    public void offer() {

    }

    public void remove() {

    }

    public void poll() {

    }

    public void element() {

    }

    public void peek() {

    }
}
