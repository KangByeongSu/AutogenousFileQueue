package com.skplanet.utils;

import com.skplanet.element.Header;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by byeongsukang on 2016. 6. 12..
 */
public class Utils {
    public static Header getHeader(FileChannel fileChannel, long offset) throws FileNotFoundException, IOException {

        ByteBuffer byteBuffer = ByteBuffer.allocate(16);

        fileChannel.position(offset);
//        raf.seek(offset);
        fileChannel.read(byteBuffer);
        byteBuffer.rewind();
        int header = byteBuffer.getInt();
        int elementCount = byteBuffer.getInt();
        int startOffset = byteBuffer.getInt();
        int endOffset = byteBuffer.getInt();

        return new Header(header, elementCount, startOffset, endOffset);
    }
}
