package com.skplanet.utils;

import java.nio.ByteBuffer;

/**
 * Created by byeongsukang on 2016. 6. 11..
 */
public class Calculator {
    public static byte[] convertBytes(int num) {
        return ByteBuffer.allocate(4).putInt(num).array();
    }

}
