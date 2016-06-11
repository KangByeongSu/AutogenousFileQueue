package com.skplanet.properties;

import java.io.File;

/**
 * Created by byeongsukang on 2016. 6. 6..
 */
public class FileQueueProperties {
    public final static String FILE_PATH = File.separator+"tmp";
    public final static String PARENT_PATH = File.separator+"autogenous_path";
    public final static String DATA_PATH = File.separator+"data";
    public final static String CONSUME_LOG_PATH = File.separator+"consume_log";
    public final static String CONSUME_FILE_NAME=File.separator+"consume-";
    public final static String CONSUME_FILE_SUFFIX=".index";
    public final static String DATA_FILE_NAME=File.separator+"page-";
    public final static String DATA_FILE_SUFFIX=".dat";
}
