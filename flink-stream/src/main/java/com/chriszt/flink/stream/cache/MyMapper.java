package com.chriszt.flink.stream.cache;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.io.*;

public class MyMapper extends RichMapFunction<Long, String> {

    private String cacheStr = null;

    private String readFile(File file) {
        StringBuffer sbf = new StringBuffer();
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String tmpStr = null;
            while((tmpStr = reader.readLine()) != null) {
                sbf.append(tmpStr);
            }
            reader.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

//        System.out.println("[open] " + Thread.currentThread().getId());

        return sbf.toString();
    }

    @Override
    public void open(Configuration parameters) {
        RuntimeContext rc = getRuntimeContext();
        DistributedCache dc = rc.getDistributedCache();
        File cacheFile = dc.getFile("localFile.txt");
        cacheStr = readFile(cacheFile);
        System.out.println("MyMapper::open(), " + Thread.currentThread().getId() + ", " + cacheStr);
    }

    @Override
    public String map(Long value) throws Exception {
        Thread.sleep(5000);
        return StringUtils.join(value, "---", cacheStr);
    }

}
