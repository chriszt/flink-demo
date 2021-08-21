package com.chriszt.flink.stream.cache;

import org.junit.Test;

public class DistributedCacheTest {

    @Test
    public void testDistributedCache () {
        String filePath = getClass().getResource("/localFile.txt").getPath();
        new DistributedCache().distributedCache(filePath);
    }

}
