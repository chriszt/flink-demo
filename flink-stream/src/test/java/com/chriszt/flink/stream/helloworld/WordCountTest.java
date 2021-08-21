package com.chriszt.flink.stream.helloworld;

import org.junit.Test;

public class WordCountTest {

    @Test
    public void testWordCount () {
        String[] words = new String[] {
                "com.chriszt.flink.stream.helloworld.WordCount",
                "com.chriszt.flink.stream.helloworld.WordCount"
        };

        new WordCount().wordCount(words);
    }

}
