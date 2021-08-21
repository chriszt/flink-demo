package com.chriszt.flink.cep;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MyWorkTest {

    @Test
    public void testWork1() {
        List<LoginEvent> lst = new ArrayList<>();
        lst.add(new LoginEvent("小明","192.168.0.1","fail"));
        lst.add(new LoginEvent("小明","192.168.0.2","fail"));
        lst.add(new LoginEvent("小王","192.168.10,11","fail"));
        lst.add(new LoginEvent("小王","192.168.10,12","fail"));
        lst.add(new LoginEvent("小明","192.168.0.3","fail"));
        lst.add(new LoginEvent("小明","192.168.0.4","fail"));
        lst.add(new LoginEvent("小王","192.168.10,10","success"));
        lst.add(new LoginEvent("小王","192.168.10,7","fail"));
        new MyWork().work1(lst);
    }

}
