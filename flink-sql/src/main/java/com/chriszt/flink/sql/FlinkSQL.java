package com.chriszt.flink.sql;

public class FlinkSQL {

    final public static String RULE1 = "select * from UserTab";

    final public static String RULE2 = "select count(id) from UserTab where id=2 or name='aaa'";

}
