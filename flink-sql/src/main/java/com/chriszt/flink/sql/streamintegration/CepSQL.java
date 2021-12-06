package com.chriszt.flink.sql.streamintegration;

public class CepSQL {

    final public static String RULE1 = "SELECT * " +
            "FROM UserTab " +
            "  MATCH_RECOGNIZE ( " +
//            "    PARTITION BY id " +
            "    ORDER BY inTime " +
            "    MEASURES " +
            "      A.id AS aid, " +
            "      B.id AS bid " +
            "    ONE ROW PER MATCH " +
            "    AFTER MATCH SKIP PAST LAST ROW " +
            "    PATTERN (A B) WITHIN INTERVAL '1' SECOND " +
            "    DEFINE " +
            "      A AS A.id = 2, " +
            "      B AS B.id = 3 " +
            "  )";

}
