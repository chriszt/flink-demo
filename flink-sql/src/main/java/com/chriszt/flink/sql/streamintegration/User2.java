package com.chriszt.flink.sql.streamintegration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class User2 {
    private String name;
    private Integer score;
    private Instant event_time;
}
