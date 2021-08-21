package com.chriszt.flink.stream.operator;

public class Trade {

    private String cardNum;

    private int trade;

    private String time;

    public Trade() {
    }

    public Trade(String cardNum, int trade, String time) {
        this.cardNum = cardNum;
        this.trade = trade;
        this.time = time;
    }

    public String getCardNum() {
        return cardNum;
    }

    public void setCardNum(String cardNum) {
        this.cardNum = cardNum;
    }

    public int getTrade() {
        return trade;
    }

    public void setTrade(int trade) {
        this.trade = trade;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Trade{" +
                "cardNum='" + cardNum + '\'' +
                ", trade=" + trade +
                ", time='" + time + '\'' +
                '}';
    }
}
