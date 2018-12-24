package com.gaoqian;

public enum Names {
    TOPIC_VALUE("ct"),HBASE_FAMILY("info"),
    CT_NAMESPACE("ct"),CT_NAMETABLE("ct:call"),HBASE_FAMILY_CCC("ccc");


    private String topic;

    private Names(String topic){
        this.topic = topic;
    }
    public String value() {

        return topic;
    }
}
