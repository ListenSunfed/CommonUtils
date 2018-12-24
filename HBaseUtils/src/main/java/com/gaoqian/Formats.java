package com.gaoqian;

public enum Formats {

    DATE_FORMAT("yyyyMMddHHmmss"),Date_yearMonth("yyyyMM");

    private String format;

    private Formats(String format) {
        this.format = format;
    }

    public String value() {
        return format;
    }
}
