package com.gaoqian;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 * @Description TODO
 * @Author Administrator
 * @Date 2018/11/5 0005 上午 11:55
 **/
public class DateUtils {
   //将指定日期转化成字符串
    public static String dateFormat(Date d, Formats f){
        SimpleDateFormat simple = new SimpleDateFormat(f.value());

        return simple.format(d);
    }
    //将指定字符串转化成指定日期
    public static Date parse(String date,Formats f) throws Exception{
        SimpleDateFormat simple = new SimpleDateFormat(f.value());
        return simple.parse(date);
    }
}
