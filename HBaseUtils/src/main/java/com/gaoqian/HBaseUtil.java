package com.gaoqian;

import org.apache.commons.httpclient.util.DateUtil;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @Description TODO
 * @Author Administrator
 * @Date 2018/11/6 0006 上午 11:58
 **/
public class HBaseUtil {
    private static ThreadLocal<Connection> connThread = new ThreadLocal<>();
    private static ThreadLocal<Admin> adminThread = new ThreadLocal<>();

    //开始
    public static void start() throws IOException {
        Connection conn = connThread.get();
        if (conn == null) {
            conn = ConnectionFactory.createConnection();
            connThread.set(conn);
        }
        Admin admin = adminThread.get();
        if (admin == null) {
            admin = conn.getAdmin();
            adminThread.set(admin);
        }

    }

    //创建命名空间，判断是否存在，
    public static void createNamespaceNX(String namespace) throws IOException {
        Admin admin = adminThread.get();
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            createNamespace(namespace);
        }
    }

    //创建命名空间
    public static void createNamespace(String namespace) throws IOException {
        Admin admin = adminThread.get();

        NamespaceDescriptor descriptor = NamespaceDescriptor.create(namespace).build();

        admin.createNamespace(descriptor);
    }

    //    public static void createTable( String tableName, int regionCount ) throws IOException {
//        createTable(tableName, regionCount);
//    }
    public static void createNametableXX(String nameTable, int regionCount) {
        try {
            Admin admin = adminThread.get();
            //表的描述器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(nameTable));
            //列的描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Names.HBASE_FAMILY.value());

            hTableDescriptor.addFamily(hColumnDescriptor);
            //当分区数为0，默认分区
            if (regionCount == 0) {
                admin.createTable(hTableDescriptor);
            } else {
                //当分区数部位0 创建指定分区数表
                byte[][] splitKeys = getSplitKeys(regionCount);
                admin.createTable(hTableDescriptor, splitKeys);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //创建表
    public static void createNametable(String nameTable, int regionCount,String observerClass) {
        try {
            Admin admin = adminThread.get();
            //表的描述器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(nameTable));
            //列的描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Names.HBASE_FAMILY.value());

            hTableDescriptor.addFamily(hColumnDescriptor);
            HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(Names.HBASE_FAMILY_CCC.value());
            hTableDescriptor.addFamily(hColumnDescriptor1);

            if(observerClass != null){
                hTableDescriptor.addCoprocessor(observerClass);
            }
            //当分区数为0，默认分区
            if (regionCount == 0) {
                admin.createTable(hTableDescriptor);
            } else {
                //当分区数部位0 创建指定分区数表
                byte[][] splitKeys = getSplitKeys(regionCount);
                admin.createTable(hTableDescriptor, splitKeys);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //当表不存在时创建
    public static void createNametableNX(String nameTable, int regionCount,String observerClass) throws Exception {
        Admin admin = adminThread.get();
        if (admin.tableExists(TableName.valueOf(nameTable))) {

            deletable(nameTable);
        }
        createNametable(nameTable, regionCount,observerClass);
    }

    //删除表
    public static void deletable(String nameTable) {
        Admin admin = adminThread.get();
        try {
            admin.disableTable(TableName.valueOf(nameTable));
            admin.deleteTable(TableName.valueOf(nameTable));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //结束
    public static void end() {
        Admin admin = adminThread.get();
        if (admin != null) {
            try {
                admin.close();
                adminThread.remove();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Connection conn = connThread.get();
        if (conn != null) {
            try {
                conn.close();
                connThread.remove();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //获取表
    public static Table getTable(String nameTable) throws IOException {
        Connection conn = connThread.get();
        Table table = conn.getTable(TableName.valueOf(nameTable));
        return table;
    }

    //获取分区号
    public static String getRegionNum(String call, String date) {
        String yearMonth = date.substring(0, 6);
        //获取hashcode值
        String userCode = call.substring(call.length() - 4);
        StringBuilder sb = new StringBuilder(userCode);
        userCode = sb.reverse().toString();

        int h1 = userCode.hashCode();
        int h2 = yearMonth.hashCode();

        //经过异或算法
        int crc = Math.abs(h1 ^ h2);
        // 在经过取模
        int RegionIndex = crc % 6;
        return RegionIndex + "";
    }

    //获取分区键
    public static byte[][] getSplitKeys(int regionCount) {
//        (-负无穷，1)（1,2）（2，正无穷）
//        假设3个分区，则分区键就有两个，假设分区有6个，分区键就有5个
        int regionkey = regionCount - 1;

        ArrayList<byte[]> bytes = new ArrayList<>();

        for (int i = 0; i < regionkey; i++) {
            String key = i + "|";
            bytes.add(Bytes.toBytes(key));
        }

        byte[][] splitKey = new byte[regionkey][];

        bytes.toArray(splitKey);

        return splitKey;
    }

    //获取一定范围内的数据
    public static List<String[]> getRangeData(String call, String startDate, String endDate) throws Exception {
//        201802-->201803
        //        201803-->201804
        //        201804-->201805
        ArrayList<String[]> list = new ArrayList<>();
        //获取日历类对象
        Calendar calendar1 = Calendar.getInstance();
        calendar1.setTime(DateUtils.parse(startDate, Formats.Date_yearMonth));
        //将字符串转化成日历
        Calendar calendar2 = Calendar.getInstance();
        calendar2.setTime(DateUtils.parse(endDate, Formats.Date_yearMonth));

        while (calendar1.getTimeInMillis() <= calendar2.getTimeInMillis()) {

            String nowDate = DateUtils.dateFormat(calendar1.getTime(), Formats.Date_yearMonth);
            //获取指定分区号
            String regionNum = getRegionNum(call, nowDate);
            //获取开始row
            String startRowkey = regionNum + "_" + call + "_" + nowDate;
            //获取结束的row
            String endRowkey = startRowkey + "|";
            //组成一个数组范围
            String[] str = {startRowkey, endRowkey};
            //添加到集合中
            list.add(str);
            //范围之内的月份拆分后每次加1个月
            calendar1.add(Calendar.MONTH, 1);
        }
        return list;
    }
}
