package com.gaoqian.other;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * HBase的API应用
 */
public class HBaseUtil {
    //创建当前线程共享内存
    private static ThreadLocal<Admin> threadLocal = new ThreadLocal<>();

    private static ThreadLocal<Connection> connectionLocal = new ThreadLocal<>();

    //获取admin对象
    public static Admin getAdmin()throws IOException {
        //获取当前线程
        Admin admin = threadLocal.get();
        if(admin==null){
            Connection conn = getConnection();
            admin = conn.getAdmin();
            //设置当前线程
            threadLocal.set(admin);
        }
        return admin;
    }
    public static Connection getConnection() throws IOException {
        Connection connection = connectionLocal.get();
        if(connection==null){
            //1.获取配置对象
            Configuration conf = HBaseConfiguration.create();

            //2。增加配置
//            conf.set("hbase.zookeeper.quorum", "hadoop102");
//            conf.set("hbase.zookeeper.property.clientPort", "2181");
            //3 建立连接
//        HBaseAdmin admin = new HBaseAdmin(conf);
            connection = ConnectionFactory.createConnection(conf);
            connectionLocal.set(connection);
        }
        return connection;
    }
    //关闭资源
    public static void close() throws IOException {
        Admin admin = threadLocal.get();
        if(admin!=null){
            admin.close();
            //将当前线程移除共享内存中
            threadLocal.remove();
        }
    }
    //判断命名空间是否存在
    public static boolean isNameSpace(String namespace)  {
        try{
            Admin admin = getAdmin();
            admin.getNamespaceDescriptor(namespace);
            return true;
        }catch(Exception e){
            e.printStackTrace();
        }
        return false;
    }
    /**
     * 创建命名空间
     * @param namespace
     */
    public static void createNamespace( String namespace ) throws Exception {

        Admin admin = getAdmin();

        NamespaceDescriptor nd = NamespaceDescriptor.create(namespace).build();

        admin.createNamespace(nd);

    }
    //判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        Admin admin = getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }
    //创建没有版本的表
    public static void createTable(String tableName, String... columnFamilies) throws Exception {
        createTbale(tableName, 1, columnFamilies);
    }
    //创建表
    public static void createTbale(String tableName,int version,String... columnFamilys) throws Exception{
        Admin admin = getAdmin();

            //创建表描述信息
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //有多个列族
            for(String column:columnFamilys){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(column);
                hColumnDescriptor.setMaxVersions(version);
                hColumnDescriptor.setMinVersions(version);
                descriptor.addFamily(hColumnDescriptor);
            }
            //提交表描述器
            admin.createTable(descriptor);
    }
    //删除表
    public static  void deleteTable(String nameTable) throws IOException {
        Admin admin = getAdmin();
        if(isTableExist(nameTable)){
            //将表状态设置成disable禁用
            admin.disableTable(TableName.valueOf(nameTable));
            //删除表
            admin.deleteTable(TableName.valueOf(nameTable));
        }
    }
    //添加数据
    public static void addData(String tablename,Put put)throws Exception{
        //获取表对象
        Table table = getConnection().getTable(TableName.valueOf(tablename));
        //将put命令添加到table对象中
        table.put(put);
    }
    //添加数据多个
    public static void addDatas(String tablename,List<Put> puts)throws Exception{
        //获取表对象
        Table table = getConnection().getTable(TableName.valueOf(tablename));
        //将put命令添加到table对象中
        table.put(puts);
    }
    //删除数据
    public static void deleteData(String tablename,Delete delete)throws Exception{
        //获取table对象
        Table table = getConnection().getTable(TableName.valueOf(tablename));
        //将delete命令添加到table对象中
        table.delete(delete);
        table.close();
    }
    //删除多行数据
    public static void deleteDatas(String tablename, List<Delete> deletes)throws Exception{
        //获取表对象
        Table table = getConnection().getTable(TableName.valueOf(tablename));
//        ArrayList<Delete> list = new ArrayList<>();
//        for(String row:rows){
//            Delete delete = new Delete(Bytes.toBytes(row));
//            //删除多行数据添加到list
//            list.add(delete);
//        }
        table.delete(deletes);
    }
    //全表扫描查询
    public static ResultScanner selectDatas(String tablename, Scan scan)throws Exception{
        Table table = getConnection().getTable(TableName.valueOf(tablename));
        return table.getScanner(scan);
    }
    //获取某一行的数据
    public static Result selectData(String tablename, Get get)throws Exception{
        //获取table对象
        Table table = getConnection().getTable(TableName.valueOf(tablename));
        //获取get命令，获取某个行信息的命令
        return table.get(get);
    }
    //获取某一行的列族的指定列的值
    public static void selectColumn(String tablename, String rowkey,String family,String column)throws Exception{
        Table table = getConnection().getTable(TableName.valueOf(tablename));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(value);
        }
    }
}
