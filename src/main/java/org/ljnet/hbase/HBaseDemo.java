package org.ljnet.hbase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;

/**
 * 这个Demo主要实现将hdfs数据导入到hbase、实现以rowkey为条件进行区间查询以及单列查询
 * https://blog.csdn.net/smile0198/article/details/37069863
 */

public class  HBaseDemo  {
    public static class HbaseMap extends Mapper<LongWritable,Text,Text,Text> {
        /**
         * 用map将hdfs中的数据以tab切割出来,数据格式如下
         * 1	陈浩文	2	绿伽大数据开发
         * 2	张小清	1	绿伽大数据开发
         * 3	景峰	3	绿伽运维
         * 陈	浩	2	绿伽网络
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split=value.toString().split("\t");
            context.write(new Text(split[0]),new Text(split[1]+"\t"+split[2]+"\t"+split[3]));
        }
    }
    public static class HbaseReducer extends TableReducer<Text, Text, NullWritable>{
        /**
         *将key作为hbase的rowkey，将数据导入列簇为f1,列为name、id、work的表中
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Put put=new Put(Bytes.toBytes(key.toString()));
            //  Put put=new Put(key.toString().getBytes());
            for(Text v:values) {
                String[] split=v.toString().split("\t");
                if(split[0]!=null) {
                    // put.add(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes(String.valueOf(split[0])));
                    put.add(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes(split[0]));
                }
                if(split[1]!=null){
                    put.add(Bytes.toBytes("f1"), Bytes.toBytes("id"), Bytes.toBytes(String.valueOf(split[1])));
                }
                if(split[2]!=null){
                    put.add(Bytes.toBytes("f1"), Bytes.toBytes("works"), Bytes.toBytes(String.valueOf(split[2])));
                    // put.add(Bytes.toBytes("f1"), Bytes.toBytes("works"), "陈".getBytes());
                    //put.add(Bytes.toBytes("f1"), Bytes.toBytes("works"),Bytes.toBytes(Bytes.toString(Bytes.toBytes(split[2]))));
                }
                context.write(NullWritable.get(),put);
            }
        }

    }

    /**
     * 判断表是否存在、若不存在自动创建
     * @param tableName
     * @throws IOException
     */
    public static void createTable(String tableName) throws IOException {
        HTableDescriptor table=new HTableDescriptor(tableName);//
        HColumnDescriptor tableColume=new HColumnDescriptor("f1");
        table.addFamily(tableColume);
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop01.ljnet.com");
        HBaseAdmin admin=new HBaseAdmin(conf);//建立与hbase集群的连接
        if(!admin.tableExists(tableName)){
            System.out.println("表已不存在！正在重新创建......");
//                  admin.disableTable(tableName);//禁用表
//                  admin.enableTable(tableName);//启用表
//                  admin.deleteTable(tableName);//删除表、前提是表是禁用的
            admin.createTable(table);
            admin.close();
        }
        //  admin.createTable(table);
        else System.out.println("新表:"+tableName+"已存在！！！");

    }
    /**
     * hbase shell终端无法直接显示中文、获取无法在hbase shell终端直接显示的中文数据到idea终端显示
     */
    /**
     *列查询
     * @throws IOException
     */
    public static void onlyTable() throws IOException {
        org.apache.hadoop.conf.Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop01.ljnet.com");//zk连接配置
        Scan scan=new Scan();
        HTable table = new HTable(conf, "hbaseDemo");
        ResultScanner scanner = table.getScanner(scan);
        for (Result r = scanner.next(); r != null; r = scanner.next()) {
            byte[] value = r.getValue("f1".getBytes(),Bytes.toBytes("name"));
            String m = new String(value);
            System.out.println("Found row: " + m);
        }
    }

    /**
     * 全表查询
     * @param tablename
     * @param startRow
     * @param stopRow
     */
    public static void ScanTable(String tablename,String startRow,String stopRow){

        HTablePool pool;//连接池
        org.apache.hadoop.conf.Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop01.ljnet.com");

        HTableInterface tbTable=null;
        ResultScanner scanner = null;
        try{
            pool=new HTablePool(conf,1);
            tbTable=pool.getTable(tablename);
            // ResultScanner rs=null;
            Scan scan=new Scan();

            if(startRow!=null){
                scan.setStartRow(Bytes.toBytes(startRow));
            }
            if(stopRow !=null){
                scan.setStopRow(Bytes.toBytes(stopRow));
            }
            scanner=tbTable.getScanner(scan);
            tbTable.close();
            for(Result r:scanner){
                for(KeyValue kv:r.raw()){
                    StringBuffer sb = new StringBuffer()
                            .append(Bytes.toString(kv.getRow())).append("\t")
                            .append(Bytes.toString(kv.getFamily()))
                            .append("\t")
                            .append(Bytes.toString(kv.getQualifier()))
                            .append("\t").append(Bytes.toString(kv.getValue()));
                    System.out.println(sb.toString());

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        createTable("hbaseDemo");
        ScanTable("hbaseDemo","1","3");//扫描rowkey 1-3 的所有数据
        conf.set(TableOutputFormat.OUTPUT_TABLE,"hbaseDemo");
        onlyTable();
        conf.set("hbase.zookeeper.quorum","hadoop01.ljnet.com");
        Job job=new Job(conf,"hth");
        job.setJarByClass( HBaseDemo.class);
        job.setMapperClass(HbaseMap.class);
        job.setReducerClass(HbaseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        FileInputFormat.addInputPath(job,new Path("hdfs://hadoop01.ljnet.com:8020/data.chw/hivetest.txt"));
        if(job.waitForCompletion(true)){
            System.out.println("ok!");
            System.exit(0);
        }else {
            System.out.println("no!");
            System.exit(1);
        }
    }
}



