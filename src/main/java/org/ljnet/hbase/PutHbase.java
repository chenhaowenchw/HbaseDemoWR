package org.ljnet.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class PutHbase {
    public static void main(String[] args) {
        Configuration config= HBaseConfiguration.create();//连接hbase

        config.set("hbase.zookeeper.quorum","10.10.103.177,10.10.103.109,10.10.103.151");
        //config.set("hbase.zookeeper.quorum","hadoop01.ljnet.com,hadoop01.ljnet.com,hadoop01.ljnet.com");//连接zk节点
        config.set("hbase.zookeeper.property.clientPort","2181");//zk端口
        HTable htable=null;
        try {
            htable=new HTable(config,"table2");
            //row
         // String rowkey=System.currentTimeMillis()/1000+""+CommUtil.getSixRadom();
            Put put=new Put(Bytes.toBytes("1002"));//rowkey的值
            put.add(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("陈"));//列簇、列、值
            htable.put(put);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(htable!=null){
                try {
                    htable.close();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }
    }
}
