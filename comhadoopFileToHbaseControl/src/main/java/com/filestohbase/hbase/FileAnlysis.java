package com.filestohbase.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class FileAnlysis {

    //通过文件路径来对XML文件进行解析
    private static HbaseItem FilexmlAnalysis(String filePath,HbaseItem hItem){

        try {

            //存储重复元素类中重复的个数
            int num = 0;

            //创建DOM解析器工厂
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            //获取解析器对象
            DocumentBuilder db = dbf.newDocumentBuilder();
            //调用DOM解析器对象paerse（string uri）方法得到Document对象
            Document doc = db.parse(filePath);
            //获得NodeList对象
            NodeList nl = doc.getElementsByTagName("Document");
//            if (nl.getLength() == 0)
//                return false;

            //遍历XML文件中的各个元素
            for (int i = 0; i < nl.getLength(); i++) {
                //得到Nodelist中的Node对象
                Node node = nl.item(i);
                //强制转化得到Element对象
                Element element = (Element) node;
                //获取各个元素的属性值
                hItem.title = element.getElementsByTagName("title").item(0).getTextContent();
                hItem.time = element.getElementsByTagName("time").item(0).getTextContent();
                hItem.sortnumber = element.getElementsByTagName("sortnumber").item(0).getTextContent();
                hItem.fundsproject = element.getElementsByTagName("fundsproject").item(0).getTextContent();
                hItem.abstracts = element.getElementsByTagName("abstracts").item(0).getTextContent();
                hItem.organization = element.getElementsByTagName("organization").item(0).getTextContent();

                //作者需要使用泛型
                num = element.getElementsByTagName("autors").getLength();
                for (int j = 0; j < num; j++) {
                    hItem.autors.add(element.getElementsByTagName("autors").item(j).getTextContent());
                }

                //关键字需要使用泛型
                //String keyword
                num = element.getElementsByTagName("keyword").getLength();
                for (int j = 0; j < num; j++) {
                    hItem.keyword.add(element.getElementsByTagName("keyword").item(j).getTextContent());
                }


                //出版社需要泛型
                //String publishinghouse
                num = element.getElementsByTagName("publishinghouse").getLength();
                List<String> publishinghouse = new ArrayList<String>();
                for (int j = 0; j < num; j++) {
                    hItem.publishinghouse.add(element.getElementsByTagName("publishinghouse").item(j).getTextContent());
                }

                //index需要泛型
                //String index
                num = element.getElementsByTagName("index").getLength();
                List<String> index = new ArrayList<String>();
                for (int j = 0; j < num; j++) {
                    hItem.index.add(element.getElementsByTagName("index").item(j).getTextContent());
                }

                //测试用
                //System.out.println("论文: " + title +"  "+ time + "  " + sortnumber + "  " + fundsproject + "  " + abstracts);
            }
        }catch (ParserConfigurationException e){
            e.printStackTrace();
        }catch (SAXException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }
        return hItem;
    }

    //对于所存入泛型的HbaseItem内的数据进行处理，放入到hbase数据库中
    private static boolean FileToHbase(String tableName,HbaseItem hitem){



        try {
            Configuration HBASE_CONFIG = new Configuration();

//        //建表所用到的代码
//        //HBASE_CONFIG.set("hbase.zookeeper.quorum", "");
//
//        String tableName = "";
//        String family="";
//        HBaseAdmin hBaseAdmin = new HBaseAdmin(HBASE_CONFIG);
//
//        if (hBaseAdmin.tableExists(tableName)) {
//            hBaseAdmin.disableTable(tableName);
//            hBaseAdmin.deleteTable(tableName);
//            System.out.println(tableName + " is exist,detele....");
//        }
//
//        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
//        HColumnDescriptor cf= new HColumnDescriptor(family);
//        htd.addFamily(cf);
//        hBaseAdmin.createTable(htd);
//        hBaseAdmin.close();
            String key = hitem.title;
            String family1 = "";
            String family2 = "";
            String family3 = "";

            HTable HBasetable = new HTable(HBASE_CONFIG,TableName.valueOf(tableName));

            List<Put> puts = new ArrayList<Put>();
            Put put1 = new Put(key.getBytes());
            put1.add(Bytes.toBytes("colfaml"),Bytes.toBytes("qual1"),Bytes.toBytes("val1"));
            puts.add(put1);

            Put put2 =  new Put(key.getBytes());
            put2.add(Bytes.toBytes("colfam2"),Bytes.toBytes("qual2"),Bytes.toBytes("val2"));
            puts.add(put2);

            Put put3 =  new Put(key.getBytes());
            put3.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual2"),Bytes.toBytes("val3"));
            puts.add(put3);

            HBasetable.put(puts);
            HBasetable.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return true;
    }

    //通过给定文件夹路径，查找该文件夹下的所有文件及文件名，并存储至泛型中
    //input：文件夹路线
    //output：该文件夹下所有文件的绝对路径
    private static List<String> XMLFilePath(String localfilePath){
        List<String> filepath = new ArrayList<String>();
        // get file list where the path has
        File file = new File(localfilePath);
        // get the folder list
        File[] array = file.listFiles();

        for (int i = 0; i < array.length; i++) {
            if (array[i].isFile()) {
                //获取文件名，文件路径，并存储到filepath中
                // System.out.println("*****" + array[i].getPath());
                filepath.add(array[i].getPath());
            }
//           //用于递归调用文件夹，存储文件（当使用该部分代码时，须将泛型filepath提至全局变量，否则会出现数据覆盖的情况）
//           else if (array[i].isDirectory()) {
//                XMLFilePath(array[i].getPath());
//            }
        }
        return  filepath;
    }


    //控制整个程序的运行
    //mian函数的输入参数为 0：文件夹的位置  1：操作的Hbase表的名称  2：对于文件夹切分的块的大小
    public static void main(String[] args){
//        //测试用
//        List<String> filepath = XMLFilePath("E:\\学习资料\\科技厅项目\\科技大数据项目\\spmary0828");
//        for(String path : filepath){
//            System.out.println(path);
//        }
        String TableName = args[1];
        List<String> filepath = XMLFilePath(args[0]);
        HbaseItem hbaseitom  = new HbaseItem();
        //通过只new一个HbaseItem对象，减少系统的内存开销，
        //通过文件路径对每个文件进行处理
        for (int i = 0; i < filepath.size(); i++) {
            FilexmlAnalysis(filepath.get(i),hbaseitom);
            FileToHbase(TableName,hbaseitom);
        }
    }
}
