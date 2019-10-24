package com.Hadoop.Project_Achieve_Hownet_Org;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

//对于知网中的论文数据进行处理，以组织为标准进行聚类
public class AchieveHownetOrg {


    public static class MyMapper extends  TableMapper<ImmutableBytesWritable,ImmutableBytesWritable>{
        //进行作业的配置
        protected void setup(Context context) throws IOException, InterruptedException{

            Configuration conf = context.getConfiguration();
            String filePath = conf.get("filePath");
            ConfigInf.initOrgMap(filePath);
        }

        //Map阶段
        public  void map(ImmutableBytesWritable row, Result values, Context context)
                throws  IOException,InterruptedException{
            //作者的拼音
            String autor= "";
            //基金类别
            String fundsproject = "";
            //论文作者
            String autors = "";
            //论文发表时间
            String time = "";
            //摘要
            String abstracts = "";
            //论文网址
            String url = "";
            //论文关键字
            String keyword = "";
            //论文发表期刊代号
            String index = "";
            //论文种类代码
            String sortnumber = "";
            //论文标题
            String title = "";
            //论文出版期刊名
            String publishinghouse = "";

            //对从Habse表中获取的数据进行提取、清洗
            Cell rawCell[] = values.rawCells();
            for (Cell cell : rawCell){
                if ("project_name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {

                }

            }
        }
    }

    public static void  main(String[] args) throws Exception{

    }

}
