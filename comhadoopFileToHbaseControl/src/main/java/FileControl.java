import java.io.IOException;

import javax.transaction.TransactionRequiredException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class FileControl {

    //用于解析论文数据集中的xml文件，并将解析后的数据内容存放至hbase数据表中：
    //hbase论文数据表结构
    //以论文文章名为rowkey，有Authors列族、Inf列族
    //Authors列族有：Authors：人名：单位简介
    //inf:abstracts:简介
    //inf:indexs:级别
    //inf:isFilter:Boolean
    //inf:keyword:论文关键字
    //inf:paperID:ID号
    //inf:publishinghouse:出版商
    //inf:sortnumber:排序号
    //inf:time:发表时间
    public static boolean FilexmlAnalysis(String fileName){
        try {
            //创建DOM解析器工厂
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            //获取解析器对象
            DocumentBuilder db = dbf.newDocumentBuilder();
            //调用DOM解析器对象paerse（string uri）方法得到Document对象
            Document doc = db.parse(fileName);
            //获得NodeList对象
            NodeList nl = doc.getElementsByTagName("Document");
            if (nl.getLength() == 0)
                return false;

            //遍历XML文件中的各个元素
            for (int i = 0; i < nl.getLength(); i++) {
                //得到Nodelist中的Node对象
                Node node = nl.item(i);
                //强制转化得到Element对象
                Element element = (Element) node;
                //获取各个元素的属性值
                String title = element.getElementsByTagName("title").item(0).getTextContent();
                String time = element.getElementsByTagName("time").item(0).getTextContent();
                String sortnumber = element.getElementsByTagName("sortnumber").item(0).getTextContent();
                String fundsproject = element.getElementsByTagName("fundsproject").item(0).getTextContent();
                String abstracts = element.getElementsByTagName("abstracts").item(0).getTextContent();
                //作者需要使用泛型
                //String autors

                //关键字需要使用泛型
                //String keyword
                String organization = element.getElementsByTagName("organization").item(0).getTextContent();

                //出版社需要泛型
                //String publishinghouse

                //index需要泛型
                //String index
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
        return true;
    }


    public static void  main(String[] args){
        System.out.println("开始解析");
        String url = "C:\\Users\\ZNW_Ha\\Desktop\\zhiwang_ScMguLP.xml";
        Boolean b = FilexmlAnalysis(url);
        if (b == true){
            System.out.println("解析完成");
        }
        else {
            System.out.println("解析失败");
        }
    }
}
