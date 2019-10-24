package com.shuting.Project_CleanInstrument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Organization implements Serializable {

	private String orgName = "";

	public Organization(String orgName) {
		this.orgName = orgName;
	}

	// 规则一：组织粒度的控制（论文中已经获取的组织名已经处理过，这里主要针对新添加的陕西省的组织信息）	
	public void rule1() {		
		if(orgName.contains("大学")) {			
			if(orgName.contains("医院")){					
				int index=orgName.indexOf("医院");
				orgName=orgName.substring(0, index+2);
			}
			else{
				int index=orgName.indexOf("大学");
				orgName=orgName.substring(0, index+2);
			}
		}
		else if(orgName.contains("学院")){			
			int index=orgName.indexOf("学院");
			orgName=orgName.substring(0, index+2);
		}			
	}	 
	
	// 规则二：小写转换为大写
	public void rule2() {
		orgName=orgName.toUpperCase();
	}
	
	// 规则三：〇规范为"O"（综合考虑规范城"O"最合适）
	public void rule3() {
		if (orgName.contains("〇")) {
			orgName = orgName.replaceAll("〇", "O");					
		} 
	}
	
	// 规则四：数字转汉字(String类型的字符串不能用"=="进行比较  eg:ch=="0"应该为ch.equals("0"))
	public void rule4() {
		Pattern p = Pattern.compile("[0-9]");
		Matcher m = p.matcher(orgName);
		while (m.find()) {			
			String ch = m.group();
			if (ch.equals("0")) {
				orgName = orgName.replaceAll("0", "O");
			} else if (ch.equals("1")) {
				orgName = orgName.replaceAll("1", "一");
			} else if (ch.equals("2")) {
				orgName = orgName.replaceAll("2", "二");
			} else if (ch.equals("3")) {
				orgName = orgName.replaceAll("3", "三");
			} else if (ch.equals("4")) {
				orgName = orgName.replaceAll("4", "四");
			} else if (ch.equals("5")) {
				orgName = orgName.replaceAll("5", "五");
			} else if (ch.equals("6")) {
				orgName = orgName.replaceAll("6", "六");
			} else if (ch.equals("7")) {
				orgName = orgName.replaceAll("7", "七");
			} else if (ch.equals("8")) {
				orgName = orgName.replaceAll("8", "八");
			} else if (ch.equals("9")) {
				orgName = orgName.replaceAll("9", "九");
			}
		}
	}
	
	//规则五：处理组织缩写问题
	public void rule5(){		
		ArrayList<String> orgOldSet=ConfigInf.orgOldSet;
		ArrayList<String> orgNewSet=ConfigInf.orgNewSet;	
		for(int i=0;i<orgOldSet.size();i++){ 			
			String orgOld=orgOldSet.get(i);
			if(orgName.contains(orgOld)){				
				int index=orgOldSet.indexOf(orgOld);
				String orgNew=orgNewSet.get(index);
				orgName=orgName.replaceAll(orgOld, orgNew);			
				break;
			}
		}		
	}
	public void rule6(){
		
		if(orgName.length()<4){
			orgName="";
		}
		else if(orgName.equals("暂无填写") || orgName.equals("暂时无填") || orgName.equals("暂时未填")){
			orgName="";			
		}	
	}
	
	public String handleOrg() {
		//总体思路：向汉字靠拢 ：0/o/〇规范为"O"（综合考虑规范城"O"最合适）		
		rule1();
		rule2();
		rule3();
		rule4();
		rule5();	
		rule6();
		return orgName;
	}
}
