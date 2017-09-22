package com.guohualife.mpms.app.plugin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greenpineyu.fel.FelEngine;
import com.greenpineyu.fel.FelEngineImpl;
import com.greenpineyu.fel.context.FelContext;
import com.guohualife.mpms.app.plugin.base.Condition;
import com.guohualife.mpms.app.plugin.base.ConditionFactory;
import com.guohualife.mpms.app.plugin.base.MpmsPluginBase;
import com.guohualife.mpms.app.plugin.base.MpmsPluginResult;

public class KibanaMonitor extends MpmsPluginBase {
	protected static Log log = LogFactory.getLog(KibanaMonitor.class);
	private JSONObject kibanaData;
	/**
	 * @param props	页面上任务配置时的参数，对于kibana来说，必须包含脚本、FEL表达式、TEMPLATE，KEYS（和FEL表达式配合使用），PARAMS（模板中的参数）
	 * @param hashMap  初始值为plugin表的一条记录
	 */
	public KibanaMonitor(Map<String, String> props,
			HashMap<String, Object> hashMap) {
		super(props, hashMap);
	}

	@Override
	public List<MpmsPluginResult> eval() {
		List<MpmsPluginResult> result = new ArrayList<MpmsPluginResult>();
		//kibana参数以分号分割
		String params = (String)hashMap.get("PLUGIN_PROPS");
		String[] keyValue = params.split(";\n");
		for(String patternItem:keyValue){
			 if (!StringUtils.isEmpty(patternItem) && !patternItem.startsWith("#")) {
	                String[] split2 = StringUtils.split(patternItem, "=",2);
	                if(split2.length<2){
	                    props.put(split2[0].trim(), "");
	                }else{
	                    props.put(split2[0].replace("\n", "").trim(), split2[1]);
	                }
	                
	            }
		}
		// 取FEL表达式和校验参数,确定是否要发送邮件
		String felExpression = props.get("FEL_EXPRESSION");
		String keys = props.get("KEYS");
		if(felExpression == null || keys == null || "".equals(felExpression)){
			return result;
		}
		//配置的sql脚本
		String sql = props.get("SQL");
		sql = sql.replace("\n", " ");
		getKibanaData(sql);
		//需要发送邮件，取Kibana的数据
		int columnBeginIndex = sql.indexOf("select")+6;
		int columnEndIndex = sql.indexOf("from");
		Map<String , Object> row = new HashMap<String, Object>();//一行数据，dataList的一个元素
		String[] columns = sql.substring(columnBeginIndex, columnEndIndex).trim().split(",");//未经处理的查询列
		List<String> column = new ArrayList<String>();//所有查询的列
		Map<String,String> alias = new HashMap<String,String>();//别名
		List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();//存放和sql查询结果一样的结果数据，即把json解析成sql查询结果一样的数据
		//组装别名map
		for(String key : columns){
			key = key.trim();
			if(key.contains(" as ") || key.trim().split(" ").length>1){
				if(key.contains(" as ")){
					String tmpKey = key.split(" as ")[0];
					alias.put(tmpKey, key.split(" as ")[1]);
					key = tmpKey;
				}else{
					if(key.trim().indexOf(" ") > 0){
						String tmpKey = StringUtils.split(key, " ", 2)[0].trim();
						alias.put(tmpKey, StringUtils.split(key, " ", 2)[1].trim());
						key = tmpKey;
					}
				}
			}else{
				alias.put(key,key);
			}
			column.add(key);
		}
		long start = System.currentTimeMillis();
		log.info("kibanaData="+kibanaData.toJSONString());
		log.info("column="+column.toString());
		log.info("从查询结果中解析数据开始"+start);
		if(sql.contains(" group by ")){
			//结合Kibana数据和模版中用到的参数，生成参数对象   查询的列和group by分组的列，约定，顺序一致，内容一致
			getDataList(kibanaData.getJSONObject("aggregations"), row,column.get(0),column,alias,dataList,sql);
		}else{
			dataList = getSampleDataList(kibanaData,column,alias);
		}
		log.info("从查询结果中解析数据结束，耗时："+(System.currentTimeMillis()-start));
		log.info("结果数据条数为："+dataList.size());
		hashMap.put("data",dataList);
		
		// 初始化FEL引擎
		FelEngine engine = new FelEngineImpl();
		FelContext context = engine.getContext();
		Map<String, BigDecimal> felData = getFelData(keys,kibanaData);
		for (Map.Entry<String, BigDecimal> entry : felData.entrySet()) {
			context.set(entry.getKey(), entry.getValue());
			hashMap.put(entry.getKey(), entry.getValue());
		}
		Boolean calresult = (Boolean) engine.eval(felExpression, context);//计算fel表达式的值
		//取模板，将数据替换模板中的占位符  ${xxx}
		String template = loadStringProp("MAIL_TEMPLATE");
		StringBuffer newTemplate = dealMailTemplate(template, dataList);
		HashMap<String,String> newTemplateMap = new HashMap<String,String>();
		newTemplateMap.put("MAIL_TEMPLATE", newTemplate.toString());
        this.setNewTemplate(newTemplateMap);
        if(calresult){
        	result.add(new MpmsPluginResult("Y", hashMap));
        }
		return result;
	}
	
	/** 明细查询
	 * @param kibanaData  查出的原始json数据
	 * @param columns  查询的列（字段）
	 * @param alias  配置SQL中的别名，key=字段名，value=别名，没有别名时，key==value=字段名
	 * @return	和一般数据库查询结果相同结构的数据List<Map>
	 */
	private List<Map<String, Object>> getSampleDataList(JSONObject kibanaData,
			List<String> columns,Map<String,String> alias) {
		List<Map<String, Object>> dataList = new ArrayList<Map<String,Object>>();
		JSONArray hits = ((JSONObject)kibanaData.get("hits")).getJSONArray("hits");//查询结果数组默认放在 hits.hits[]
		for(int i = 0;i<hits.size();i++){
			Map<String,Object> row = new HashMap<String,Object>();
			JSONObject source = (JSONObject) ((JSONObject)hits.get(i)).get("_source");//查询的字段数据都放在_source下面，暂时不考了_source同级的字段
			for(String column : alias.keySet()){
				row.put(alias.get(column), source.get(column));
			}
			dataList.add(row);
		}
		return dataList;
	}

	/**获取fel表达式中包含的key及其value组成的map
	 * @param keys fel表达式中的key，在配置参数中有，示例：hits.total,hits.score
	 * @param kibanaData kibana查询出的数据（json格式）
	 * @return	fel表达式中的所有key和该key在kibanaData中的value组成的map
	 * 2017-08-21
	 */
	private Map<String, BigDecimal> getFelData(String keys,
			JSONObject kibanaData) {
		Map<String, BigDecimal> result = new HashMap<String,BigDecimal>();
		String[] key = keys.replace("，", ",").split(",");
		try{
			for(int i = 0;i<key.length;i++){
				BigDecimal value = (BigDecimal)getJsonValue(kibanaData,key[i],true);
				log.info("计算FEL表达式：key="+key[i]+",value="+value.toString());
				result.put("KEY"+i, value);//用于计算fel表达式的值
				result.put(key[i], value);//保证其他地方能够使用这个key
			}
		}catch(Exception e){
			log.error("计算表达式异常，keys="+keys);
		}
	
		return result;
	}

	/**
	 * @param kibanaData 从kibana查出的json数据对象
	 * @param key	键，例如：hits.total,hits.aggresions.buckets[1].aa.buckets[2].key
	 * @param isNumber	返回值是否为数值类型
	 * @return    kibana查询结果中对应key的值  String/Bigdecimal,或者ArrayList<String>/ArrayList<Bigdecimal>
	 * 目前只支持key中包含单个数组的情况，例如hits.hit[2].group,不支持hits.hit[2].group[1].key
	 * 2017-08-21
	 * 2017-08-22 完善
	 */
	private Object getJsonValue(JSONObject kibanaData,String key,boolean isNumber) {
		/*
		 * 以.分割对象属性引用，循环取到最终结果
		 * 如果key=hits.hits[]，那么返回JSONArray,如果key=hits.hits[1].group，那么返回具体值
		 * 替换调回车符，因为可能sql太长，字段不在同一行
		 */
		String[] parts = key.replaceAll("\n", "").split("\\.");
		JSONObject midData = kibanaData ;
		for(int i = 0;i < parts.length;i++){
			if(parts[i].indexOf("[")>0 ){
				if(midData.get(parts[i].substring(0, parts[i].indexOf("["))) instanceof JSONArray){//2017-08-23 完善
					int beginIndex = parts[i].indexOf("[")+1;
					JSONArray jsonArray = null;
					try {
						jsonArray = (JSONArray)midData.get(parts[i].substring(0, beginIndex-1));
						int endIndex = parts[i].indexOf("]");//数组长度可能是一位数，也可能是更多位数
						midData = jsonArray.getJSONObject(Integer.parseInt(parts[i].substring(beginIndex, endIndex)));
					} catch (NumberFormatException e) {
						log.error("配置参数有误，参数为："+parts[i]);
						e.printStackTrace();
						//如果key是以[]结尾，那么返回ArrayList<String>，此种情况出现在模板中，模板中会有hits[i].key
						//去数值计算fel的时候，取的是单个数值，不会是数组，故此处就不考虑数值数组
						ArrayList<String> result = new ArrayList<String>(jsonArray.size());
						//返回数组  例如，hits.hits[].group.keyword,将会返回ArrayList<String>
						//LOOPSTART:hits.hits[]#  模板中的这个位置应该这么写
						//模板中循环体 中  写法是：hits.hits[].group.keyword
						if(i != parts.length -1){
							for(int j=0;j<jsonArray.size();j++){
								result.add(j, getJsonValue(kibanaData, key.replace("[]", "["+j+"]"), false).toString());
							}
							return result;
						}else{
							return new Integer(jsonArray.size());//专门为循环体次数设置的值
						}
						
					}catch(Exception e){
						log.error("转换JSONArray异常,json中的"+parts[i]+"不是数组，json为:"+kibanaData.toJSONString());
					}
				}else{
					return parseJsonError(kibanaData,parts[i]);
				}
			}else{
				if(i != parts.length -1){
					midData = (JSONObject)(midData.get(parts[i]));
				}else {
					if(isNumber){
						return  midData.getBigDecimal(parts[i]);
					}else{
						return midData.get(parts[i]);
					}
				}
			}
		}
		return null;
	}

	/**
	 * @param kibanaData 查询kibana的结果json
	 * @param string  取结果中的该key的值
	 * @return  转换失败时默认返回值
	 * 2017-08-22
	 */
	private Object parseJsonError(JSONObject kibanaData, String string) {
		log.error("从json中取值错误，json数据为："+kibanaData.toJSONString()+"\n"+"取值的key为："+string);
		return null;
	}

	/**处理模板中的循环
	 * @param template		模板字符串，包含换行\n
	 * @param dataMap		保存有模板中存在的key及其value
	 * @return	处理过的模板字符串
	 * 2017-08-22
	 */
	private StringBuffer dealMailTemplate(String template,
			List<Map<String, Object>> dataList) {
		if(StringUtils.isEmpty(template)){
            StringBuffer result = new StringBuffer();
            result.append("未设置邮件模板!");
            return result;
        }

        String[] split = StringUtils.split(template, "\n");
        List < String > lines = Arrays.asList(split);

        StringBuffer result = new StringBuffer("");

        int size = lines.size();
        // 模板行游标
        int lineCursor = 0;
        for (;;) {
            String string = lines.get(lineCursor);
            if (string.trim().startsWith("LOOP:")) {
            	String templateLoopPart = string.trim().substring(5);
                Set<String> keySet = dataList.get(0).keySet();
                String part = "";
                for(Map<String,Object> row:dataList){
                	for(String column:keySet){
                		part = templateLoopPart.replaceAll("${"+column+"}"	, row.get(column).toString());
                	}
                	result.append(part).append("\n");
                }
            }
            // 循环区域处理
            else if (string.trim().startsWith("LOOPSTART:")) {
                String templateLoopPart = new String("");
                for (;;) {
                    lineCursor++;
                    if (lineCursor >= size) {
                        break;
                    }
                    String string2 = lines.get(lineCursor);
                    if (string2.trim().startsWith(("LOOPEND:"))) {
                        break;
                    }
                    else {
                    	templateLoopPart = templateLoopPart + string2 + "\n";
                    }
                }
                Set<String> keySet = dataList.get(0).keySet();
                for(Map<String,Object> row:dataList){
                	String loopResult = templateLoopPart;
                	for(String column:keySet){
                		loopResult = loopResult.replace("${"+column+"}"	, row.get(column).toString());
                	}
                	result.append(loopResult);
                }
            }
            // 普通行
            else {
                result.append(string + "\n");
            }

            lineCursor++;
            if (lineCursor >= size) {
                break;
            }
        }
        return result;
	}

	/**
	 * @param currJson  当前处理到的json对象，分组时，会有层级的概念，每一个层级表示一个分组条件，每一个层级内的所有下级使用相同的当前层级的所有参数
	 * 	例如，group by a,b,c,d，那么当遍历到c时，c层级使用的公共参数为a层级参数+b层级参数，类似于树形结构，子节点几成父节点的属性
	 * @param row	 用来保存之前所有层级的已获得的参数
	 * @param  curColumnName   当前列名，group by 分组字段中的一个，用来和别名map一起取别名
	 * @param  columns  所有的分组字段，用来结合当前字段，获取下一个字段。有顺序。
	 *  @param  alias 别名map，key=字段名，value=别名，没有别名的字段，key==value==字段名
	 *  @param  dataList，最终结果数据
	 */
	private void getDataList(JSONObject currJson, Map<String,Object> row,String curColumnName,List<String> columns,Map<String,String> alias,
			List<Map<String,Object>> dataList,
			String sql) {
		if("".equals(curColumnName)){//当前字段为空，表示，所有group by字段都处理完毕，可以将row中的数据加到最终结果数据里了 
			row.put("count", currJson.get("doc_count"));
			/*每条数据都得是独立内存空间，新的内存地址，不能使用row
			 * dataList.add(map)实际动作是，将dataList当前索引（下标）的值，指向map的内存地址，如果使用row，而row始终不变指向一个地址，
			 * 那么最终结果是dataList里所有的map都是一样的
			 */
			Map<String,Object> newRow = new HashMap<String,Object>();
			newRow.putAll(row);
			dataList.add(newRow);
		}else{
			JSONObject json = (JSONObject)currJson.get(curColumnName);//由于，代码上，默认以列名作为分组的名称，所以此处参数为curColumnName
			JSONArray jsonArray = json.getJSONArray("buckets");
			Map<String,Object> nextRow = new HashMap<String,Object>();
			nextRow.putAll(row);
			for(int j = 0;j<jsonArray.size();j++){
				JSONObject nextJson = (JSONObject) jsonArray.getJSONObject(j).clone();
				row.put(alias.get(curColumnName), nextJson.get("key"));//把当前列的值放入  行  row里面
				nextRow.putAll(row);
				getDataList(nextJson, nextRow, getNextColumn(columns,curColumnName),columns, alias, dataList, sql);
			}
		}
	}

	/**
	 * @param columns  所有查询字段数组，有序的
	 * @param curColumnName	当前字段
	 * @return	下一个字段
	 */
	private String getNextColumn(List<String> columns,String curColumnName){
		for(int i = 0;i<columns.size();i++){
			if(columns.get(i).equals(curColumnName)){
				if(i != columns.size()-1){
					return columns.get(i+1);
				}else{
					return "";
				}
			}
		}
		return "";
	}
	
	private void getKibanaData(String sql){
		try {
			sql = replaceDate(sql);
			int beginIndex = sql.indexOf("from")+4;
			int endIndex  = sql.indexOf("where");
			String indecies = sql.substring(beginIndex, endIndex).trim();
			String ip = props.get("IP");
			String port = props.get("PORT");
			if(StringUtils.isEmpty(ip) || StringUtils.isEmpty(port)){
				log.info("kibana配置错误，没有配置ip或者端口");
				throw new Exception("配置错误");
			}
			String urlStr = "http://"+ip+":"+port+"/"+indecies+"/_search";
			URL url = new URL(urlStr);
            log.info("kibana监控：连接服务器开始，url为："+urlStr);
			HttpURLConnection  connection = (HttpURLConnection) url.openConnection();    
			connection.setDoOutput(true);  
            connection.setDoInput(true);  
            connection.setRequestMethod("POST"); // 可以根据需要 提交 GET、POST、DELETE、INPUT等http提供的功能  
            connection.setUseCaches(false);  
            connection.setInstanceFollowRedirects(true);  
              
            //设置http头 消息  
            connection.setRequestProperty("Content-Type","application/json");  //设定 请求格式 json，也可以设定xml格式的  
            //connection.setRequestProperty("Content-Type", "text/xml");   //设定 请求格式 xml，  
            connection.setRequestProperty("Accept","application/json");//设定响应的信息的格式为 json，也可以设定xml格式的  
            connection.connect();  
            //组装查询json字符串
            beginIndex = endIndex +5;
            endIndex = sql.indexOf("group by")>0?sql.indexOf("group by"):sql.length();
            String[] conditions = sql.substring(beginIndex, endIndex).trim().split("and");
            ArrayList<String> musts = new ArrayList<String>();
            ArrayList<String> mustNots = new ArrayList<String>();
            for(String condition:conditions){
            	Condition cond = ConditionFactory.newInstance(condition.trim());
            	if(cond.getType().equals("must")){
            		musts.add(cond.toString());
            	}else if(cond.getType().equals("mustNot")){
            		mustNots.add(cond.toString());
            	}
            }
            //取参数top：查询结果显示前多少条记录，类似于oracle的rownum<=top
            Object top = props.get("TOP");
            StringBuilder script = new StringBuilder("{\"query\":{");
            StringBuilder endStr = new StringBuilder("}");
            if(top != null){
            	script.replace(0, 1, "{\"size\":"+top+",");
            }
            if(!musts.isEmpty() || !mustNots.isEmpty()){
            	script.append("\"bool\":{");
            	//endStr.append("}");
            	if(!musts.isEmpty()){
            		script.append("\"must\":[");
            		fillBoolScript(script,musts);
            	}
            	if(!mustNots.isEmpty()){
            		if(!musts.isEmpty()){
            			script.append(",");
            		}
            		script.append("\"must_not\":[");
            		fillBoolScript(script,mustNots);
            	}
            	script.append("}");//bool的结束
            }
            //query结束，添加结束}
            script.append("}");
            fillGroupbyScript(script,sql);
            //String finalScript = 
            /*取参数order：对查询结果排序，包括排序字段和排序类型（默认降序desc，升序为asc）
             * 排序字段必填  配置时类似  ORDER:@timestamp asc,group.keyword desc;
             */
            Object order = props.get("ORDER");
            if(order != null){
            	String[] orders = order.toString().split(",");
            	int length = orders.length;
            	script.append(",\"sort\":[");
            	//循环所有排序
        		for(int i=0;i<length;i++){
        			script.append(getSortStr(orders[i]));
        			if(length != 1 && i != length-1){
        				script.append(",");
        			}
        		}
            	script.append("]");//补上sort的结尾}
            }
            script.append(endStr);//结束顶级的｛｝
            log.info("转换后的查询脚本为：\n"+script);
            JSONObject data = JSONObject.parseObject(script.toString());
            OutputStream out = connection.getOutputStream();                          
            out.write(data.toString().getBytes());  
            out.flush();  
            out.close();
			BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(),"utf-8")); // 获取输入流
			String line = null;
			StringBuilder sb = new StringBuilder();
			while ((line = br.readLine()) != null) {
			    sb.append(line + "\n");
			}
			JSONObject jsonObject = JSON.parseObject(sb.toString());
			this.kibanaData = jsonObject;
			/*JSONArray datas = (JSONArray)((JSONObject)((JSONObject)jsonObject.get("aggregations")).get("aggName")).get("buckets");
			Object[] result = datas.toArray();
			for(Object item:result){
				System.out.println("系统名称："+((JSONObject)item).get("key")+",  异常数量："+((JSONObject)item).get("doc_count")+"\n");
			}*/
			System.out.println("查询结果为："+sb.toString());
		} catch (MalformedURLException e) {
			log.info("kibana异常：MalformedURLException");
			log.info("kibana异常信息："+e.getMessage());
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			log.info("kibana异常：UnsupportedEncodingException");
			log.info("kibana异常信息："+e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			log.info("kibana异常信息："+e.getMessage());
			log.info("kibana异常：IOException");
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * @param string 排序字符串，例如 group.keyword asc    排序方式可选，默认排序方式为降序  desc
	 * @return	在查询脚本中可以使用的排序字符串 例如 {"group.keyword":{"order":"asc"}}
	 */
	private String getSortStr(String string) {
		// TODO Auto-generated method stub
		String[] sortParts = StringUtils.split(string, " ",2);
		if(sortParts.length == 1){
			return "{\""+sortParts[0].trim()+"\":{\"order\":\"desc\"}}";
		}else{
			return "{\""+sortParts[0].trim()+"\":{\"order\":\""+sortParts[1].trim()+"\"}}";
		}
	}

	/**
	 * @param script  已经组装好的query脚本
	 * @param sql  配置的查询sql
	 */
	private void fillGroupbyScript(StringBuilder script, String sql) {
		// TODO Auto-generated method stub
		if(sql.indexOf(" group by ") < 0){
			return;
		}
		script.append(",");//分组查询  和  query  平级，要用逗号分开
		String groupColumnStr = null;
		int beginIndex = sql.indexOf(" group by ")+10;
		groupColumnStr = sql.substring(beginIndex);//例如：beat.hostname.keyword order by _count,group.keyword
		String[] groupColumns = groupColumnStr.split(",");
		int length = groupColumns.length;//填充尾部的}时使用
		for(int i=0;i<length;i++){
			script.append(getAggPrePart(groupColumns[i]));
			if(i != length-1 && length != 1){
				script.append(",");
			}
		}
		for(int i=0;i<length;i++){
			//if(i != length-1){
				script.append("}}");
			/*}else{
				script.append("}");
			}*/
			
		}
		//script.append("}");
	}
	
	/**
	 * @param column  group by 的列，一般是单一的列名，如group.keyword order by xx asc
	 * 可以增加排序和取前多少条记录的配置，配置信息以 空格 分割，内容是
	* top 4 order by _term/_count  asc/desc _term表示，按照排序字段的值排序,_count表示按照分组后的count(1)排序
	 * @return  group by 对应的聚合字符串（不包含结尾},因为多字段group by时，
	 * 后面的aggs是嵌套在前面的aggs里面的，所以只能在把所有group by都组装完毕后，
	 * 有多少个group by字段就追加多少个}即可
	 */
	private String getAggPrePart(String column){
		column = column.trim();
		String topLimit = "";
		String orderType = null;
		String orderValue = null;
		boolean spaceFlag = false;//真正的column是否需要截取到空格的位置
		int size = -1;
		//取  显示前多少条数据的值
		if(column.indexOf(" top ")>0){
			spaceFlag = true;
			String leftStr = column.substring(column.indexOf("top")+3).trim();
			try{
				topLimit = leftStr.substring(0, leftStr.indexOf(" "));
				size = Integer.parseInt(topLimit);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		//取排序字段和排序类型
		if(column.indexOf(" order by ")>0){
			spaceFlag = true;
			String leftStr = column.substring(column.indexOf("order by")+8).trim();
			String[] parts = leftStr.split(" ");
			int length = parts.length;
			if(length == 1){
				orderType = "\"_term\":";
				orderValue = "\""+parts[0]+"\"";
			}else if(length == 2){
				orderType = "\""+parts[0]+"\":";
				orderValue = "\""+parts[1]+"\"";
			}
		}
		column = column.substring(0, spaceFlag?column.indexOf(" "):column.length());
		String result = "\"aggs\": {\n" +
								"    \""+column+"\": {\n" + 
								"      \"terms\": {\n" + 
								"        \"field\": \""+column+"\"\n" + (size == -1?"":",\"size\":"+size)+
								""+(orderType == null?"":",\"order\":{"+orderType+orderValue+"}")+
								"      }";
		log.info("分组字符串为："+result);
		return result;
	}

	protected Object loadObjectProp(String key) {
        return hashMap.get(key);
    }

    protected String loadStringProp(String key) {
        Object object = loadObjectProp(key);
        if (object != null) {
            return object.toString();
        }
        else {
            return null;
        }
    }
    
    public Map<String,String> getProps(){
    	return this.props;
    }

    /**
     * @param kibanaData  查询出的kibanaData对象
     * @param oldKey 例如  hits.aggressions.buckets[].myagg.buckets[].key  包含至少一个[]
     * @return 将oldKey中的所有[]替换成[数字],数字的取值范围是  该数组在kibanaData中的长度
     * 例如hits.aggressions.buckets[]  ，hits.aggressions.buckets在kibanaData中长度是4，那么该[]中的数字就是0-3
     * 
     */
    private ArrayList<String> getNewKeys(JSONObject kibanaData,String oldKey){
    	ArrayList<String> newKeys = new ArrayList<String>();
    	if(oldKey.indexOf("[]")<0){
    		//log.error("错误的调用，方法：getNewKeys,oldKey="+oldKey+"\n kibanaData="+kibanaData.toJSONString());
    		newKeys.add(oldKey);
    		return newKeys;
    	}else{
    		String tmpKey = oldKey.substring(0,oldKey.indexOf("[]"));
    		int length = getJsonArrayLength(kibanaData, tmpKey);
    		for(int i=0;i<length;i++){
    			String nextKey = oldKey.replaceFirst("\\[\\]", "["+i+"]");
    			newKeys.addAll(getNewKeys(kibanaData, nextKey));
    		}
    	}
    	return newKeys;
    }
    
    /**
     * @param kibanaData 原始查处的数据
     * @param key	包含两种情况：1、hits.aggressions.buckets,2、hits.aggressions.buckets[2].aggname.buckets
     * @return
     */
    private int getJsonArrayLength(JSONObject kibanaData,String key){
    	String[] parts = key.split("\\.");//分割json的层级引用，所以，自定义的
		JSONObject midData = kibanaData ;
		int result = 0;
		for(int i = 0;i < parts.length;i++){
			if(parts[i].indexOf("[")>0 ){//循环到第二种情况的buckets[2]处
				if(midData.get(parts[i].substring(0, parts[i].indexOf("["))) instanceof JSONArray){//2017-08-23 完善
					int beginIndex = parts[i].indexOf("[")+1;
					JSONArray jsonArray = null;
					try {
						jsonArray = (JSONArray)midData.get(parts[i].substring(0, parts[i].indexOf("[")));
						//因为数组长度可能是一位数、两位数、任意位数，所以，使用  ]  的下表作为截取endIndex
						int endIndex = parts[i].indexOf("]");
						midData = jsonArray.getJSONObject(Integer.parseInt(parts[i].substring(beginIndex, endIndex)));
					} catch (NumberFormatException e) {//如果key是以[]结尾，那么返回ArrayList<String>，此种情况出现在模板中，模板中会有hits[i].key
						log.error("getJsonArrayLength：配置参数错误！取参引用是："+parts[i]+"\n  kibanaData为："+kibanaData.toJSONString());
						e.printStackTrace();
					}catch(Exception e){
						log.error("转换JSONArray异常,json中的"+parts[i]+"不是数组，json为:"+kibanaData.toJSONString());
					}
				}else{
					parseJsonError(kibanaData,parts[i]);
					return 0;
				}
			}else{
				if(i == parts.length-1){
					try{
						result = ((JSONArray)midData.get(parts[i])).size();
					}catch(Exception e){
						log.error("配置参数错误！取参引用是："+parts[i]+"\n  kibanaData为："+kibanaData.toJSONString());
					}
				}else{
					midData = (JSONObject)(midData.get(parts[i]));
				}
			}
		}
		return result;
    }
    
    /**
     * @param sql  配置的kibana查询sql
     * @return  替换调${today}和${lastDay}后的结果
     */
    private String replaceDate(String sql){
    	Date now = new Date();
    	Date today = DateUtils.truncate(now, Calendar.DAY_OF_MONTH);
    	Date lastDay = DateUtils.addDays(today, -1);
    	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	String todayStr =  df.format(today).replaceFirst(" ", "T");
    	String lastDayStr = df.format(lastDay).replaceFirst(" ", "T");
    	return sql.replaceAll("\\$\\{today\\}", todayStr).replaceAll("\\$\\{lastDay\\}", lastDayStr);
    }

    private void fillBoolScript(StringBuilder script,ArrayList<String> source){
    	for(int i=0;i<source.size();i++){
			if(i != source.size()-1){
				script.append(source.get(i)).append(",");
			}else{
				script.append(source.get(i)).append("]");
			}
		}
    }
    
    /**
     * @param originalLongestKey   查询语句中，最长的那个字段 例如：aggregations.group-keyword.buckets[].beat-hostname-keyword.buckets[].key
     * 		original意思是原始的，就是中括号里面没有数字的
     *     作用是  用来在最后替换  html循环体时，当作一个样例，从这个样例中取  待替换的字符串，详见replacedMatcher的相关操作
     * @param longestKey   每次递归调用都不一样的最长的key，随着递归的深度，该字段中的[]逐个被替换成[数字]
     *     当循环到，该值中不包含[]时，则开始替换，替换方案是，将原始最长字段中的每一段，替换成  该值的每一段，故采用正则匹配
     * @param resultTemplates   html循环体被替换后的每一个循环体的  字符串，假设，循环体是${a[].key},且在kibanaData中a的长度为2
     *    那么，resultTemplates的结构为{["${a[0].key}","${a[1].key}"]}
     * @param originalTemplate   原始模板循环体，item表中的模板中的循环体，页面配置的
     */
    private void getTemplateLoopPartList(String originalLongestKey,String longestKey,List<String> resultTemplates,String originalTemplate){
    	//TODO
    	//longestKey中 []  的个数
    	int length = longestKey.split("\\[\\]").length-1;
    	if(length >= 1){
    		for(int i = 0;i<length;i++){
        		int jsonArrayLength = getJsonArrayLength(kibanaData, longestKey.substring(0, longestKey.indexOf("[]")));
        		for(int j = 0;j<jsonArrayLength;j++){
        			String newKey = longestKey.replaceFirst("\\[\\]", "["+j+"]");
        			getTemplateLoopPartList(originalLongestKey,newKey,resultTemplates,originalTemplate);
        		}
        	}
    	}else{
    		Pattern replacedPattern = Pattern.compile(".+?\\[\\]\\.");
    		Matcher replacedMatcher = replacedPattern.matcher(originalLongestKey);
    		Pattern valuePattern = Pattern.compile(".+?\\[\\d+\\]\\.");
    		Matcher valueMatcher = valuePattern.matcher(longestKey);
    		
    		 while(replacedMatcher.find()){
    			 if( valueMatcher.find()){
    				 originalTemplate = originalTemplate.replaceAll(replacedMatcher.group().replace("[", "\\[").replace("]", "\\]"), valueMatcher.group());
    			 }
    		 }
    		 resultTemplates.add(originalTemplate);
    	}
    	
    }
}
