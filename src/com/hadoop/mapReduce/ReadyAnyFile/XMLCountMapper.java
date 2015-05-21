package com.hadoop.mapReduce.ReadyAnyFile;

import java.io.IOException;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class XMLCountMapper extends Mapper<LongWritable, Text, Text, Text> { 
	HashMap<String, String> map = new HashMap<String, String>();
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration job = context.getConfiguration();
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		//String filename = fileSplit.getPath().getName();
		final Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(fileSplit.getPath());
		try{
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(fileIn);
		doc.getDocumentElement().normalize();
		NodeList nodeLst = doc.getElementsByTagName("book");
		for (int s = 0; s < nodeLst.getLength(); s++) {
			String mapKey=null;
			String mapValue=null;
			NodeList yearlstNm =null;
			NodeList lstNm = null;
			Node fstNode = nodeLst.item(s);

			if (fstNode.getNodeType() == Node.ELEMENT_NODE) {

				Element fstElmnt = (Element) fstNode;
				/*NodeList fstNmElmntLst = fstElmnt.getElementsByTagName("author");
				Element fstNmElmnt = (Element) fstNmElmntLst.item(0);
				NodeList fstNm = fstNmElmnt.getChildNodes();*/

				NodeList lstNmElmntLst = fstElmnt.getElementsByTagName("title");
				Element lstNmElmnt = (Element) lstNmElmntLst.item(0);
				lstNm = lstNmElmnt.getChildNodes();

				NodeList yearNmElmntLst = fstElmnt.getElementsByTagName("publish_date");
				Element yearNmElmnt = (Element) yearNmElmntLst.item(0);
				yearlstNm = yearNmElmnt.getChildNodes();
				
				mapValue = ((Node) lstNm.item(0)).getNodeValue();
				mapKey = ((Node) yearlstNm.item(0)).getNodeValue();
				
			}
			if(!map.containsKey(mapValue)){
				map.put(mapValue,mapKey);
				context.write(new Text(mapKey), new Text(mapValue));
			}
			
		}
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
		
	}
}