import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

public class DBapp {

ArrayList<Table> tables;
String temp;
String file = "Tables";
public DBapp() throws FileNotFoundException, ClassNotFoundException, IOException{
	this.readFromFile();
}
public void writeTableToFile() throws IOException{
FileOutputStream newfile = new FileOutputStream(this.file);
ObjectOutputStream out = new ObjectOutputStream(newfile);
out.writeObject(this.tables);
out.close();
ByteArrayOutputStream bos = new ByteArrayOutputStream();

newfile.close();
}
public void readFromFile() throws IOException, ClassNotFoundException {
	ObjectInputStream in;
	try {
		in = new ObjectInputStream(new FileInputStream(this.file));
		this.tables = (ArrayList<Table>)in.readObject();
	} catch (FileNotFoundException e) {
		this.tables = new ArrayList<Table>();
		this.writeTableToFile();
		//e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws IOException  {
		Table table = new Table(strTableName, strClusteringKeyColumn,
				htblColNameType);
		tables.add(table);
		this.writeTableToFile();

	}

	@SuppressWarnings("deprecation")
	//the brin is updated after the insert
	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		ArrayList<String> colName = new ArrayList<String>();
		ArrayList<Object> colValue = new ArrayList<Object>();
		Table wantedTable = null;
		for(int i=0;i<tables.size();i++){
		if(tables.get(i).TableName.equals(strTableName)){
			 wantedTable = tables.get(i);
			break;
		}
		}
		///////////////////////////////////////////////////
		Enumeration e = htblColNameValue.keys();
		ArrayList<String> temp = new ArrayList<String>();
	    while (e.hasMoreElements()) {
	      String Column = (String) e.nextElement();
	      temp.add(Column);
	    }	
	    for(int i = temp.size()-1;i>=0;i--){
	    	colName.add(temp.get(i));
	    }
	    for(int i=0;i<colName.size();i++){
	    	colValue.add(htblColNameValue.get(colName.get(i)));
	    }
			/////////////////////////////
	    String insertedRecord = helper(wantedTable,htblColNameValue);
		if(wantedTable.pages==null){
			Page newpage = new Page(wantedTable.TableName + wantedTable.counter);
			wantedTable.counter ++;
			newpage.page.add(insertedRecord);
			newpage.writeToFile();
			wantedTable.pages = new ArrayList<Page>();
			wantedTable.pages.add(newpage);
			}
		else{
			String key = wantedTable.Key;
        	Object keyValue = htblColNameValue.get(key);
        	
        	wantedTable.insert(keyValue, insertedRecord);

		}
		wantedTable.updateBrinIndex();
		this.writeTableToFile();
	}
	public String helper(Table table,Hashtable<String,Object> Values) throws DBAppException{
		ArrayList<String> colType = table.ColType;
		ArrayList<String> colName = table.ColName;
		String record = "";
		if((Values.get(table.ColName.get(0))).getClass().toString().equals("class "+colType.get(0))){
		 record =Values.get(table.ColName.get(0))+"";
		for(int i=1;i<table.ColName.size();i++){
			if((Values.get(table.ColName.get(i))).getClass().toString().equals("class "+colType.get(i))){
			record += ","+ (Values.get(table.ColName.get(i)));
			
		}
			else{
				throw new DBAppException("Incorrect entry for the data1");
			}
			}}
		else{
			throw new DBAppException("Incorrect entry for the data2");
		}
		return record;
	}
	//the Brin index is updated after delete
	public void deleteFromTable(String strTableName,Hashtable<String, Object> htblColNameValue)throws DBAppException, FileNotFoundException, ClassNotFoundException, IOException{
		ArrayList<String> colName = new ArrayList<String>();
		ArrayList<Object> colValue = new ArrayList<Object>();
		Table wantedTable = null;
		
		for(int i=0;i<tables.size();i++){
			if(tables.get(i).TableName.equals(strTableName)){
				 wantedTable = tables.get(i);
				break;
			}}
		
		Enumeration e = htblColNameValue.keys();
		ArrayList<String> temp = new ArrayList<String>();
	    while (e.hasMoreElements()) {
	      String Column = (String) e.nextElement();
	      temp.add(Column);
	    }	
	    for(int i = temp.size()-1;i>=0;i--){
	    	colName.add(temp.get(i));
	    }
	    for(int i=0;i<colName.size();i++){
	    	colValue.add(htblColNameValue.get(colName.get(i)));
	    }
			/////////////////////////////
	    Object clusterKeyToDelete = htblColNameValue.get(wantedTable.ColName.get(0));
	    wantedTable.deleteRecord(clusterKeyToDelete);
	    wantedTable.updateBrinIndex();
		this.writeTableToFile();
	}
	public Iterator selectFromTable(String strTableName, String strColumnName, Object[] objarrValues,
            String[] strarrOperators){
		Iterator it = null ;
		for(int i=0;i<tables.size();i++){
			if(tables.get(i).TableName.equals(strTableName)){
				it = tables.get(i).selectValues(strColumnName, objarrValues, strarrOperators).iterator();
			}
		}
		return it;
	}
	
	public void createBRINIndex(String strTableName,String strColName ) throws ClassNotFoundException, IOException
	{ 
		for(int i=0;i<this.tables.size();i++){
			if(this.tables.get(i).TableName.equals(strTableName)){
				this.tables.get(i).CreateDense(strColName);
				this.tables.get(i).availIndicies.remove(strColName);
				this.tables.get(i).availIndicies.put(strColName, true);
				this.tables.get(i).writeToCsv();
				this.writeTableToFile();
				break;
			}
		}
		
	}
	//the BrinIndex is updated after update
	public void updateTable(String strTableName, String strKey,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException{
		Table wantedTable = null;
		for(int i=0;i<tables.size();i++){
		if(tables.get(i).TableName.equals(strTableName)){
			 wantedTable = tables.get(i);
			break;
		}
		
	  
		
	}
		  String record = helper(wantedTable,htblColNameValue);
			 wantedTable.updateTableHelper(record,strKey);
			 wantedTable.updateBrinIndex();
			 this.writeTableToFile();
}}
