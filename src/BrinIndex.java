import java.io.Serializable;
import java.util.ArrayList;


public class BrinIndex implements Serializable{
ArrayList<Page> index;
String colName;
String colType;
int counter = 0;
public BrinIndex(String colType,String colName){
	this.index= new ArrayList<Page>();
	this.colType=colType;
	this.colName = colName;
}
}
