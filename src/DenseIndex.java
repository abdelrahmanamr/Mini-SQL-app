import java.io.Serializable;
import java.util.ArrayList;


public class DenseIndex implements Serializable{
    public ArrayList<Page> values;
    public String colName;
    public String colType;
    public String indexName;
    public int counter =0;
	public DenseIndex(String colName,String colType,String indexName) {
     values = new ArrayList<Page>(); 
     this.colName= colName;
     this.colType = colType;
     this.indexName = indexName;
     }
   public void createNewPage(){
	   Page x = new Page(indexName + counter);
	   counter++;
	   values.add(x);
   }

}