
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable{
public ArrayList<String>page = new ArrayList<String>();
private int maxRecords= 3;
private String file;
public Page(String filename){
	page = new ArrayList<String>();
	this.file = filename;
}
//public Page(){
//	page = new ArrayList<String>();
//	file = "/Users/abdelrahmanamrsalem/Desktop/OS";
//}
public void writeToFile() throws IOException{
FileOutputStream newfile = new FileOutputStream(this.file);
ObjectOutputStream out = new ObjectOutputStream(newfile);
String output  = (String)this.page.get(0);
for(int i = 1;i<page.size();i++){
    output += "-"+page.get(i);
}
out.writeObject(output);
out.close();
newfile.close();
}


public void readFromFile() throws FileNotFoundException, IOException, ClassNotFoundException{
	ObjectInputStream in = new ObjectInputStream(new FileInputStream(this.file));
	String x = (String)in.readObject();
	String[] y = x.split("-"); 
	this.page.clear();
	for(int i = 0;i<y.length;i++)
	this.page.add(y[i]);
}


public ArrayList getData() throws IOException, FileNotFoundException, ClassNotFoundException{
	readFromFile();
	return this.page;
}

public String getPageName(){
	return this.file;
}

public boolean isFull(){
	return page.size()==maxRecords;
}

//public static void main(String[]args) throws ClassNotFoundException{
//	Page x  = new Page();
//	try {
//		//x.writeToFile();
//		//x.readFromFile();
//		//for(int i = 0; i<x.page.length;i++){
//			//System .out.println(x.page[i]);
//		//}
//	} catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
//}
}


