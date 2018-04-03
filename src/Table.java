import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;

public class Table implements Serializable{
	public String TableName;
	public String Key;
	public ArrayList<Page> pages;
	public ArrayList<String> ColName;
	public ArrayList<String> ColType;
	public ArrayList<BrinIndex> tableBrinIndices;
	public ArrayList<DenseIndex> tableDenseIndices;
	public Hashtable<String,Boolean> availIndicies;
	int counter = 1;

	public Table(String TableName, String Key,
			Hashtable<String, String> htblColNameType)
			throws FileNotFoundException {
		this.TableName = TableName;
		this.Key = Key;
		ColName = new ArrayList<String>();
		ColType = new ArrayList<String>();
		tableBrinIndices=new ArrayList<BrinIndex>();
		tableDenseIndices = new ArrayList<DenseIndex>();
		availIndicies = new Hashtable<>();
		CreateTable(htblColNameType);
		writeToCsv();
	}

	private void CreateTable(Hashtable<String, String> htblColNameType) {
		Enumeration e = htblColNameType.keys();
		ArrayList<String> temp = new ArrayList<String>();
		while (e.hasMoreElements()) {
			String Column = (String) e.nextElement();
			temp.add(Column);
		}
		ColName.add(Key);
		for (int i = temp.size() - 1; i >= 0; i--) {
			if (temp.get(i) != Key) {
				ColName.add(temp.get(i));
			}
			availIndicies.put(temp.get(i), false);
		}
		for (int i = 0; i < ColName.size(); i++) {
			ColType.add(htblColNameType.get(ColName.get(i)));
		}
	}

	public void writeToCsv() throws FileNotFoundException {
		PrintWriter pw = new PrintWriter(new File("data/"+this.TableName+"metadata.csv"));
		StringBuilder sb = new StringBuilder();
		sb.append("Table Name");
		sb.append(',');
		sb.append("Column Name");
		sb.append(',');
		sb.append("Column Type");
		sb.append(',');
		sb.append("Key");
		sb.append(',');
		sb.append("Indexed");
		sb.append('\n');
		for (int i = 0; i < this.ColName.size(); i++) {
			sb.append(TableName);
			sb.append(',');
			sb.append(ColName.get(i));// name of the column
			sb.append(',');
			sb.append(ColType.get(i));// type of column
			sb.append(',');
			if (ColName.get(i).equals(Key)) {
				sb.append("True");
				sb.append(',');
			} else {
				sb.append("False");
				sb.append(',');
			}
			if(availIndicies.get(ColName.get(i))){
				sb.append(true);
			}
			else{
			sb.append("False");
			}
			sb.append('\n');
		}
		pw.write(sb.toString());
		pw.close();

	}

	public void insert(Object primaryKey, String record)
			throws FileNotFoundException, ClassNotFoundException, IOException {
		ArrayList<Page> newPages = new ArrayList<Page>();
		String temprecord = record;
		Object tempPK = primaryKey;
		for (int i = 0; i < this.pages.size(); i++) {
			this.pages.get(i).readFromFile();
			Page currentPage = this.pages.get(i);
			Page newPage = new Page(currentPage.getPageName());
			newPages.add(newPage);
			for (int j = 0; j < currentPage.page.size(); j++) {
				String clusterkey = currentPage.page.get(j).split(",")[0];
				String clusterKeyType = this.ColType.get(0);
				Object x = null;
				// System.out.println(clusterKeyType.substring(10));
				if (clusterKeyType.substring(10).equals("Integer")) {
					x = new Integer(clusterkey);
				} else if (clusterKeyType.substring(10).equals("String")) {
					x = new String(clusterkey);
				} else if (clusterKeyType.substring(10).equals("Boolean")) {
					x = new Boolean(clusterkey);
				} else if (clusterKeyType.substring(10).equals("Double")) {
					x = new Double(clusterkey);
				}

				if (compare(tempPK, x, x.getClass().getSimpleName()) <= 0) {
					newPages.get(i).page.add(temprecord);
					temprecord = this.pages.get(i).page.get(j);
					tempPK = x;
				} else {
					newPages.get(i).page.add(this.pages.get(i).page.get(j));
				}
			}
			newPages.get(i).writeToFile();
		}
		if (newPages.get(newPages.size() - 1).isFull()) {
			Page x = new Page(this.TableName + this.counter);
			counter++;
			x.page.add(temprecord);
			x.writeToFile();
			newPages.add(x);
		} else {
			newPages.get(newPages.size() - 1).page.add(temprecord);
			newPages.get(newPages.size() - 1).writeToFile();
		}
		this.pages = newPages;
	}

	public static int compare(Object x,Object y,String type){
		char c = type.charAt(0);
		switch(c){
			case'I':{Integer a= new Integer((int) x);
			Integer b= new Integer((int) y);
			if(a<=b){
				
				return -1;
			}else{
				return 1;
			}
			 }
			case'D':{Double a= new Double((double) x);
			Double b= new Double((double) y);
			if(a<=b){
				
				return -1;
			}else{
				return 1;
			}
			 }
			case'S':{String a= new String((String)x);
			String b= new String((String) y);
			
				return a.compareTo(b);
			
				
			}
			default : return 0;
			 
		}
	}
	public void deleteRecord(Object clusterKeyToDelete) throws FileNotFoundException, ClassNotFoundException, IOException{
		 String clusterKeyType = this.ColType.get(0).substring(10);
		 ArrayList<Page> tableAfterDeletion = new ArrayList<Page>();
		 int k = 0;
		 int pagecounter = -1;
		    for(int i = 0;i<this.pages.size();i++){
		    	Page page = this.pages.get(i);
		    	page.readFromFile();
		    	for(int j=0;j<page.page.size();j++){
		    		
		    		String primaryKey = page.page.get(j).split(",")[0];
		    		if(!((primaryKey.toString()).equals(clusterKeyToDelete.toString()))){
		    			if(k==0){
			    			Page newPage = new Page(page.getPageName());
			    			newPage.page.add(page.page.get(j));
			    			tableAfterDeletion.add(newPage);
			    			pagecounter++;
	                        k++;
			    		}else{
			    			tableAfterDeletion.get(pagecounter).page.add(page.page.get(j));
			    			k++;
			    			if(k==3){
			    				tableAfterDeletion.get(pagecounter).writeToFile();
			    				k=0;
			    			}
			    		}
		    	
		    		}
		    		
		    	}
		    }
		    this.pages=tableAfterDeletion;
	}
	
	public void updateTableHelper(String updatedRecord, String strKey)
			throws DBAppException, IOException{
		

		for(int i=0;i<pages.size();i++){

			for(int j=0;j<pages.get(i).page.size();j++){
				
				String newRecord = updatedRecord.split(",")[0];
				String oldRecord = pages.get(i).page.get(j).split(",")[0];
				if(newRecord.equals(oldRecord)){
					pages.get(i).page.set(j, updatedRecord);
				}
				
			}
			pages.get(i).writeToFile();
			
		}
		
	}
	public void CreateDense(String colName) throws IOException, ClassNotFoundException{
		String DenseName = TableName + colName + "DenseIndex";
		String colType = ColType.get(ColName.indexOf(colName)).substring(10);
        DenseIndex x = new DenseIndex(colName,colType,DenseName);
        ArrayList<String> fullTuple = new ArrayList<String>();
        ArrayList<String> sortedTuple = new ArrayList<String>();
        ArrayList<String> finalTuple = new ArrayList<String>();
        if(colName.equals(this.Key)){
        	for(int i =0 ; i<pages.size();i++){
        		x.createNewPage();
        	    for(int j=0;j<pages.get(i).page.size();j++){
        			String clusterkey = pages.get(i).page.get(j).split(",")[0];
        			String data = clusterkey +"," + i+","+j;
                    x.values.get(i).page.add(data);
        		}
        	    x.values.get(i).writeToFile();
        	}
        }
        else{
        	int colIndex = ColName.indexOf(colName);
        	for(int i =0;i<pages.size();i++){
        		for(int j = 0;j<pages.get(i).page.size();j++){
        			String index = pages.get(i).page.get(j).split(",")[colIndex];
        			fullTuple.add(index+","+i+","+j);
        			sortedTuple.add(index);
        		}
        	}
        	Collections.sort(sortedTuple);
        	for(int i = 0;i<sortedTuple.size();i++){
        		for(int j=0;j<fullTuple.size();j++){
        		if(compareForIndex(fullTuple.get(j).split(",")[0],sortedTuple.get(i),colType)==0){
        			finalTuple.add(fullTuple.get(j));
      			    fullTuple.remove(j);
        			break;
        		}}
        	}
        	        int counter =0;		
        	   while(counter<finalTuple.size()){
        	    	x.createNewPage();
        	    	while(!(x.values.get(x.counter-1).isFull()) &&counter<finalTuple.size()){
        	    		x.values.get(x.counter-1).page.add(finalTuple.get(counter));
            		    counter++;

        	    	}
        	    	x.values.get(x.counter-1).writeToFile();

        		}
        	}
        if(tableDenseIndices.isEmpty()){
        	tableDenseIndices.add(x);
        }
        else{
        for(int i =0;i<tableDenseIndices.size();i++){
        	if(tableDenseIndices.get(i).indexName.equals(DenseName)){
        		tableDenseIndices.remove(i);
        		break;
        	}
        }
        tableDenseIndices.add(x);
        }
        this.CreatBrinIndex(x);
        
	}
	public void CreatBrinIndex(DenseIndex denseIndex) throws FileNotFoundException, ClassNotFoundException, IOException{
		BrinIndex brinIndex = new BrinIndex(denseIndex.colType,denseIndex.colName);
		Page page = null;
		boolean lastTime = false;
		boolean firstTime = true;
		for(int i = 0;i<denseIndex.values.size();i++){			
			String record = ((String)denseIndex.values.get(i).getData().get(0)).split(",")[0]+","+((String)denseIndex.values.get(i).getData().get(denseIndex.values.get(i).getData().size()-1)).split(",")[0] +","+i;
			if(firstTime){
				page = new Page(this.TableName+denseIndex.colName+"BrinIndex"+brinIndex.counter);
				page.page.add(record);
				brinIndex.counter++;
				lastTime = false;
				firstTime = false;
			}
			else
			{
			    if(i%3==0)
			    {
			    page.writeToFile();
			    brinIndex.index.add(page);
				page = new Page(this.TableName+denseIndex.colName+"BrinIndex"+brinIndex.counter);
				brinIndex.counter++;
				page.page.add(record);
				lastTime= true;
			    }
			    else
			    {
				page.page.add(record);
				lastTime = false;
			    }
			}
			//record = denseIndex.values.get(i).
		}
		if(!lastTime && denseIndex.values.size()>0){
			page.writeToFile();
		    brinIndex.index.add(page);
		}
		for(int i = 0;i<this.tableBrinIndices.size();i++){
			if(this.tableBrinIndices.get(i).colName.equals(brinIndex.colName)){
				this.tableBrinIndices.remove(i);
				break;
			}
		}
		this.tableBrinIndices.add(brinIndex);
	}
	public int compareForIndex(String x,String y,String type){
		char c = type.charAt(0);
		switch(c){
			case'I':{int a= Integer.parseInt(x);
			int b= Integer.parseInt(y);
			if(a==b){
				
				return 0;
			}else{
				return -1;
			}
			 }
			case'D':{double a= Double.parseDouble(x);
		      double b= Double.parseDouble(y);
			if(a==b){
				
				return 0;
			}else{
				return -1;
			}
			 }
			case'S':{
			
				return x.compareTo(y);
			
				
			}
			default : return 1;
			 
		}
	}
	public void updateBrinIndex() throws ClassNotFoundException, IOException{
		for(int i = 0;i<this.ColName.size();i++){
			if(this.availIndicies.get(this.ColName.get(i))){
				this.CreateDense(this.ColName.get(i));
			}
		}
	}

	public ArrayList<String> selectValues(String colName , Object[] objects , String[] operators){
		ArrayList<String> result = new ArrayList<String>();
		ArrayList<Integer> DensePages = new ArrayList<Integer>();  
		int placeOfCol = this.ColName.indexOf(colName);
		String colType = this.ColType.get(placeOfCol);
		if(availIndicies.get(colName) == false){
			for(int i =0;i<pages.size();i++){
				for(int j=0;j<pages.get(i).page.size();j++){
					String record = pages.get(i).page.get(j).split(",")[placeOfCol];
					if(compareForLeftPart(objects[0],record,operators[0],colType)==1 && compareForRightPart(objects[1],record,operators[1], colType)==1){
						result.add(pages.get(i).page.get(j));
					}
				}
			}
		}
		else{
			BrinIndex x =null;
			DenseIndex y = null;
			for(int i=0;i<this.tableBrinIndices.size();i++){
				if(this.tableBrinIndices.get(i).colName.equals(colName)){
					x = this.tableBrinIndices.get(i);
				}
			}
			for(int i=0;i<this.tableDenseIndices.size();i++){
				if(this.tableDenseIndices.get(i).colName.equals(colName)){
					y = this.tableDenseIndices.get(i);
				}
			}
			for(int i=0;i<x.index.size();i++){
				for(int j=0;j<x.index.get(i).page.size();j++){
					String min = x.index.get(i).page.get(j).split(",")[0];
					String max = x.index.get(i).page.get(j).split(",")[1];
					if((compareForLeftPart(objects[0], min, operators[0], colType)==1||compareForLeftPart(objects[0], max, operators[0], colType)==1)&&(compareForLeftPart(objects[1], min, operators[1], colType)==1||compareForRightPart(objects[1], max, operators[1], colType)==1)){
						DensePages.add(Integer.parseInt(x.index.get(i).page.get(j).split(",")[2]));
					}
					
				}
			}
			for(int i =0;i<DensePages.size();i++){
				for(int j = 0;j<y.values.get(DensePages.get(i)).page.size();j++){
					String record = y.values.get(DensePages.get(i)).page.get(j).split(",")[0];
					if(compareForLeftPart(objects[0],record,operators[0],colType)==1 && compareForRightPart(objects[1],record,operators[1], colType)==1){
						int pageNumber = Integer.parseInt(y.values.get(DensePages.get(i)).page.get(j).split(",")[1]);
						int recordNumber = Integer.parseInt(y.values.get(DensePages.get(i)).page.get(j).split(",")[2]);
						result.add(this.pages.get(pageNumber).page.get(recordNumber));
					}
				}
			}
			
		}
		
		return result;
	}
	
	public int compareForLeftPart(Object minValue,String tableValue,String operator,String colType){
		char x = colType.charAt(10);
		switch(x){
		case 'I':{Integer a = new Integer((int) minValue);
		Integer b = Integer.parseInt(tableValue);
			if(operator.equals("<")){
				if(b>a){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">")){
				if(a>b){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals("<=")){
				if(b>=a){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">=")){
				if(a>=b){
					return 1;
				}
				else{
					return -1;
				}
			}
			return 0;
		}
		//------------------------------------------------------------------------------------------------
		case 'D':{Double a = new Double((double) minValue);
		Double b = Double.parseDouble(tableValue);
			if(operator.equals("<")){
				if(b>a){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">")){
				if(a>b){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals("<=")){
				if(b>=a){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">=")){
				if(a>=b){
					return 1;
				}
				else{
					return -1;
				}
			}
			return 0;
		}
		//--------------------------------------------------------------------------------------------------
		case 'S':{String a = new String((String) minValue);
			if(operator.equals("<")){
				if(tableValue.toLowerCase().compareTo(a.toLowerCase()) >0){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">")){
				if(a.toLowerCase().compareTo(tableValue.toLowerCase())>0){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals("<=")){
				if(tableValue.toLowerCase().compareTo(a.toLowerCase())>0|| tableValue.toLowerCase().equals(a.toLowerCase())){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">=")){
				if(a.toLowerCase().compareTo(tableValue.toLowerCase())>0|| a.toLowerCase().equals(tableValue.toLowerCase())){
					return 1;
				}
				else{
					return -1;
				}
			}
			return 0;
		}
		default: return 0;
		}
		
		}
	
	public int compareForRightPart(Object maxValue,String tableValue,String operator,String colType){
		char x = colType.charAt(10);
		switch(x){
		case 'I':{Integer a = new Integer((int) maxValue);
		Integer b = Integer.parseInt(tableValue);
			if(operator.equals("<")){
				if(b<a){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">")){
				if(a<b){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals("<=")){
				if(b<=a){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">=")){
				if(a<=b){
					return 1;
				}
				else{
					return -1;
				}
			}
			return 0;
		}
		//------------------------------------------------------------------------------------------------
		case 'D':{Double a = new Double((double) maxValue);
		Double b = Double.parseDouble(tableValue);
			if(operator.equals("<")){
				if(b<a){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">")){
				if(a<b){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals("<=")){
				if(b<=a){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">=")){
				if(a<=b){
					return 1;
				}
				else{
					return -1;
				}
			}
			return 0;
		}
		//--------------------------------------------------------------------------------------------------
		case 'S':{String a = new String((String) maxValue);
			if(operator.equals("<")){
				if(tableValue.compareTo(a) <0){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">")){
				if(a.compareTo(tableValue)<0){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals("<=")){
				if(tableValue.compareTo(a)<0 || tableValue.equals(a)){
					return 1;
				}
				else{
					return -1;
				}
			}
			else if(operator.equals(">=")){
				if(a.compareTo(tableValue)<0 || a.equals(tableValue)){
					return 1;
				}
				else{
					return -1;
				}
			}
			return 0;
		}
		default: return 0;
		}
		
		}
	
//	public String searchUsingBrinIndex(BrinIndex brinIndex,String insertedKey,String colType){
//		String res = "";
//		int pageInDense;
//		DenseIndex denseIndex;
//		for(int i = 0;i<this.tableDenseIndices.size();i++)
//		{
//			if(this.tableDenseIndices.get(i).colName.equals(brinIndex.colName)){
//				denseIndex = this.tableDenseIndices.get(i);
//				break;
//			}
//		}
//		for(int i = 0;i<brinIndex.index.size();i++){
//			for(int j =0;j<brinIndex.index.get(i).page.size();j++){
//				
//				if((compareForLeftPart(brinIndex.index.get(i).page.get(j).split(",")[0], insertedKey, "<=",colType)==1) && (compareForRightPart(brinIndex.index.get(i).page.get(j).split(",")[1],insertedKey,"<=", colType)==1))
//			    {
//					pageInDense = Integer.parseInt(brinIndex.index.get(i).page.get(j).split(",")[2]);
//					break;
//				}
//			}
//		}
//		for(int i =0;i<)
//		return res;
//	}
}
