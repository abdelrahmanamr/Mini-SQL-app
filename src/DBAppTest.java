import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Hashtable;


public class DBAppTest {
	public static void main(String[] args){
		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		DBapp x;
		try {
			x = new DBapp();
		try {
			x.createTable( strTableName, "id", htblColNameType );
		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
			e.printStackTrace();
} 			
	try {
				
			Hashtable htblColNameValue = new Hashtable( );
			// Insertions to table 
			htblColNameValue.put("id", new Integer( 2343432 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			x.insertIntoTable( strTableName , htblColNameValue );
			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 453455 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			x.insertIntoTable( strTableName , htblColNameValue );
			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 5674567 ));
			htblColNameValue.put("name", new String("Dalia Noor" ) );
			htblColNameValue.put("gpa", new Double( 1.25 ) );
			x.insertIntoTable( strTableName , htblColNameValue );
			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 743498 ));
			htblColNameValue.put("name", new String("John Noor" ) );
			htblColNameValue.put("gpa", new Double( 1.5 ) );
			x.insertIntoTable( strTableName , htblColNameValue );
			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 78452 ));
			htblColNameValue.put("name", new String("Zaky Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.88 ) );
			x.insertIntoTable( strTableName , htblColNameValue );
			
			ArrayList<Page> pages = x.tables.get(0).pages;
			
			 pages = x.tables.get(0).pages;

				for(int i = 0;i<pages.size();i++){
					for(int j = 0;j<pages.get(i).page.size();j++){
					System.out.println(pages.get(i).page.get(j));
				}}
//				 Deletion from table
			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 2343432 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			x.deleteFromTable( strTableName , htblColNameValue );
			System.out.println();
			System.out.println("After Deleting Ahmed Nour with id 2343432 Records");
			pages = x.tables.get(0).pages;

			for(int i = 0;i<pages.size();i++){
				for(int j = 0;j<pages.get(i).page.size();j++){
				System.out.println(pages.get(i).page.get(j));
			}}
			//-------------------------------------------------------------------------\\
			// update table 
			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 743498 ));
			htblColNameValue.put("name", new String("george Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.8 ) );
			x.updateTable( strTableName ,"id",htblColNameValue );
			System.out.println();
            System.out.println("After Updating John Nour to george Noor");
		 pages = x.tables.get(0).pages;

				for(int i = 0;i<pages.size();i++){
					for(int j = 0;j<pages.get(i).page.size();j++){
					System.out.println(pages.get(i).page.get(j));
				}}
				
				System.out.println();
				System.out.println("To try the Brin Index Please Run the test.java File");
	}
				
	catch(DBAppException db){
				System.out.println(db.getMessage());
				
			}
			catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
	}
			catch (NoSuchMethodException e) {
			//	 TODO Auto-generated catch block
				e.printStackTrace();
			}
				catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				}
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
	}
	catch (IllegalArgumentException e) {
////				// TODO Auto-generated catch block
				e.printStackTrace();
	}
		 catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
	catch (ClassNotFoundException e1) {
		// TODO Auto-generated catch block
	e1.printStackTrace();
	}
	
		}}


	