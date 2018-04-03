import java.io.IOException;
import java.util.Iterator;


public class testSelect {
public static void main(String[] args){
	try {
		DBapp x = new DBapp();
		String strTableName = "Student";
		Object[]objarrValues = new Object[2]; 
		objarrValues[0] = new Double(0.75); 
		objarrValues[1] = new Double(1); 
		String[] strarrOperators = new String[2]; 
		strarrOperators[0] = "<="; 
		strarrOperators[1] = "<";
		Iterator resultSet = x.selectFromTable(strTableName, "gpa",
				 objarrValues, strarrOperators );
		System.out.println("The result of Selecting 0.75<=gpa<1");
		System.out.println();
		while(resultSet.hasNext()){
			System.out.println(resultSet.next());
		}
		System.out.println();
		System.out.println("Thank You :)");
		
	} catch (ClassNotFoundException | IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}
}
