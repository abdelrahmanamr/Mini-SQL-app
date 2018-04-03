import java.io.FileNotFoundException;
import java.io.IOException;


public class test {

	public static void main (String[] args){
		DBapp x;
	
		try {
			x = new DBapp();
		x.createBRINIndex("Student","gpa" );
		System.out.println("The result records in the brin Index for the gpa");
		System.out.println();
			for(int i = 0;i<x.tables.get(0).tableBrinIndices.get(0).index.size();i++){
			//	x.tables.get(0).tableBrinIndices.get(0).index.get(i).readFromFile();
				for(int j = 0;j<x.tables.get(0).tableBrinIndices.get(0).index.get(i).page.size();j++){
					System.out.println(x.tables.get(0).tableBrinIndices.get(0).index.get(i).page.get(j));
				}
			}
			System.out.println();
			System.out.println("To Test the Select Please Run the testSelect.java");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
