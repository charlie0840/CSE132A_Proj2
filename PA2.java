/**
 * This Java program exemplifies the basic usage of JDBC.
 * Requirements:
 * (1) JDK 1.6+.
 * (2) SQLite3.
 * (3) SQLite3 JDBC jar (https://bitbucket.org/xerial/sqlitejdbc/downloads/sqlite-jdbc-3.8.7.jar).
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
public class PA2 {
 	public static void main(String[] args) {
 		Connection conn = null; // Database connection.
 		try {
 			// Load the JDBC class.
 			Class.forName("org.sqlite.JDBC");
			// Get the connection to the database.
			// - "jdbc" : JDBC connection name prefix.
 			// - "sqlite" : The concrete database implementation
 			// (e.g., sqlserver, postgresql).
 			// - "pa2.db" : The name of the database. In this project,
 			// we use a local database named "pa2.db". This can also
 			// be a remote database name.
 			conn = DriverManager.getConnection("jdbc:sqlite:pa2.db");
 			System.out.println("Opened database successfully.");
 			// Use case #1: Create and populate a table.
 			// Get a Statement object.
 			Statement stmt = conn.createStatement();


/*
 			stmt.executeUpdate("DROP TABLE IF EXISTS Student;");
 			// Student table is being created just as an example. You
 			// do not need Student table in PA2
			stmt.executeUpdate("CREATE TABLE Student(FirstName, LastName);");
 			stmt.executeUpdate(
			"INSERT INTO Student VALUES('F1','L1'),('F2','L2');");
 			// Use case #2: Query the Student table with Statement.
 			// Returned query results are stored in a ResultSet
 			// object.
 			ResultSet rset = stmt.executeQuery("SELECT * from Student;");
 			// Print the FirstName and LastName columns.
 			System.out.println ("\nStatement result:");
 			// This shows how to traverse the ResultSet object.
 			while (rset.next()) {
 				// Get the attribute value.
 				System.out.print(rset.getString("FirstName"));
				System.out.print("---");
 				System.out.println(rset.getString("LastName"));
 			}
 			// Use case #3: Query the Student table with
 			// PreparedStatement (having wildcards).
 			PreparedStatement pstmt = conn.prepareStatement(
 				"SELECT * FROM Student WHERE FirstName = ?;");
			// Assign actual value to the wildcard.
 			pstmt.setString (1, "F1");
 			rset = pstmt.executeQuery ();
 			System.out.println ("\nPrepared statement result:");
 			while (rset.next()) {
 				System.out.print(rset.getString("FirstName"));
 				System.out.print("---");
 				System.out.println(rset.getString("LastName"));
 			}*/
			
			int count = 1;

			stmt.executeUpdate("CREATE TABLE Connected(airline char(32), origin char(32), destination char(32), stops Integer);");

			stmt.executeUpdate("INSERT INTO Connected SELECT *,0 FROM FLIGHT;");
	
			stmt.executeUpdate("CREATE TABLE tempInter(airline char(32), origin char(32), destination char(32), stops Integer);");

			stmt.executeUpdate("CREATE TABLE deltaInter(airline char(32), origin char(32), destination char(32), stops Integer);");

			stmt.executeUpdate("CREATE TABLE preInter(airline char(32), origin char(32), destination char(32), stops Integer);");

			ResultSet retSet;

			while(!(count<= 0)){
				stmt.executeUpdate("DELETE FROM preInter;");
				stmt.executeUpdate("INSERT INTO preInter SELECT * FROM Connected");
				stmt.executeUpdate("DELETE FROM Connected;");
				stmt.executeUpdate("DELETE FROM tempInter;");
				stmt.executeUpdate("DELETE FROM deltaInter;");			


				stmt.executeUpdate("INSERT INTO tempInter SELECT y.airline, x.origin, y.destination, y.stops + 1 FROM Flight x," 
					+ " preInter y WHERE y.origin = x.destination AND x.airline = y.airline AND x.origin <> y.destination;");

				stmt.executeUpdate("DELETE FROM tempInter WHERE EXISTS" + 
					"(SELECT * FROM preInter p WHERE p.origin = tempInter.origin AND p.airline = tempInter.airline AND "+
					"p.destination = tempInter.destination);");
				
							

				stmt.executeUpdate("INSERT INTO Connected SELECT * FROM preInter UNION SELECT * FROM tempInter;");

				stmt.executeUpdate("INSERT INTO deltaInter SELECT * FROM Connected EXCEPT SELECT * FROM preInter;");

				retSet = stmt.executeQuery("SELECT count(*) as c FROM deltaInter;");			
				
				retSet.next();
			
				count = retSet.getInt("c");				
				System.out.println(count);
				if(count == 0)
					retSet.close();
			}
 			// Close the ResultSet and Statement objects.
 			//retSet.close();
 			stmt.close();
 		} catch (Exception e) {
 			throw new RuntimeException("There was a runtime problem!", e);
 		} finally {
			try {
 				if (conn != null) conn.close();
 			} catch (SQLException e) {
 				throw new RuntimeException(
 				"Cannot close the connection!", e);
 			}
 		}
 	}
}
