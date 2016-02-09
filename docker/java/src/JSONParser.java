/**
 * <h1>LyonTechHub</h1>
 * <h2>DataScience Appliquée</h2>
 * <h3>Parser JSON</h3>
 * Récupère le fichier <strong>JSON</strong>, le traite et l'introduit en base de donnée.
 * <pre>Créée le 16/01/2016</pre>
 * <pre>Modifié le 09/02/2016</pre>
 * 
 * <p>Utilisation de deux API :
 * 	<ul>
 * 		<li>JsonP 1.0.4</li>
 * 		<li>JDBC PostGreSQL 9.4</li>
 * 	</ul>
 *</p>
 *<p>Benchmark : 3600-3700 ms</p>
 * 
 * @author Mathieu Febvay -> mat.febvay@hotmail.fr
 * @version 1.2
 */

package fr.lyontechhub.datascienceappliquee.velov;

import java.io.IOException;
import java.io.InputStream;

import java.net.MalformedURLException;
import java.net.URL;

// Need JDBC PostgreSQL Lib
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

// Need JsonP Lib
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

public class JSONParser 
{
	/**
	 * 
	 * @param args[0] doit être l'adresse de la base de données avec le numéro de port ex: 0.0.0.0:5432 ou localhost:5432
	 * @param args[1] doit être le nom de la base de données
	 * @param args[2] doit être le nom de la table où insérer les données
	 * @param args[3] doit être le nom d'utilisateur pour se connecter à la base de données
	 * @param args[4] doit être le mot de passe pour se connecter à la base de données
	 */
	
	// Lancement de l'application
	public static void main(String[] args) 
	{
		long startingTime = 0;		
		long endingTime = 0;
		int i;
		
		Connection postGreSQLConnection = null;
		PreparedStatement preparedInsertStatement = null;
		String insertQuery = "INSERT INTO " +  args[2]
				+ "(number, name, address, address2, commune, nmarrond, bonus, pole, lat, lng, bike_stands,"
				+ "status, available_bike_stands, available_bikes, availabilitycode, availability, banking, the_geom, gid, last_update, last_update_fme, created_date) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,point(?,?),?,?,?,?);";
		DateTimeFormatter formatDate = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		
		try 
		{			
			System.out.print("Connection à la base de données...");
		
			postGreSQLConnection = DriverManager.getConnection(
					"jdbc:postgresql://" + args[0] + "/" + args[1],
					args[3],
					args[4]
					);
			// Amélioration des performances
			postGreSQLConnection.setAutoCommit(false);
			
			System.out.println("OK");
			
			System.out.print("Préparation de la requête...");
			
			// On prépare la requête d'insertion des données
			preparedInsertStatement = postGreSQLConnection.prepareStatement(insertQuery);
			
			System.out.println("OK");
		
			// Départ du benchmark
			startingTime = System.nanoTime();
			
			// On défini l'URL du fichier à récupérer
			URL dataUrl = new URL("https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json");
			
			// On récupère ce fichier en ouvrant un flux/
			InputStream is = dataUrl.openStream();
			
			// On va lire ce flux avec le parser JSON
			JsonReader jsRdr = Json.createReader(is);
			
			// On crée un objet JSON
			JsonObject jsObj = jsRdr.readObject();
			
			// On va récupérer la clé qui nous intéresse qui est un tableau de clé/valeur
			JsonArray jsArray = jsObj.getJsonArray("values");
			
			// On initialise le nombre d'insertions à 0
			i = 0;
			
			// Pour chaque tableau on crée un objet JSON 
			for (JsonObject result : jsArray.getValuesAs(JsonObject.class))
			{
				LocalDateTime now = LocalDateTime.now();
				String dateTime = now.format(formatDate);
				
				preparedInsertStatement.setInt(1, Integer.parseInt(result.getString("number")));
				preparedInsertStatement.setString(2, (result.getString("name")));
				preparedInsertStatement.setString(3, (result.getString("address")));
				preparedInsertStatement.setString(4, (result.getString("address2")));
				preparedInsertStatement.setString(5, (result.getString("commune")));
				if (result.getString("nmarrond").contains("None"))
				{
					preparedInsertStatement.setInt(6, -1);
				}
				else
				{
					preparedInsertStatement.setInt(6, Integer.parseInt(result.getString("nmarrond")));
				}
				preparedInsertStatement.setString(7, (result.getString("bonus")));
				preparedInsertStatement.setString(8, (result.getString("pole")));
				preparedInsertStatement.setFloat(9, Float.parseFloat((result.getString("lat"))));
				preparedInsertStatement.setFloat(10, Float.parseFloat((result.getString("lng"))));
				preparedInsertStatement.setInt(11, Integer.parseInt(result.getString("bike_stands")));
				preparedInsertStatement.setString(12, (result.getString("status")));
				preparedInsertStatement.setInt(13, Integer.parseInt(result.getString("available_bike_stands")));
				preparedInsertStatement.setInt(14, Integer.parseInt(result.getString("available_bikes")));
				preparedInsertStatement.setInt(15, Integer.parseInt(result.getString("availabilitycode")));
				preparedInsertStatement.setString(16, (result.getString("availability")));
				preparedInsertStatement.setBoolean(17, Boolean.parseBoolean(result.getString("banking")));
				preparedInsertStatement.setFloat(18, Float.parseFloat((result.getString("lat"))));
				preparedInsertStatement.setFloat(19, Float.parseFloat((result.getString("lng"))));
				preparedInsertStatement.setInt(20, Integer.parseInt(result.getString("gid")));
				preparedInsertStatement.setTimestamp(21, Timestamp.valueOf(result.getString("last_update")));
				preparedInsertStatement.setTimestamp(22, Timestamp.valueOf(result.getString("last_update_fme")));
				preparedInsertStatement.setTimestamp(23, Timestamp.valueOf(dateTime));
				
				preparedInsertStatement.addBatch();
				i++;
				
			}// for
		
			preparedInsertStatement.executeBatch();
			postGreSQLConnection.commit();
			
			// On ferme le flux
			is.close();
			
			System.out.println("Insertion de " + i + " lignes dans la base de données");
			
			// Fin du benchmark et calcul du temps d'execution
			endingTime = System.nanoTime();
			System.out.println("Temps d'execution: " + (endingTime-startingTime)/1000000 + " ms");
			
			// On attend 1 minute
			Thread.sleep(60*1000);				 
		}// try
		
		// Exception pour l'URL
		catch (MalformedURLException e) 
		{
			System.out.println("Erreur dans l'URL : " + e.getMessage());
		} 
		// Exception pour le flux
		catch (IOException e) 
		{
			System.out.println("Erreur lors de l'ouveture du flux entrant : " + e.getMessage());
		}
		// Exception pour la base de données
		catch (SQLException e) 
		{
			System.out.println("Erreur lors de la connection à la base de données : " + e.getMessage());
		} 
		// Erreur dans le lancement du thread de pause
		catch (InterruptedException e) 
		{
			System.out.println("Erreur lors du lancement du Thread : " + e.getMessage());
		}
			
		try 
		{
			// On ferme la requête
			preparedInsertStatement.close();
			// On ferme la connection
			postGreSQLConnection.close();
		} 
		catch (SQLException e) 
		{
			System.out.println("Erreur lors de la fermeture des connections : " + e.getMessage());
		}
		
	}// void main
}// class
