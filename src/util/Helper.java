package util;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.File;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Helper {
  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static List<String> readFile(String fileName) 
    throws IOException {
      System.out.println("Reading " + fileName + "...");
      List<String> data = new ArrayList<String>();

      // Get file from classpath
      InputStream is = Helper.class.getResourceAsStream("/data/" + fileName);
      Reader reader = new InputStreamReader(is); 

      BufferedReader in = new BufferedReader(reader);
      String line;
      
      while ((line = in.readLine())!= null) {
        data.add(line); 
      }
      in.close();
      return data;
  }

  public static int grabHour(String dateStr) {
    int pos = dateStr.indexOf(" ");
    return Integer.parseInt(dateStr.substring(pos + 1, pos + 3));
  }  
}