package util;

import java.util.*;
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

  public static int grabMonth(String dateStr) {
    int pos = 0;
    return Integer.parseInt(dateStr.substring(pos + 5, pos + 7));
  }

  public static int grabDay(String dateStr) {
    int pos = 0;
    return Integer.parseInt(dateStr.substring(pos + 8, pos + 10));
  }


  public static int grabHour(String dateStr) {
    int pos = dateStr.indexOf(" ");
    return Integer.parseInt(dateStr.substring(pos + 1, pos + 3));
  }

  public static int grabMinute(String dateStr) {
    int pos = dateStr.indexOf(" ");
    return Integer.parseInt(dateStr.substring(pos + 4, pos + 6));
  }  

  public static int grabSecond(String dateStr) {
    int pos = dateStr.indexOf(" ");
    return Integer.parseInt(dateStr.substring(pos + 7, pos + 9));
  }

  public static HashMap mapLocation(List list) {
    HashMap results = new HashMap<>();
    for (Object r : list) {
      String record = (String) r;
      String[] tokens = record.split(",");
      String code = tokens[0];
      String location = tokens[1];
      if(results.get(code)==null) {
        results.put(code,location);
      }
    }
    return results;

  }
}