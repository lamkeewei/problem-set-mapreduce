/* 
  In which hour of the day is SIS Level 3 classroom 3-1 most popular?
*/

package tasks;

import java.io.*;
import java.util.*;
import aa.Mapper;
import aa.Reducer;
import aa.MapReduce;
import util.Helper;
import java.net.*;

public class TaskThree implements Mapper, Reducer { 
  private final String FILTER = "101020";
  private final int START_TIME = 12;
  private final int END_TIME = 13;
  private static Map<String,String> locationMapping;

  @Override
  public HashMap map(List list) {
    HashMap results = new HashMap();

    for (Object r : list) {
      String record = (String) r;
      String[] tokens = record.split(",");      
      String roomID = locationMapping.get(tokens[2]);

      if (roomID != null && roomID.equals("SMUSISL3SR3-1")) {
        int hour = Helper.grabHour(tokens[0]);
        Set<String> userIds = (Set<String>) results.get(hour);

        if (userIds == null) {
          userIds = new HashSet<String>();
          userIds.add(tokens[1]);

          results.put(hour, userIds);
        } else {
          userIds.add(tokens[1]);
        }
      }
    }

    return results;
  }

  @Override
  public HashMap reduce(Object key, List data) {
    HashMap map = new HashMap(1);
    Set<String> userIds = new HashSet<>();

    for (Object o : data) {
      userIds.addAll((Set<String>) o);
    }

    map.put(key, userIds.size());
    return map;
  }

  
  public static void main(String[] args) {
    List<String> data = new ArrayList<>();

    try {
      locationMapping = Helper.loadLocationMappings();
      System.out.println(locationMapping.keySet().size() + " mappings loaded");

      for (String fileName : args) {
        data.addAll(Helper.readFile(fileName));
      }

      System.out.println(data.size() + " records loaded");
    } catch (IOException e) {
      System.err.println("Can't read file.  See stack trace");
      e.printStackTrace();
      System.exit(0);
    }

    TaskThree mapper = new TaskThree(); 
    TaskThree reducer = new TaskThree(); 
    HashMap<Object, List> results = null;

    System.gc(); 
    long s = System.currentTimeMillis();

    try {       
      results = MapReduce.mapReduce(mapper, reducer, data, 5); 

    } catch (InterruptedException e) {

      System.out.println("Something unexpected happened");
      e.printStackTrace();
    }

    long e = System.currentTimeMillis(); 

    System.out.println("Clock time elapsed: " + (e - s) + " ms");

    for (Object key : results.keySet()) {
      List values = results.get(key);

      System.out.println(key + ":00:00 - " + values.get(0) + " people");
    }
  }
}