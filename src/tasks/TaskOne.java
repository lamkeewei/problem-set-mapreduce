package tasks;

import java.io.*;
import java.util.*;
import aa.Mapper;
import aa.Reducer;
import aa.MapReduce;
import util.Helper;
import java.net.*;

public class TaskOne implements Mapper, Reducer { 
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

      if (tokens[2].startsWith(FILTER) && roomID.indexOf("SR2-") > -1) {
        int hour = Helper.grabHour(tokens[0]);
        boolean withinTime = hour == START_TIME || tokens[2].indexOf("13:00:00") > -1;

        if (withinTime) {          
          if(results.get(roomID) == null) {
            List newList = new LinkedList();
            newList.add(tokens[1]);

            results.put((String) roomID, newList);
          } else {
            List prev = (LinkedList) results.get(roomID);
            prev.add(tokens[1]);
          }
        }
      }
    }
    // System.out.println("Results size: " + results.size());
    return results;
  }

  @Override
  public HashMap reduce(Object key, List data) {
    HashMap map = new HashMap(1);
    Set set = new HashSet();

    for(Object o: data){
      set.addAll((List) o);    
    }  

    map.put(key, set.size());
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

    TaskOne mapper = new TaskOne(); 
    TaskOne reducer = new TaskOne(); 
    HashMap<Object, List> results = null;

    System.gc(); 
    long s = System.currentTimeMillis();

    try {       
      results = MapReduce.mapReduce(mapper, reducer, data, 1); 

    } catch (InterruptedException e) {

      System.out.println("Something unexpected happened");
      e.printStackTrace();
    }

    long e = System.currentTimeMillis(); 

    System.out.println("Clock time elapsed: " + (e - s) + " ms");

    int max = 0;
    String location = "";

    for (Object key : results.keySet()) {
      // list size is 1, caused reducer also performed combine step
      int size = (int)results.get(key).get(0);
      if (size > max) {
        max = size;
        location = (String) key;
      }
    }

    System.out.println("Count: " + max);
    System.out.println("Location: " + location);
  }
}