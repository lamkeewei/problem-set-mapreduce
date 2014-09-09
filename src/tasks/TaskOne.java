package tasks;

import java.io.*;
import java.util.*;
import aa.Mapper;
import aa.Reducer;
import aa.MapReduce;
import util.Helper;

public class TaskOne implements Mapper, Reducer { 
  private final String FILTER = "101020";
  private final int START_TIME = 12;
  private final int END_TIME = 13;

  @Override
  public HashMap map(List list) {
    HashMap<String, String> results = new HashMap<>();

    for (Object r : list) {
      String record = (String) r;
      String[] tokens = record.split(",");
      int hour = Helper.grabHour(tokens[0]);
      
      boolean withinTime = hour >= START_TIME && hour <= END_TIME; 

      if (tokens[2].startsWith(FILTER) && withinTime) {
        results.put(tokens[2], tokens[1]);
      }
    }

    return results;
  }

  @Override
  public HashMap reduce(Object key, List data) {
      HashMap<Object, Integer> result = new HashMap<Object, Integer>(1);
      result.put(key, data.size());
      return result;
  }

  
  public static void main(String[] args) {
    List<String> data = null;

    try {
        data = Helper.readFile(args[0]);
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

      results = MapReduce.mapReduce(mapper, reducer, data, 5); 

    } catch (InterruptedException e) {

        System.out.println("Something unexpected happened");
        e.printStackTrace();  
    }

    long e = System.currentTimeMillis(); 

    System.out.println("Clock time elapsed: " + (e - s) + " ms");

    for (Object key : results.keySet())
    {
        System.out.println("Key found: " + key);
        List values = results.get(key);
        for (Object o : values)
        {
            System.out.println("value = " + o);
        }
    }
  }

  /**
   * Test Methods
   */

  public static void testMapper(String[] args) {
    List<String> records = new ArrayList<>();
    records.add("2014-02-01 12:00:38,34faeb58d58db27491c85ba8e683c0cc6764dc84,1010200001,-9900,3,3");
    records.add("2014-02-01 00:00:38,88dec4dc8140c033d97eed866ba932c5ac7accfa,1010500054,99,3,3");
    records.add("2014-02-01 00:00:38,b688dd6cdb5e554a2b3e8c116261f9a330f7ea3a,1010200047,99.9,3,3");

    TaskOne mapper = new TaskOne();
    HashMap<String, String> results = mapper.map(records);

    System.out.println("==== Mapper Results ====");
    for (String key : results.keySet()) {
      System.out.println(key + ":" + results.get(key));
    }
  }
}