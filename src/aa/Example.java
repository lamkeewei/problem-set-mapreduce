package aa;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Some example code on how to use the MapReduce Framework
 */
public class Example implements Mapper, Reducer
{
    private static int fileSize = 5; //approximate lines in the data file

    /**
     * This shows how to read some data (currently one file as a command line argument)
     * Then launches the mapReduce framework
     * Prints the time taken to run
     * Outputs all the data returned from the Reducers
     * @param args a file
     */
    public static void main(String[] args)
    {
        List<String> data = null;
        try
        {
            data = readFile(args[0]);
        }
        catch (IOException e)
        {
            //I'm a good boy and do something with the exception!
            System.err.println("Can't read file.  See stack trace");
            e.printStackTrace();
            System.exit(0);
        }

        Example mapper = new Example(); //create mapper object
        Example reducer = new Example(); //create reducer object
        HashMap<Object, List> results = null;

        System.gc(); //good to do garbage collection before timing anything
        long s = System.currentTimeMillis(); //ready, set, GO!

        //Here we start the map reduce job on ~5 shards
        try { results = MapReduce.mapReduce(mapper, reducer, data, 5); }

        catch (InterruptedException e) //this shouldn't happen
        {
            System.out.println("Something unexpected happened");
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        long e = System.currentTimeMillis(); //And the winning time is....

        System.out.println("Clock time elapsed: " + (e - s) + " ms");

        //Prints all data found in results
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
     * One convenient way to read text data out of a file.
     */
    private static List<String> readFile(String fileName) throws IOException
    {
        List<String> data = new ArrayList<String>(fileSize);
        BufferedReader in = new BufferedReader(new FileReader(fileName));
        String line;
        while ((line = in.readLine())!= null)
        {
            data.add(line);
        }
        in.close();
        return data;
    }

    /**
     * A very simple mapper - returns the size of the data given to it
     * @param list the data shard for this mapper
     * @return HashMap: key = "Size", value = number of entries given to this mapper
     */
    @Override
    public HashMap map(List list)
    {
        HashMap <String, Integer> map = new HashMap<String, Integer>(1); //map to be returned

        map.put("Size", list.size()); //size of the data handed to the mapper

        return map;
    }

    /**
     * A very simple reducer which just counts the number of mappers which passed data into this key.
     * Since each mapper returns at most one data element for a key, and all of those are passed as a list, the
     * size of the list is the number of mappers which generated data.
     *
     * In reality you'd want to do something with the data given by the mappers
     *  (sum them, concatenate them, search through them, etc)
     *
     * @param key the key given to this reducer to work on; only one key is given, all values for that key are given
     * @param data a list of all values for this key
     * @return key = given key ("Size"), value = total size of the data set
     */
    @Override
    public HashMap reduce(Object key, List data)
    {
        HashMap<Object, Integer> result = new HashMap<Object, Integer>(1);
        result.put(key, data.size());
        return result;
    }
}
