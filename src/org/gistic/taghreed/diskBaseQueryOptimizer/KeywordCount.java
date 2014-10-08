/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.jetty.util.QuotedStringTokenizer;
import org.gistic.taghreed.collections.Tweet;

/**
 *
 * @author louai
 */
public class KeywordCount {

    private String file;
    HashMap<String, Integer> keywordList;
    final boolean ASC = true;
    final boolean DESC = false;

    public KeywordCount() {
        this.keywordList = new HashMap<String, Integer>();
    }

    
    public static void main(String[] args) {
//        args = new String[2];
//        args[0] = "/Users/louai/microblogsDataset/test/data/2014-01-06" ;
//        args[1] = "10" ;
        KeywordCount count = new KeywordCount();
        count.file = args[0];
        count.Tekonize();
        count.PrintHashMap(Integer.parseInt(args[1]));
    }

    /**
     * This method tekonize the keyword the output written into he keyword hash
     * map as Hashmap<Keyword,count>
     *
     * @throws UnsupportedEncodingException
     * @throws FileNotFoundException
     * @throws IOException
     * @throws ParseException
     */
    public void Tekonize() {
        try {
            BufferedReader reader;
            reader = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(file), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                Tweet objTweet = new Tweet(line);
                StringTokenizer tokenizer = new QuotedStringTokenizer(objTweet.tweetText, " ");
                while (tokenizer.hasMoreTokens()) {
                    String token = tokenizer.nextToken();
                    token = token.replace('*', ' ');
//                    token = token.replace('&', ' ');
//                    token = token.replace('!', ' ');
//                    token = token.replace('#', ' ');
//                    token = token.replace('%', ' ');
//                    token = token.replace('^', ' ');
//                    token = token.replace('@', ' ');
//                    token = token.replace('~', ' ');
                    
                    if (keywordList.containsKey(token)) {
                        //increament the keyword
                        try{
                        int oldValue = keywordList.get(token);
                        keywordList.replace(token, oldValue, (oldValue+1));
                        }catch (Exception e){
                            System.out.println("error in change value");
                        }
                    } else {
                        keywordList.put(token, 1);
                    }
                    System.out.println(token);
                }
            }
            Map<String, Integer> sorted = sortByComparator(keywordList, DESC);

            keywordList = (HashMap<String, Integer>) sorted;
        } catch (FileNotFoundException ex) {
            Logger.getLogger(KeywordCount.class.getName()).log(Level.SEVERE, null, ex);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(KeywordCount.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(KeywordCount.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(KeywordCount.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchElementException ex) {
            Logger.getLogger(KeywordCount.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchMethodError ex) {
            Logger.getLogger(KeywordCount.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(KeywordCount.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void PrintHashMap(int top) {
        for (Entry<String, Integer> entry : keywordList.entrySet()) {
            System.out.println("Key: " + entry.getKey() + " V= " + entry.getValue());
            top--;
            if(top == 0)
                return;
        }
    }

    private Map<String, Integer> sortByComparator(HashMap<String, Integer> unsortMap, final boolean order) {
        List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Integer>>() {
            public int compare(Entry<String, Integer> o1,
                    Entry<String, Integer> o2) {
                if (order) {
                    return o1.getValue().compareTo(o2.getValue());
                } else {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }

        });
        // Maintaining insertion order with the help of LinkedList
        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Entry<String, Integer> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;

    }

}
