package com.insecure.madredexamples.newapi.utils.parsers;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Karan Khosla
 */
public class XMLLineParser {

    public static void main(String[] args) {
        String XMLine = " <row Id=\"-1\" Reputation=\"1\" CreationDate=\"2012-02-14T18:31:38.453\" DisplayName=\"Community\" LastAccessDate=\"2012-02-14T18:31:38.453\" Location=\"on the server farm\" AboutMe=\"&lt;p&gt;Hi, I'm not really a person.&lt;/p&gt;&#xD;&#xA;&lt;p&gt;I'm a background process that helps keep this site clean!&lt;/p&gt;&#xD;&#xA;&lt;p&gt;I do things like&lt;/p&gt;&#xD;&#xA;&lt;ul&gt;&#xD;&#xA;&lt;li&gt;Randomly poke old unanswered questions every hour so they get some attention&lt;/li&gt;&#xD;&#xA;&lt;li&gt;Own community questions and answers so nobody gets unnecessary reputation from them&lt;/li&gt;&#xD;&#xA;&lt;li&gt;Own downvotes on spam/evil posts that get permanently deleted&lt;/li&gt;&#xD;&#xA;&lt;li&gt;Own suggested edits from anonymous users&lt;/li&gt;&#xD;&#xA;&lt;li&gt;&lt;a href=&quot;http://meta.stackoverflow.com/a/92006&quot;&gt;Remove abandoned questions&lt;/a&gt;&lt;/li&gt;&#xD;&#xA;&lt;/ul&gt;\" Views=\"0\" UpVotes=\"2162\" DownVotes=\"601\" Age=\"2\" AccountId=\"-1\" />\n";
        Map<String, String> map = convertToMap(XMLine);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            System.out.println("KEY:  " + entry.getKey() + ", VALUE: " + entry.getValue());
        }
    }

    public static Map<String, String> convertToMap(String XMLine) {
        Map<String, String> dataMap = new HashMap<>();
        try {
            String[] tokens = StringUtils.substring(XMLine.trim(), 0, XMLine.length() - 1).trim().split("\"");


            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];
                key = key.replaceAll("<", "");
                key = key.replaceAll(">", "");
                key = key.replaceAll("/", "");
                key = key.substring(0, key.length() - 1);
                key = key.trim();

                if (val.trim().equals(""))
                    continue;
                dataMap.put(key, val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            e.printStackTrace();
        }
        return dataMap;
    }
}
