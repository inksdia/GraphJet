package com.graph;

import com.google.gson.*;
import com.graph.beans.HashTag;
import com.graph.beans.Message;
import com.graph.beans.ProfileUser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by saurav on 29/03/17.
 */

/**
 * GraphHelper class for testing etc
 */
public class GraphHelper {

    public static List<Message> getMessages(String identifier, int i) throws IOException {

        try {
            JsonArray array = getJsonArray(identifier, i);

            System.out.println("Total message in file: " + array.size());

            List<Message> messages = new ArrayList<>();

            for (JsonElement element : array) {
                JsonObject object = element.getAsJsonObject().get("_source").getAsJsonObject();
                Message msg = convertToMessage(object);
                if (msg != null) {
                    messages.add(msg);
                }
            }
            System.out.println("Total Message received: " + messages.size());

            //saveToFile(messages);

            return messages;

        } catch (Throwable ex) {
            ex.printStackTrace();
        }

        return Collections.emptyList();
    }

    private static void saveToFile(List<Message> messages) throws IOException {
        Gson gson = new Gson();
        FileWriter fileWriter = new FileWriter("/Users/saurav/src/GraphJet2/nextTime.txt");
        fileWriter.write(gson.toJson(messages));
        fileWriter.close();
    }

    private static Message convertToMessage(JsonObject sourceMap) {
        try {
            Message message = new Message();
            message.setId(Long.valueOf(sourceMap.get("snMId").getAsString().split("_")[0])); //Instagram, not needed
            JsonElement m = sourceMap.get("add").getAsJsonObject().get("m_std");
            message.setMessage(m.getAsString());
            ProfileUser profile = new ProfileUser();
            JsonObject user = sourceMap.get("fU").getAsJsonObject();
            profile.setId(Long.valueOf(user.get("uI").getAsString()));
            profile.setName(user.get("sN").getAsString());
            message.setUser(profile);
            JsonArray hTs = sourceMap.get("hT").getAsJsonArray();
            HashTag[] hashTags = new HashTag[hTs.size()];
            for (int i = 0; i < hTs.size(); i++) {
                hashTags[i] = new HashTag(null, hTs.get(i).getAsString());
            }
            message.setHashTags(hashTags);

            if (sourceMap.get("mTp") != null && sourceMap.get("mTp").getAsInt() == 8) {
                JsonElement qsId = sourceMap.get("add").getAsJsonObject().get("qsId");
                if (qsId != null) {
                    message.setRetweetId(Long.valueOf(qsId.getAsString()));
                    message.setRetweet(true);
                }
            } else {
                message.setRetweet(false);
            }
            return message;
        } catch (Exception e) {
            System.out.println(sourceMap);
            e.printStackTrace();
        }
        return null;
    }

    public static JsonArray getJsonArray(String identifier, int i) throws FileNotFoundException {
        return type2(identifier, i);
        //return type1();
    }

    public static JsonArray type2(String folderName, int i) throws FileNotFoundException {
        JsonParser parser = new JsonParser();
        ClassLoader classLoader = GraphHelper.class.getClassLoader();
        String path = classLoader.getResource("log4j.properties").getPath();
        path = path.substring(0, path.lastIndexOf("/"));
        JsonElement a = parser.parse(new FileReader(path + "/" + folderName + "/" + folderName + ".txt" + i));
        return a.getAsJsonArray();
    }

    public static JsonArray type1() throws FileNotFoundException {
        JsonParser parser = new JsonParser();
        //JsonElement a = parser.parse(new FileReader("/Users/saurav/src/GraphJet2/UM_1.json"));
        JsonElement a = parser.parse(new FileReader("/Users/saurav/src/GraphJet2/output.txt"));
        return a.getAsJsonObject().get("hits").getAsJsonObject().get("hits").getAsJsonArray();
    }

}