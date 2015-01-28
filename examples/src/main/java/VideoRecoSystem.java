import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rupesh on 27/1/15.
 */

//10.132.235.67:3000

public class VideoRecoSystem {

    static int numUsers , numCategories;
    static ArrayList<String> itemDetails;
    static ArrayList<Integer> allUsers;
    static ArrayList<Integer> itemIds;
    static ArrayList<Double> ratings;
    static ArrayList<String> allCategories;


    public static class UserCatReco{
        ArrayList<ArrayList<Integer>> recommendations = new ArrayList<ArrayList<Integer>>();
        public UserCatReco(){
            recommendations = new ArrayList<ArrayList<Integer>>();
            for(int i = 0 ; i < numCategories ; i++) recommendations.add(new ArrayList<Integer>());
        }
    }

    public static void main(String args[]) throws IOException, TasteException, JSONException {
        InputStreamReader input = new InputStreamReader(System.in);
        BufferedReader br = new BufferedReader(input);
        videosReco();
    }

    public static ArrayList<String> fetchData(String s , int flag) throws IOException {
        //  System.setProperty("java.net.useSystemProxies", "true");
        BufferedReader reader = null;
        InputStream is = new URL(s).openStream();
        try {
            BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
            String jsonText = readAll(rd);
            JSONArray json = new JSONArray(jsonText);
            int length = json.length();
            itemDetails = new ArrayList<String>();

            if(flag == 0) {
                for (int i = 0; i < length; i++) {
                    String rating = String.valueOf(json.getJSONObject(i).get("rating"));
                    String Id = String.valueOf(json.getJSONObject(i).get("id"));
                    String videoId = String.valueOf(json.getJSONObject(i).get("videoId"));
                    String str = Id + "," + videoId + "," + rating;
                    itemDetails.add(str);
                }
            }
            else{
                for (int i = 0; i < length; i++) {

                    String Id = String.valueOf(json.getJSONObject(i).get("id"));

                    itemDetails.add(Id);
                }
            }


            return itemDetails;
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            is.close();
        }
        return null;
    }
    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }


    public static JSONArray videosReco() throws IOException, TasteException, JSONException {

        allCategories = getAllCategories();
        numCategories = allCategories.size();
        allUsers = getAllUsers();
        numUsers = allUsers.size();

        int clicks[][] = new int[numUsers][numCategories];
        int totalItems[][] = new int[numUsers][numCategories];
        double interest[][] = new double[numUsers][numCategories];
        int numDownloads[][] = new int[numUsers][numCategories];
        double tempSum[] = new double[numUsers];

        for(int i = 0 ; i < numUsers ; i++){
            for(int j = 0 ; j < numCategories ; j++){
                ratings = new ArrayList<Double>();
                itemIds = new ArrayList<Integer>();
                populateList(allUsers.get(i) , allCategories.get(j));
                clicks[i][j] = getSum(ratings);
                totalItems[i][j] = itemIds.size();
                interest[i][j] = (double)clicks[i][j]/totalItems[i][j];
                tempSum[i] += interest[i][j];
            }
        }

        for(int i = 0 ; i < numUsers ; i++) {
            for (int j = 0; j < numCategories; j++) {
                interest[i][j] = interest[i][j] / tempSum[i];
                numDownloads[i][j] = (int) ( interest[i][j] *10);
            }
        }

        ArrayList<ArrayList<Integer>> recommendations = new ArrayList<ArrayList<Integer>>();
        int breakingIndex[][] = new int[numUsers][numCategories];
        UserCatReco userDetails[] = new UserCatReco[numUsers];
        for(int i = 0 ; i < numUsers ; i++) {
            userDetails[i] = new UserCatReco();
            recommendations.add(new ArrayList<Integer>());
        }

        for(int j = 0 ; j < numCategories ; j++){
            File f = getAllVideosForCategory(allCategories.get(j));                     //Format: userId,itemId,clicks
            ArrayList<ArrayList<Integer>> reco = recoInCategory(f);
            for(int i = 0 ; i < numUsers ; i++){
                if(reco.size() != 0) {
                    breakingIndex[i][j] = reco.get(i).size();
                    for (int k = 0; k < breakingIndex[i][j]; k++) {
                        int s = reco.get(i).get(k);
                        userDetails[i].recommendations.get(j).add(s);
                    }
                }
            }
        }

        for(int i = 0 ; i < numUsers ; i++){
            int flag = 0;
            while(flag == 0) {
                int counter = 0;
                for (int j = 0; j < numCategories; j++) {
                    int len = userDetails[i].recommendations.get(j).size();
                    for (int k = 0; k < numDownloads[i][j]; k++) {
                        if (len > 0) {
                            int s = userDetails[i].recommendations.get(j).get(0);
                            recommendations.get(i).add(s);
                            userDetails[i].recommendations.get(j).remove(0);
                            len--;
                        }
                    }
                    counter += len;
                }
                if(counter == 0) flag = 1;
            }
        }
        JSONArray a = convertRecoToJson(recommendations);
 //       executePost(a);
        return a;
    }

//    public static String executePost(JSONArray jsonArray ) throws JSONException {
//        URL url;
//        JSONObject j = new JSONObject();
//        j.put("array",jsonArray);
//        HttpURLConnection connection = null;
//        try {
//            //Create connection
//            url = new URL("http://10.132.235.67:3000/postVLogs");
//            connection = (HttpURLConnection)url.openConnection();
//            connection.setRequestMethod("POST");
//            connection.setRequestProperty("Content-Type",
//                    "application/x-www-form-urlencoded");
//
//            connection.setRequestProperty("Content-Length", "" +
//                    Integer.toString(j.toString().getBytes().length));
//            connection.setRequestProperty("Content-Language", "en-US");
//
//            connection.setUseCaches (false);
//            connection.setDoInput(true);
//            connection.setDoOutput(true);
//
//            //Send request
//            DataOutputStream wr = new DataOutputStream (
//                    connection.getOutputStream ());
//            wr.writeBytes (j.toString());
//            wr.flush ();
//            wr.close ();
//
//            //Get Response
//            InputStream is = connection.getInputStream();
//            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
//            String line;
//            StringBuffer response = new StringBuffer();
//            while((line = rd.readLine()) != null) {
//                response.append(line);
//                response.append('\r');
//            }
//            rd.close();
//            return response.toString();
//
//        } catch (Exception e) {
//
//            e.printStackTrace();
//            return null;
//
//        } finally {
//
//            if(connection != null) {
//                connection.disconnect();
//            }
//        }
//    }

    private static JSONArray convertRecoToJson(ArrayList<ArrayList<Integer>> list) throws JSONException, IOException {
        int l = list.size();
        JSONArray jsonArray = new JSONArray();
        for(int i = 0 ; i < l ; i++){
            JSONObject obj = new JSONObject();
            int sz = list.get(i).size();
            for(int j = 0 ; j < sz ; j++){
                String s = "V" + list.get(i).get(j);
                obj.put("videoId" , s);
            }
            jsonArray.put(obj);
        }

        FileWriter file = new FileWriter("/home/rupesh/RecommendedFiles/RecommendationVideos.txt");
        try {
            for(int i = 0 ; i < l ; i++) {
                file.write(jsonArray.get(i).toString());
            }

        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            file.flush();
            file.close();
        }

        return jsonArray;
    }


    private static int getSum(ArrayList<Double> ratings) {
        int size = ratings.size();
        int sum =0;
        for(int i=0;i<size;i++)
        {
            sum+=ratings.get(i);
        }
        return sum;
    }
    //Fetch List of ItemIds and List of ratings.
    private static void populateList(Integer userId, String category) throws IOException {
        String s = "http://10.132.235.67:3000/VLogsCatUser/" + category + "/" + userId;
        ArrayList<String> details = fetchData(s , 0);
        int l = details.size();
        for(int i = 0 ; i < l ; i++){
            String token[] = details.get(i).split(",");
            itemIds.add(Integer.parseInt(token[1].substring(1)));
            ratings.add(Double.parseDouble(token[2]));
        }
        return;
    }

    private static void fetchItemsForUser(Integer userId, ArrayList<String> list) {
        //TODO: Check and find all itemids for user:userId.

    }

    private static File getAllVideosForCategory(String category) throws IOException {

        //TODO:Write code to fetch all videos corr to category

        ArrayList<String> list = fetchData("http://10.132.235.67:3000/VLogsCat/" + category , 0);

        //  ArrayList<String> itemDetails = fetchItemsForUser(1,list);
        File file = new File("DataforCategory.csv");
        file.createNewFile();
        FileWriter writer = new FileWriter(file);
        int l = list.size();
        for(int i = 0 ; i < l ; i++){
            String token[] = list.get(i).split(",");
            String s = token[0] + "," + token[1].substring(1) + "," + token[2];
            writer.write(s + "\n");
        }
        writer.flush();
        writer.close();
        return file;
    }

    private static ArrayList<String> getAllCategories() {
        ArrayList<String> a = new ArrayList<String>();
        a.add("1");
        a.add("2");
        a.add("3");
        a.add("4");
        return a;
    }

    private static ArrayList<Integer> getAllUsers() throws IOException {
        ArrayList<String> a = fetchData("http://10.132.235.67:3000/VUsers" , 2);
        ArrayList<Integer> users = new ArrayList<Integer>();
        int l = a.size();
        for(int i = 0 ; i < l ; i++){
            users.add(Integer.parseInt(a.get(i)));
        }
        return users;
    }

    public static ArrayList<ArrayList<Integer>> recoInCategory(File f) throws IOException, TasteException {
        ArrayList<ArrayList<Integer>> recommendation = new ArrayList<ArrayList<Integer>>();
        if(f.length() == 0) return recommendation;
        DataModel model = new FileDataModel(f);      //Required file format: userId , ItemId , rating
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
        UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
        ArrayList<Integer> users = getAllUsers();
        int numUsers = users.size();

        for(int i = 0 ; i < numUsers ; i++){
            try {
                List<RecommendedItem> recommendations = recommender.recommend(users.get(i), 20);
                int l = recommendations.size();
                recommendation.add(new ArrayList<Integer>());
                for (int j = 0; j < l; j++) {
                    long fileId = recommendations.get(j).getItemID();
                    recommendation.get(i).add((int) fileId);
                }
            }
            catch(NoSuchUserException e){
                recommendation.add(new ArrayList<Integer>());
            }
        }

        return recommendation;
    }
}
//TODO:get the list of categories on the database

