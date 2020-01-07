package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import edu.yu.cs.fall2019.intro_to_distributed.Util;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class Client {


    public static Response sendHTTP(String hostName, int hostPort, String context, String toCompile) throws IOException {
        URL serverURL = new URL("http://" + hostName + ":" + hostPort + "/" + context);

        HttpURLConnection connect = (HttpURLConnection) serverURL.openConnection();
        connect.setRequestMethod("GET");
        if(toCompile == null) {
            connect.connect();
        } else {
            connect.setDoOutput(true);
            DataOutputStream stream = new DataOutputStream(connect.getOutputStream());
            stream.writeBytes(toCompile);
            stream.flush();
            stream.close();
        }

        int responseCode = connect.getResponseCode();
        String responseBody;
        if(responseCode == 200) {
            responseBody = new String(Util.readAllBytes(connect.getInputStream()));
        } else {
            responseBody = new String(Util.readAllBytes(connect.getErrorStream()));
        }

        Response resp = new Response(responseCode, responseBody);

        return resp;
    }

    static class Response
    {
        private int code;
        private String body;
        public Response(int code, String body)
        {
            this.code = code;
            this.body = body;
        }
        public int getCode()
        {
            return this.code;
        }
        public String getBody()
        {
            return this.body;
        }
    }
}
