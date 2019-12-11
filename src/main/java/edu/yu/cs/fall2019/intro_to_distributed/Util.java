package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class Util
{
    public static byte[] readAllBytes(InputStream in) throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int numberRead;
        byte[] data = new byte[40960];
        while ((numberRead = in.read(data, 0, data.length)) != -1)
        {
            buffer.write(data, 0, numberRead);
        }
        return buffer.toByteArray();
    }

    public static Thread startAsDaemon(Runnable run, String name)
    {
        Thread thread = new Thread(run,name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }
}