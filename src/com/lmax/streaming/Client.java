package com.lmax.streaming;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.management.ReflectionException;

public class Client
{
    public static void main(String[] args) throws Exception
    {
        if (2 != args.length)
        {
            System.out.println("Usage Client <url> <number of clients>");
            return;
        }
        
        int numberOfClients = Integer.parseInt(args[1]);
        Executor executor = Executors.newFixedThreadPool(numberOfClients);
        List<Reader> readers = new ArrayList<Reader>();
        for (int i = 0; i < numberOfClients; i++)
        {
            Reader reader = new Reader(args[0]);
            executor.execute(reader);
            readers.add(reader);
        }
        
        ReaderList readerList = new ReaderList(readers);
        ManagementFactory.getPlatformMBeanServer().registerMBean(readerList, new ObjectName("com.lmax:type=readers"));
    }
    
    public static class ReaderList implements DynamicMBean
    {
        private final Map<String, Reader> readers = new HashMap<String, Client.Reader>();

        public ReaderList(List<Reader> readers)
        {
            int i = 0;
            for (Reader reader : readers)
            {
                this.readers.put("reader." + i++, reader);
            }
        }
        
        @Override
        public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException,
                                                    ReflectionException
        {
            return readers.get(attribute).getBytes();
        }
        
        @Override
        public AttributeList getAttributes(String[] attributes)
        {
            AttributeList list = new AttributeList();
            for (String name : attributes)
            {
                list.add(readers.get(name).getBytes());
            }
            return list;
        }
        
        @Override
        public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException,
                                                                                    ReflectionException
        {
            return null;
        }
        
        @Override
        public void setAttribute(Attribute attribute) throws AttributeNotFoundException,
                                                     InvalidAttributeValueException, MBeanException,
                                                     ReflectionException
        {
        }
        
        @Override
        public AttributeList setAttributes(AttributeList attributes)
        {
            return null;
        }
        
        @Override
        public MBeanInfo getMBeanInfo()
        {
            MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[readers.size()];
            int i = 0;
            for (Entry<String, Reader> entry : readers.entrySet())
            {
                attributes[i++] = new MBeanAttributeInfo(entry.getKey(), "java.lang.Long", entry.getKey(), true, false, false);
            }
            
            return new MBeanInfo(ReaderList.class.getName(), "Readers", attributes, null, null, null);
        }
    }
    
    public static class Reader implements Runnable
    {
        private final String url;
        private long bytes = 0;

        public Reader(String url)
        {
            this.url = url;
        }
        
        public long getBytes()
        {
            return bytes;
        }
        
        @Override
        public void run()
        {
            try
            {
                URI uri = new URI(url);
                HttpURLConnection cn = (HttpURLConnection) uri.toURL().openConnection();
                
                cn.setDoOutput(true);
                
                InputStream inputStream = cn.getInputStream();
                
                byte[] buffer = new byte[1024];
                while (true)
                {
                    if (bytes == 0)
                    {                        
                        System.out.println("Started reading from: " + url);
                    }
                    
                    int len = inputStream.read(buffer);                    
                    bytes += len;
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }            
        }
    }
}
