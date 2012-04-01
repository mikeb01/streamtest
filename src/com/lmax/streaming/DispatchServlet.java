package com.lmax.streaming;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;
import java.util.zip.CRC32;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

@SuppressWarnings("serial")
public class DispatchServlet extends HttpServlet
{
    private static final Logger LOGGER = Logger.getLogger("LOADTEST");
    
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Queue<AsyncContext> contexts = new ConcurrentLinkedQueue<AsyncContext>();
    private volatile Disruptor<byte[]> disruptor;
    private Generator generator;
    
    @SuppressWarnings("unchecked")
    @Override
    public void init() throws ServletException
    {
        try
        {
            disruptor = new Disruptor<byte[]>(new EventFactory<byte[]>()
            {
                @Override
                public byte[] newInstance()
                {
                    return new byte[256];
                }
            }, 262144, executor);
            
            disruptor.handleEventsWith(new LogicHandler());
            disruptor.start();
            generator = new Generator(executor);
            
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            mBeanServer.registerMBean(new StandardMBean(generator, GeneratorMBean.class), new ObjectName("com.lmax:type=generator"));            
        }
        catch (Exception e)
        {
            throw new ServletException(e);
        }
    }
    
    @Override
    protected void service(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException
    {
        Data data = (Data) req.getAttribute("data");
        if (null == data)
        {
            contexts.add(req.startAsync(req, res));
        }
        else
        {
            PrintWriter writer = res.getWriter();
            data.print(writer);
            writer.flush();
            contexts.add(req.startAsync(req, res));
        }
    }
    
    public interface GeneratorMBean
    {
        public long getRate();
        public void setRate(long value);
        public long getErrorCount();
        public long getCursor();
        
        public void start();
        public void stop();
    }
    
    public class Generator implements Runnable, GeneratorMBean
    {        
        private final byte[] input = new byte[256];
        private volatile long rate = 1024;
        private long errorCount = 0;
        private final ExecutorService executor;
        private volatile Future<?> submit;
        
        public Generator(ExecutorService executor)
        {
            this.executor = executor;
            Random r = new Random(55);
            r.nextBytes(input);
        }
        
        public void start()
        {
            submit = executor.submit(this);
        }
        
        public void stop()
        {
            submit.cancel(true);
        }
        
        public long getCursor()
        {
           return disruptor.getRingBuffer().getCursor();
        }
        
        @Override
        public void run()
        {
            try
            {
                RingBuffer<byte[]> ringBuffer = disruptor.getRingBuffer();
                long previousRate = 0;
                
                // Warm up
                if (!sendMessages(ringBuffer, rate, 30))
                {
                    LOGGER.warning("Failed during warmup");
                    return;
                }
                
                while (true)
                {                
                    if (sendMessages(ringBuffer, rate, 10))
                    {
                        previousRate = rate;
                        rate *= 2;
                    }
                    else
                    {
                        LOGGER.info("Failed at: " + rate + " messages per second");
                        break;
                    }
                }
                
                long upperBound = rate;
                long lowerBound = previousRate;
                
                while (true)
                {
                    long newRate = (upperBound + lowerBound) / 2;
                    long rateDelta = newRate - rate;
                    
                    if (-16 < rateDelta && rateDelta < 16)
                    {
                        LOGGER.info("Stable at: " + rate + " messages per second");
                        return;
                    }
                    
                    previousRate = rate;
                    rate = newRate;
                    
                    if (sendMessages(ringBuffer, rate, 10))
                    {
                        lowerBound = rate;
                    }
                    else
                    {
                        upperBound = rate;
                    }
                }
            }
            catch (InterruptedException e)
            {
                LOGGER.warning("Exiting...");
            }
        }

        private boolean sendMessages(RingBuffer<byte[]> ringBuffer, long messagesPerSecond, int runtimeSeconds) throws InterruptedException
        {
            LOGGER.info("Rate: " + messagesPerSecond + ", for " + runtimeSeconds + "s");
            
            long runtimeNanos = TimeUnit.SECONDS.toNanos(runtimeSeconds);
            
            long t0 = System.nanoTime();
            long delta = 0;
            long sent = 0;
            
            do
            {
                delta = System.nanoTime() - t0;
                long shouldHaveSent = (messagesPerSecond * delta) / 1000000000;
                
                for (; sent < shouldHaveSent; sent++)
                {
                    if (!send(ringBuffer))
                    {
                        return false;
                    }
                }
                
                LockSupport.parkNanos(1);
            }
            while (delta <= runtimeNanos);
            
            int count = 0;
            do
            {
                Thread.sleep(1000);
                count++;
            } 
            while (!ringBuffer.hasAvailableCapacity(ringBuffer.getBufferSize()));
            
            return count == 1;
        }

        private boolean send(RingBuffer<byte[]> ringBuffer)
        {
            
            if (ringBuffer.hasAvailableCapacity(1))
            {
                long next = ringBuffer.next();
                byte[] event = ringBuffer.get(next);
                System.arraycopy(input, 0, event, 0, event.length);
                ringBuffer.publish(next);
                return true;
            }
            else
            {
                errorCount++;
                return false;
            }
        }

        public long getRate()
        {
            return rate;
        }

        public void setRate(long rate)
        {
            this.rate = rate;
        }

        public long getErrorCount()
        {
            return errorCount;
        }
    }
    
    public class LogicHandler implements EventHandler<byte[]>
    {
        private final CRC32 crc32 = new CRC32();
        private final MessageDigest md5Digest;
        private final MessageDigest sha1Digest;
        
        public LogicHandler() throws NoSuchAlgorithmException
        {
            md5Digest = MessageDigest.getInstance("MD5");
            sha1Digest = MessageDigest.getInstance("SHA1");
        }
        
        @Override
        public void onEvent(byte[] data, long seq, boolean endOfBatch) throws Exception
        {
            crc32.update(data);
            long crc = crc32.getValue();
            
            byte[] md5 = md5Digest.digest(data);
            byte[] sha1 = sha1Digest.digest(data);
            
            Data d = new Data(String.valueOf(crc), toHex(md5), toHex(sha1));
            int size = contexts.size();
            
            for (int i = 0; i < size; i++)
            {
                AsyncContext context = contexts.poll();
                if (null != context)
                {
                    context.getRequest().setAttribute("data", d);
                    try
                    {
                        context.dispatch();
                    }
                    catch (Exception e)
                    {
                    }
                }
                else
                {
                    break;
                }
            }
            
            crc32.reset();
            md5Digest.reset();
            sha1Digest.reset();
        }
    }
    
    private String toHex(byte[] bs)
    {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bs)
        {            
            String hex = Integer.toHexString(0xFF & b);
            if (hex.length() == 1) 
            {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
    
    private static class Data
    {
        private final String crc;
        private final String md5;
        private final String sha1;

        public Data(String crc, String md5, String sha1)
        {
            this.crc = crc;
            this.md5 = md5;
            this.sha1 = sha1;
        }
        
        public void print(Writer writer) throws IOException
        {
            for (int i = 0; i < 10; i++)
            {
                writer.append("crc=");
                writer.append(crc);
                writer.append(",mds=");
                writer.append(md5);
                writer.append(",sha1=");
                writer.append(sha1);
                writer.append("\n");
            }
            writer.append("\n");
        }
    }
}
