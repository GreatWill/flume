package sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

/**
 * @Description:
 * @Auther: chenjw
 * @Date: 2018/9/19 10:28
 */
public class FirstSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(FirstSink.class);

    private static final String PROP_KEY_FILENAME = "fileName";  //接受配置文件传过来的参数，key

    private String fileName; //在config中初始化

    public void configure(Context context) {
        //context里面是flume配置文件对应的key、value
        fileName = context.getString(PROP_KEY_FILENAME);
    }

    public Status process() throws EventDeliveryException {

        System.out.println("-------------------start my sink -----------------");
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        Event event = null;
        System.out.println("-------------------ready to try -----------------");
        try{
            txn.begin();
            System.out.println("-------------- txn.begin -----------------");
            while(true){
                System.out.println("------------------ while -----------------");
                event = ch.take();
                if(event != null)
                    break;
            }
            logger.debug("Get event");
            String body = new String(event.getBody());
            String res = body + ":" + System.currentTimeMillis() + "\r\n";
            File file = new File(fileName);
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(file, true);

                fos.write(res.getBytes());

                fos.close();

            } catch (Exception e) {
                e.printStackTrace();
            }

            txn.commit();

        }catch (Throwable th){
            //事物执行不成功,回归事物
            txn.rollback();

            if(th instanceof Error){
                throw (Error) th;
            }else{
                throw new EventDeliveryException(th);
            }
        }finally {
            txn.close();
        }
        return Status.READY;
    }


}
