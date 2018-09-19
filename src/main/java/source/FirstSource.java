package source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class FirstSource extends AbstractSource implements PollableSource, Configurable {

    public Status process() throws EventDeliveryException {
        Random random = new Random();
        int num = random.nextInt(100);
        String text = "hello world" + num;
        Map<String, String> header = new HashMap<String, String>();
        header.put("id",Integer.toString(num));
        this.getChannelProcessor().processEvent(EventBuilder.withBody(text, Charset.forName("UTF-8"), header));
        return Status.READY;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    public void configure(Context context) {

    }
}
