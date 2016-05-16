import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Created by gridgan on 31.03.2016.
 */
public class ClientNode {

    private static Logger log = LoggerFactory.getLogger(ClientNode.class);

    /** Cache name. */
    private static final String CACHE_NAME = "query";

    public static void main(String[] args) throws InterruptedException {
//        try (
            final Ignite ignite = Ignition.start("client-ignite.xml");
//        ) {
            log.info("Clisent Start!!!");
            IgniteBiPredicate<UUID,DiscoveryEvent> localListener = cerateListener();
            IgnitePredicate<DiscoveryEvent> remListener = new RemoteDiscoveryEventFilter();

            ignite.events(ignite.cluster().forServers()).remoteListen(localListener, remListener,
                EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_SEGMENTED, EventType.EVT_NODE_JOINED);


//        } finally {
//            log.info("Clisent Stop!!!");
//        }
    }

    static private IgniteBiPredicate<UUID,DiscoveryEvent> cerateListener() {
        return new IgniteBiPredicate<UUID,DiscoveryEvent>() {
            @Override
            public boolean apply(UUID uuid, DiscoveryEvent event) {
                System.out.println("\"++Local discovery event type:  " +  event.type());
                System.out.println("\"++Local discovery event node:  " +  event.eventNode());
                if (event.type()==(EventType.EVT_NODE_FAILED) || event.type()==(EventType.EVT_NODE_LEFT)
                    || event.type()==(EventType.EVT_NODE_SEGMENTED)|| event.type()==(EventType.EVT_NODE_JOINED)) {
                    event.eventNode().addresses();
                    for (String addr: event.eventNode().addresses()) {
                        // Инвалидирем кэши
                        System.out.println("\"++Local discovery event:  " +  event.eventNode());

                    };
                    return  true;
                }
                return false;
            }
        };
    }

    private static class RemoteDiscoveryEventFilter implements IgnitePredicate<DiscoveryEvent> {

        @Override public boolean apply(DiscoveryEvent event) {
            System.out.println("++Remote discovery event type:  " +  event.type());
            System.out.println("++Remote discovery event: " + event);
            if (event.type()==(EventType.EVT_NODE_FAILED) || event.type()==(EventType.EVT_NODE_LEFT)
                || event.type()==(EventType.EVT_NODE_SEGMENTED)|| event.type()==(EventType.EVT_NODE_JOINED))
                return  true;
            return false;
        }

    }
}
