import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
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

//            ignite.events().localListen(new IgnitePredicate<DiscoveryEvent>() {
//                @Override public boolean apply(DiscoveryEvent event) {
//                    System.out.println("\"++Local discovery event node:  " +  event.type() + " " + event.eventNode());
//                    return true;
//                }
//            }, EventType.EVT_NODE_LEFT);


//        } finally {
//            log.info("Clisent Stop!!!");
//        }
    }

    static private IgniteBiPredicate<UUID,DiscoveryEvent> cerateListener() {
        return new IgniteBiPredicate<UUID,DiscoveryEvent>() {
            @Override
            public boolean apply(UUID uuid, DiscoveryEvent event) {
                System.out.println("\"++Local discovery event node:  " +  event.type() + " " + event.eventNode());
                return true;
            }
        };
    }

    private static class RemoteDiscoveryEventFilter implements IgnitePredicate<DiscoveryEvent> {

        @Override public boolean apply(DiscoveryEvent event) {
            System.out.println("++Remote discovery event node:  " +  event.type() + " " + event.eventNode());
            return true;
        }

    }
}
