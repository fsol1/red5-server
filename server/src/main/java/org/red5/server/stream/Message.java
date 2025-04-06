package org.red5.server.stream;

import java.io.IOException;
import java.util.Collection;
import org.slf4j.Logger;

import org.red5.server.api.stream.IBroadcastStream;
import org.red5.server.api.stream.IStreamListener;
import org.red5.server.api.stream.IStreamPacket;
import org.red5.server.messaging.IMessage;
import org.red5.server.messaging.IMessageInput;
import org.red5.server.messaging.IMessageOutput;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.stream.message.RTMPMessage;
import org.red5.server.stream.message.ResetMessage;

public class Message {

    public Message() {
    }

    /**
     * Push message
     */
    public void pushMessage(IMessage message, IMessageOutput msgOut, Collection<IStreamListener> listeners, IBroadcastStream stream, Logger log) throws IOException {
        if (msgOut != null) {
            msgOut.pushMessage(message);
        }
        // Notify listeners about received packet
        if (message instanceof RTMPMessage) {
            final IRTMPEvent rtmpEvent = ((RTMPMessage) message).getBody();
            if (rtmpEvent instanceof IStreamPacket) {
                for (IStreamListener listener : listeners) {
                    try {
                        listener.packetReceived(stream, (IStreamPacket) rtmpEvent);
                    } catch (Exception e) {
                        log.error("Error while notifying listener " + listener, e);
                    }
                }
            }
        }
    }

    /**
     * Send reset message
     */
    public void sendResetMessage(IMessageOutput msgOut, Collection<IStreamListener> listeners, IBroadcastStream stream, Logger log) {
        // Send new reset message
        try {
            this.pushMessage(new ResetMessage(), msgOut, listeners, stream, log);
        } catch (IOException err) {
            log.error("Error while sending reset message.", err);
        }
    }

    /**
     * Getter for next RTMP message.
     *
     * @return Next RTMP message
     */
    protected RTMPMessage getNextRTMPMessage(IMessageInput msgIn, Logger log) {
        IMessage message;
        do {
            // Pull message from message input object...
            try {
                message = msgIn.pullMessage();
            } catch (Exception err) {
                log.error("Error while pulling message.", err);
                message = null;
            }
            // If message is null then return null
            if (message == null) {
                return null;
            }
        } while (!(message instanceof RTMPMessage));
        // Cast and return
        return (RTMPMessage) message;
    }
}
