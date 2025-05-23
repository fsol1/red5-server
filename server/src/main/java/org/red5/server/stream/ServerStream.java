/*
 * RED5 Open Source Media Server - https://github.com/Red5/ Copyright 2006-2023 by respective authors (see below). All rights reserved. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless
 * required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.red5.server.stream;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.mina.core.buffer.IoBuffer;
import org.red5.codec.IAudioStreamCodec;
import org.red5.codec.IStreamCodecInfo;
import org.red5.codec.IVideoStreamCodec;
import org.red5.codec.StreamCodecInfo;
import org.red5.server.api.IContext;
import org.red5.server.api.scheduling.IScheduledJob;
import org.red5.server.api.scheduling.ISchedulingService;
import org.red5.server.api.scope.IScope;
import org.red5.server.api.stream.IPlayItem;
import org.red5.server.api.stream.IPlaylistController;
import org.red5.server.api.stream.IServerStream;
import org.red5.server.api.stream.IStreamAwareScopeHandler;
import org.red5.server.api.stream.IStreamListener;
import org.red5.server.api.stream.IStreamPacket;
import org.red5.server.api.stream.StreamState;
import org.red5.server.api.stream.support.SimplePlayItem;
import org.red5.server.messaging.IFilter;
import org.red5.server.messaging.IMessage;
import org.red5.server.messaging.IMessageComponent;
import org.red5.server.messaging.IMessageInput;
import org.red5.server.messaging.IMessageOutput;
import org.red5.server.messaging.IPassive;
import org.red5.server.messaging.IPipe;
import org.red5.server.messaging.IPipeConnectionListener;
import org.red5.server.messaging.IProvider;
import org.red5.server.messaging.IPushableConsumer;
import org.red5.server.messaging.OOBControlMessage;
import org.red5.server.messaging.PipeConnectionEvent;
import org.red5.server.net.rtmp.event.AudioData;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.net.rtmp.event.VideoData;
import org.red5.server.stream.message.RTMPMessage;
import org.red5.server.stream.message.ResetMessage;
import org.red5.server.util.ScopeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation for server side stream.
 *
 * @author The Red5 Project
 * @author Steven Gong (steven.gong@gmail.com)
 * @author Paul Gregoire (mondain@gmail.com)
 */
public class ServerStream extends AbstractStream implements IServerStream, IFilter, IPushableConsumer, IPipeConnectionListener {

    private static final Logger log = LoggerFactory.getLogger(ServerStream.class);

    private static final long WAIT_THRESHOLD = 0;

    private static EnumSet<StreamState> PLAYING_OR_PAUSED = EnumSet.of(StreamState.PLAYING, StreamState.PAUSED);

    /**
     * Stream published name
     */
    protected String publishedName;

    /**
     * Actual playlist controller
     */
    protected IPlaylistController controller;

    /**
     * Default playlist controller
     */
    protected IPlaylistController defaultController;

    /**
     * Rewind flag state
     */
    private boolean isRewind;

    /**
     * Random flag state
     */
    private boolean isRandom;

    /**
     * Repeat flag state
     */
    private boolean isRepeat;

    /**
     * Message input
     */
    private IMessageInput msgIn;

    /**
     * Message output
     */
    private IMessageOutput msgOut;

    /**
     * Provider service
     */
    private IProviderService providerService;

    /**
     * Scheduling service
     */
    private ISchedulingService scheduler;

    /**
     * Live broadcasting scheduled job name
     */
    private volatile String liveJobName;

    /**
     * VOD scheduled job name
     */
    private volatile String vodJobName;

    /**
     * VOD start timestamp
     */
    private long vodStartTS;

    /**
     * Server start timestamp
     */
    private long serverStartTS;

    /**
     * Next msg's timestamp
     */
    private long nextTS;

    /**
     * Next RTMP message
     */
    private RTMPMessage nextRTMPMessage;

    /** Listeners to get notified about received packets. */
    private CopyOnWriteArraySet<IStreamListener> listeners = new CopyOnWriteArraySet<>();

    /**
     * Recording listener
     */
    private WeakReference<IRecordingListener> recordingListener;

    /**
     * Message
     */
    private Message message;

    /**
     * Item
     */
    private Item item;

    /** Constructs a new ServerStream. */
    public ServerStream() {
        defaultController = new SimplePlaylistController();
        message = new Message();
        item = new Item();
    }

    /** {@inheritDoc} */
    public void addItem(IPlayItem item) {
        this.item.addItem(item);
    }

    /** {@inheritDoc} */
    public void addItem(IPlayItem item, int index) {
        this.item.addItem(item, index);
    }

    /** {@inheritDoc} */
    public void removeItem(int index) {
        this.item.removeItem(index);
    }

    /** {@inheritDoc} */
    public void removeAllItems() {
        this.item.removeAllItems();
    }

    /** {@inheritDoc} */
    public int getItemSize() {
        return this.item.getItemSize();
    }

    public CopyOnWriteArrayList<IPlayItem> getItems() {
        return this.item.getItems();
    }

    /** {@inheritDoc} */
    public int getCurrentItemIndex() {
        return this.item.getCurrentItemIndex();
    }

    /** {@inheritDoc} */
    public IPlayItem getCurrentItem() {
        return this.item.getCurrentItem();
    }

    /** {@inheritDoc} */
    public IPlayItem getItem(int index) {
        return this.item.getItem(index);
    }

    /** {@inheritDoc} */
    public void previousItem() {
        stop();
        moveToPrevious();
        int index = this.item.getCurrentItemIndex();
        if (index == -1) {
            return;
        }
        IPlayItem item = this.item.getItems().get(index);
        play(item);
    }

    /** {@inheritDoc} */
    public boolean hasMoreItems() {
        return this.item.hasMoreItems(isRepeat);
    }

    /** {@inheritDoc} */
    public void nextItem() {
        stop();
        moveToNext();
        int index = this.item.getCurrentItemIndex();
        if (index == -1) {
            return;
        }
        IPlayItem item = this.item.getItems().get(index);
        play(item);
    }

    /** {@inheritDoc} */
    public void setItem(int index) {
        if (index < 0 || index >= this.item.getItemSize()) {
            return;
        }
        stop();
        this.item.setCurrentItemIndex(index);
        IPlayItem item = this.item.getItems().get(index);
        play(item);
    }

    /** {@inheritDoc} */
    public boolean isRandom() {
        return isRandom;
    }

    /** {@inheritDoc} */
    public void setRandom(boolean random) {
        isRandom = random;
    }

    /** {@inheritDoc} */
    public boolean isRewind() {
        return isRewind;
    }

    /** {@inheritDoc} */
    public void setRewind(boolean rewind) {
        isRewind = rewind;
    }

    /** {@inheritDoc} */
    public boolean isRepeat() {
        return isRepeat;
    }

    /** {@inheritDoc} */
    public void setRepeat(boolean repeat) {
        isRepeat = repeat;
    }

    /** {@inheritDoc} */
    public void setPlaylistController(IPlaylistController controller) {
        this.controller = controller;
    }

    /** {@inheritDoc} */
    public void saveAs(String name, boolean isAppend) throws IOException {
        // one recording listener at a time via this entry point
        if (recordingListener == null) {
            IScope scope = getScope();
            // create a recording listener
            IRecordingListener listener = (IRecordingListener) ScopeUtils.getScopeService(scope, IRecordingListener.class, RecordingListener.class);
            // initialize the listener
            if (listener.init(scope, name, isAppend)) {
                // get decoder info if it exists for the stream
                IStreamCodecInfo codecInfo = getCodecInfo();
                log.debug("Codec info: {}", codecInfo);
                if (codecInfo instanceof StreamCodecInfo) {
                    StreamCodecInfo info = (StreamCodecInfo) codecInfo;
                    IVideoStreamCodec videoCodec = info.getVideoCodec();
                    log.debug("Video codec: {}", videoCodec);
                    if (videoCodec != null) {
                        //check for decoder configuration to send
                        IoBuffer config = videoCodec.getDecoderConfiguration();
                        if (config != null) {
                            log.debug("Decoder configuration is available for {}", videoCodec.getName());
                            VideoData videoConf = new VideoData(config.asReadOnlyBuffer());
                            try {
                                log.debug("Setting decoder configuration for recording");
                                listener.getFileConsumer().setVideoDecoderConfiguration(videoConf);
                            } finally {
                                videoConf.release();
                            }
                        }
                    } else {
                        log.debug("Could not initialize stream output, videoCodec is null.");
                    }
                    IAudioStreamCodec audioCodec = info.getAudioCodec();
                    log.debug("Audio codec: {}", audioCodec);
                    if (audioCodec != null) {
                        //check for decoder configuration to send
                        IoBuffer config = audioCodec.getDecoderConfiguration();
                        if (config != null) {
                            log.debug("Decoder configuration is available for {}", audioCodec.getName());
                            AudioData audioConf = new AudioData(config.asReadOnlyBuffer());
                            try {
                                log.debug("Setting decoder configuration for recording");
                                listener.getFileConsumer().setAudioDecoderConfiguration(audioConf);
                            } finally {
                                audioConf.release();
                            }
                        }
                    } else {
                        log.debug("No decoder configuration available, audioCodec is null.");
                    }
                }
                // set as primary listener
                recordingListener = new WeakReference<>(listener);
                // add as a listener
                addStreamListener(listener);
                // start the listener thread
                listener.start();
            } else {
                log.warn("Recording listener failed to initialize for stream: {}", name);
            }
        } else {
            log.info("Recording listener already exists for stream: {}", name);
        }
    }

    /** {@inheritDoc} */
    public String getSaveFilename() {
        if (recordingListener != null) {
            return recordingListener.get().getFileName();
        }
        return null;
    }

    /** {@inheritDoc} */
    public IProvider getProvider() {
        return this;
    }

    /** {@inheritDoc} */
    public String getPublishedName() {
        return publishedName;
    }

    /** {@inheritDoc} */
    public void setPublishedName(String name) {
        publishedName = name;
    }

    /**
     * Start this server-side stream
     */
    public void start() {
        if (state.get() != StreamState.UNINIT) {
            throw new IllegalStateException("State " + state + " not valid to start");
        }
        if (this.item.getItemSize() == 0) {
            throw new IllegalStateException("At least one item should be specified to start");
        }
        if (publishedName == null) {
            throw new IllegalStateException("A published name is needed to start");
        }
        try {
            IScope scope = getScope();
            IContext context = scope.getContext();
            providerService = (IProviderService) context.getBean(IProviderService.BEAN_NAME);
            // publish this server-side stream
            providerService.registerBroadcastStream(scope, publishedName, this);
            scheduler = (ISchedulingService) context.getBean(ISchedulingService.BEAN_NAME);
        } catch (NullPointerException npe) {
            log.warn("Context beans were not available; this is ok during unit testing", npe);
        }
        setState(StreamState.STOPPED);
        this.item.setCurrentItemIndex(-1);
        nextItem();
    }

    /**
     * Stop this server-side stream
     */
    public void stop() {
        if (PLAYING_OR_PAUSED.contains(state.get())) {
            if (liveJobName != null) {
                scheduler.removeScheduledJob(liveJobName);
                liveJobName = null;
            }
            if (vodJobName != null) {
                scheduler.removeScheduledJob(vodJobName);
                vodJobName = null;
            }
            if (msgIn != null) {
                msgIn.unsubscribe(this);
                msgIn = null;
            }
            if (nextRTMPMessage != null) {
                nextRTMPMessage.getBody().release();
            }
            stopRecording();
            setState(StreamState.STOPPED);
        }
    }

    /**
     * Stops any currently active recording.
     */
    public void stopRecording() {
        IRecordingListener listener = null;
        if (recordingListener != null && (listener = recordingListener.get()).isRecording()) {
            notifyRecordingStop();
            // remove the listener
            removeStreamListener(listener);
            // stop the recording listener
            listener.stop();
            // clear and null-out the thread local
            recordingListener.clear();
            recordingListener = null;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("incomplete-switch")
    public void pause() {
        switch (state.get()) {
            case PLAYING:
                setState(StreamState.PAUSED);
                break;
            case PAUSED:
                setState(StreamState.PLAYING);
                vodStartTS = 0;
                serverStartTS = System.currentTimeMillis();
                scheduleNextMessage();
        }
    }

    /** {@inheritDoc} */
    public void seek(int position) {
        // seek only allowed when playing or paused
        if (PLAYING_OR_PAUSED.contains(state.get())) {
            sendVODSeekCM(msgIn, position);
        }
    }

    /** {@inheritDoc} */
    public void close() {
        if (PLAYING_OR_PAUSED.contains(state.get())) {
            stop();
        }
        if (msgOut != null) {
            msgOut.unsubscribe(this);
        }
        notifyBroadcastClose();
        setState(StreamState.CLOSED);
    }

    /** {@inheritDoc} */
    public void onOOBControlMessage(IMessageComponent source, IPipe pipe, OOBControlMessage oobCtrlMsg) {
    }

    /** {@inheritDoc} */
    public void pushMessage(IPipe pipe, IMessage message) throws IOException {
        pushMessage(message);
    }

    /**
     * Pipe connection event handler. There are two types of pipe connection events so far, provider push connection event and provider disconnection event.
     *
     * Pipe events handling is the most common way of working with pipes.
     *
     * @param event
     *            Pipe connection event context
     */
    public void onPipeConnectionEvent(PipeConnectionEvent event) {
        switch (event.getType()) {
            case PROVIDER_CONNECT_PUSH:
                if (event.getProvider() == this && (event.getParamMap() == null || !event.getParamMap().containsKey("record"))) {
                    this.msgOut = (IMessageOutput) event.getSource();
                }
                break;
            case PROVIDER_DISCONNECT:
                if (this.msgOut == event.getSource()) {
                    this.msgOut = null;
                }
                break;
            default:
                break;
        }
    }

    /**
     * Play a specific IPlayItem. The strategy for now is VOD first, Live second.
     *
     * @param item
     *            Item to play
     */
    protected void play(IPlayItem item) {
        // dont play unless we are stopped
        if (state.get() == StreamState.STOPPED) {
            // assume this is not live stream
            boolean isLive = false;
            if (providerService != null) {
                msgIn = providerService.getVODProviderInput(getScope(), item.getName());
                if (msgIn == null) {
                    msgIn = providerService.getLiveProviderInput(getScope(), item.getName(), true);
                    isLive = true;
                }
                if (msgIn == null) {
                    log.warn("ABNORMAL Can't get both VOD and Live input from providerService");
                    return;
                }
            }
            setState(StreamState.PLAYING);
            this.item.setCurrentItem(item);
            sendResetMessage();
            if (msgIn != null) {
                msgIn.subscribe(this, null);
            }
            if (isLive) {
                if (item.getLength() >= 0) {
                    liveJobName = scheduler.addScheduledOnceJob(item.getLength(), new IScheduledJob() {
                        public void execute(ISchedulingService service) {
                            if (liveJobName == null) {
                                return;
                            }
                            liveJobName = null;
                            onItemEnd();
                        }
                    });
                }
            } else {
                long start = item.getStart();
                if (start < 0) {
                    start = 0;
                }
                sendVODInitCM(msgIn, (int) start);
                startBroadcastVOD();
            }
        }
    }

    /**
     * Play next item on item end
     */
    protected void onItemEnd() {
        nextItem();
    }

    /**
     * Push message
     *
     * @param message
     *            Message
     */
    private void pushMessage(IMessage message) throws IOException {
        this.message.pushMessage(message, msgOut, listeners, this, log);
    }

    /**
     * Send reset message
     */
    private void sendResetMessage() {
        this.message.sendResetMessage(msgOut, listeners, this, log);
    }

    /**
     * Begin VOD broadcasting
     */
    protected void startBroadcastVOD() {
        nextRTMPMessage = null;
        vodStartTS = 0;
        serverStartTS = System.currentTimeMillis();
        IStreamAwareScopeHandler handler = getStreamAwareHandler();
        if (handler != null) {
            if (recordingListener != null && recordingListener.get().isRecording()) {
                // callback for record start
                handler.streamRecordStart(this);
            } else {
                // callback for publish start
                handler.streamPublishStart(this);
            }
        }
        notifyBroadcastStart();
        scheduleNextMessage();
    }

    /**
     * Notifies handler on stream broadcast stop
     */
    protected void notifyBroadcastClose() {
        IStreamAwareScopeHandler handler = getStreamAwareHandler();
        if (handler != null) {
            try {
                handler.streamBroadcastClose(this);
            } catch (Throwable t) {
                log.error("error notify streamBroadcastStop", t);
            }
        }
    }

    /**
     * Notifies handler on stream recording stop
     */
    private void notifyRecordingStop() {
        IStreamAwareScopeHandler handler = getStreamAwareHandler();
        if (handler != null) {
            try {
                handler.streamRecordStop(this);
            } catch (Throwable t) {
                log.error("Error in notifyBroadcastClose", t);
            }
        }
    }

    /**
     * Notifies handler on stream broadcast start
     */
    protected void notifyBroadcastStart() {
        IStreamAwareScopeHandler handler = getStreamAwareHandler();
        if (handler != null) {
            try {
                handler.streamBroadcastStart(this);
            } catch (Throwable t) {
                log.error("error notify streamBroadcastStart", t);
            }
        }
    }

    /**
     * Pull the next message from IMessageInput and schedule it for push according to the timestamp.
     */
    protected void scheduleNextMessage() {
        boolean first = (nextRTMPMessage == null);
        long delta = 0L;
        do {
            nextRTMPMessage = getNextRTMPMessage();
            if (nextRTMPMessage != null) {
                IRTMPEvent rtmpEvent = nextRTMPMessage.getBody();
                // filter all non-AV messages
                if (rtmpEvent instanceof VideoData || rtmpEvent instanceof AudioData) {
                    rtmpEvent = nextRTMPMessage.getBody();
                    nextTS = rtmpEvent.getTimestamp();
                    if (first) {
                        vodStartTS = nextTS;
                        first = false;
                    }
                    delta = nextTS - vodStartTS - (System.currentTimeMillis() - serverStartTS);
                    if (delta < WAIT_THRESHOLD) {
                        if (doPushMessage()) {
                            if (state.get() != StreamState.PLAYING) {
                                // Stream is not playing, don't load more messages
                                nextRTMPMessage = null;
                            }
                        } else {
                            nextRTMPMessage = null;
                        }
                    }
                }
            } else {
                onItemEnd();
            }
        } while (nextRTMPMessage != null || delta < WAIT_THRESHOLD);
        // start the job all over again
        vodJobName = scheduler.addScheduledOnceJob(delta, new IScheduledJob() {
            public void execute(ISchedulingService service) {
                if (vodJobName != null) {
                    vodJobName = null;
                    if (doPushMessage()) {
                        if (state.get() == StreamState.PLAYING) {
                            scheduleNextMessage();
                        } else {
                            // Stream is paused, don't load more messages
                            nextRTMPMessage = null;
                        }
                    }
                }
            }
        });
    }

    private boolean doPushMessage() {
        boolean sent = false;
        IPlayItem curItem = this.item.getCurrentItem();
        long start = curItem.getStart();
        if (start < 0) {
            start = 0;
        }
        if (curItem.getLength() >= 0 && nextTS - start > curItem.getLength()) {
            onItemEnd();
            return sent;
        }
        if (nextRTMPMessage != null) {
            sent = true;
            try {
                pushMessage(nextRTMPMessage);
            } catch (IOException err) {
                log.error("Error while sending message.", err);
            }
            nextRTMPMessage.getBody().release();
        }
        return sent;
    }

    /**
     * Getter for next RTMP message.
     *
     * @return Next RTMP message
     */
    protected RTMPMessage getNextRTMPMessage() {
        return this.message.getNextRTMPMessage(msgIn, log);
    }

    /**
     * Send VOD initialization control message
     *
     * @param msgIn
     *            Message input
     * @param start
     *            Start timestamp
     */
    private void sendVODInitCM(IMessageInput msgIn, int start) {
        if (msgIn != null) {
            // Create new out-of-band control message
            OOBControlMessage oobCtrlMsg = new OOBControlMessage();
            // Set passive type
            oobCtrlMsg.setTarget(IPassive.KEY);
            // Set service name of init
            oobCtrlMsg.setServiceName("init");
            // Create map for parameters
            Map<String, Object> paramMap = new HashMap<>(1);
            // Put start timestamp into Map of params
            paramMap.put("startTS", start);
            // Attach to OOB control message and send it
            oobCtrlMsg.setServiceParamMap(paramMap);
            msgIn.sendOOBControlMessage(this, oobCtrlMsg);
        }
    }

    /**
     * Send VOD seek control message
     *
     * @param msgIn
     *            Message input
     * @param position
     *            New timestamp to play from
     */
    private void sendVODSeekCM(IMessageInput msgIn, int position) {
        OOBControlMessage oobCtrlMsg = new OOBControlMessage();
        oobCtrlMsg.setTarget(ISeekableProvider.KEY);
        oobCtrlMsg.setServiceName("seek");
        Map<String, Object> paramMap = new HashMap<>(1);
        paramMap.put("position", Integer.valueOf(position));
        oobCtrlMsg.setServiceParamMap(paramMap);
        msgIn.sendOOBControlMessage(this, oobCtrlMsg);
        // Reset properties
        vodStartTS = 0;
        serverStartTS = System.currentTimeMillis();
        if (nextRTMPMessage != null) {
            try {
                pushMessage(nextRTMPMessage);
            } catch (IOException err) {
                log.error("Error while sending message.", err);
            }
            nextRTMPMessage.getBody().release();
            nextRTMPMessage = null;
        }
        ResetMessage reset = new ResetMessage();
        try {
            pushMessage(reset);
        } catch (IOException err) {
            log.error("Error while sending message.", err);
        }
        scheduleNextMessage();
    }

    /**
     * Move to the next item updating the currentItemIndex.
     */
    protected void moveToNext() {
        int index = this.item.getCurrentItemIndex();
        int size = this.item.getItemSize();
        if (index >= size) {
            this.item.setCurrentItemIndex(size - 1);
        }
        if (controller != null) {
            this.item.setCurrentItemIndex(controller.nextItem(this, index));
        } else {
            this.item.setCurrentItemIndex(defaultController.nextItem(this, index));
        }
    }

    /**
     * Move to the previous item updating the currentItemIndex.
     */
    protected void moveToPrevious() {
        int index = this.item.getCurrentItemIndex();
        int size = this.item.getItemSize();
        if (index >= size) {
            this.item.setCurrentItemIndex(size - 1);
        }
        if (controller != null) {
            this.item.setCurrentItemIndex(controller.previousItem(this, index));
        } else {
            this.item.setCurrentItemIndex(defaultController.previousItem(this, index));
        }
    }

    public void addStreamListener(IStreamListener listener) {
        listeners.add(listener);
    }

    public Collection<IStreamListener> getStreamListeners() {
        return listeners;
    }

    public void removeStreamListener(IStreamListener listener) {
        listeners.remove(listener);
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ServerStream [publishedName=" + publishedName + ", controller=" + controller + ", defaultController=" + defaultController + ", isRewind=" + isRewind + ", isRandom=" + isRandom + ", isRepeat=" + isRepeat + ", items=" + this.item.getItems() + ", currentItemIndex=" + this.item.getCurrentItemIndex() + ", currentItem=" + this.item.getCurrentItem() + ", providerService=" + providerService + ", scheduler="
                + scheduler + ", liveJobName=" + liveJobName + ", vodJobName=" + vodJobName + ", vodStartTS=" + vodStartTS + ", serverStartTS=" + serverStartTS + ", nextTS=" + nextTS + "]";
    }

}
