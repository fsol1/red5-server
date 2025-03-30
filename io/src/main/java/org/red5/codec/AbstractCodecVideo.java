package org.red5.codec;

import org.apache.mina.core.buffer.IoBuffer;
import org.slf4j.Logger;

public abstract class AbstractCodecVideo extends AbstractVideo {

    private static Logger log;

    private static boolean isDebug;

    /** Video decoder configuration data */
    private FrameData decoderConfiguration;

    public AbstractCodecVideo(VideoCodec c) {
        codec = c;
        this.reset();
    }

    /** {@inheritDoc} */
    @Override
    public boolean canDropFrames() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void reset() {
        decoderConfiguration = new FrameData();
        softReset();
    }

    // reset all except decoder configuration
    private void softReset() {
        keyframes.clear();
        interframes.clear();
        numInterframes.set(0);
    }

    /** {@inheritDoc} */
    @Override
    public boolean canHandleData(IoBuffer data) {
        boolean result = false;
        if (data.limit() > 0) {
            // read the first byte and ensure its codec type
            result = ((data.get() & 0x0f) == codec.getId());
            data.rewind();
        }
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public boolean addData(IoBuffer data) {
        return addData(data, (keyframeTimestamp + 1));
    }

    /** {@inheritDoc} */
    @Override
    public boolean addData(IoBuffer data, int timestamp) {
        //log.trace("addData timestamp: {} remaining: {}", timestamp, data.remaining());
        if (data.hasRemaining()) {
            // mark
            int start = data.position();
            // get frame type
            byte frameType = data.get();
            byte avcType = data.get();
            if ((frameType & 0x0f) == codec.getId()) {
                // check for keyframe
                if ((frameType & 0xf0) == FLV_FRAME_KEY) {
                    if (isDebug) {
                        log.debug("Keyframe - codec type: {}", avcType);
                    }
                    // rewind
                    data.rewind();
                    switch (avcType) {
                        case 1: // keyframe
                            //log.trace("Keyframe - keyframeTimestamp: {} {}", keyframeTimestamp, timestamp);
                            // get the time stamp and compare with the current value
                            if (timestamp != keyframeTimestamp) {
                                //log.trace("New keyframe");
                                // new keyframe
                                keyframeTimestamp = timestamp;
                                // if its a new keyframe, clear keyframe and interframe collections
                                softReset();
                            }
                            // store keyframe
                            keyframes.add(new FrameData(data));
                            break;
                        case 0: // configuration
                            if (isDebug) {
                                log.debug("Decoder configuration");
                            }

                            decoderConfiguration.setData(data);
                            softReset();
                            break;
                    }
                    //log.trace("Keyframes: {}", keyframes.size());
                } else if (bufferInterframes) {
                    //log.trace("Interframe");
                    if (isDebug) {
                        log.debug("Interframe - codec type: {}", avcType);
                    }
                    // rewind
                    data.rewind();
                    try {
                        int lastInterframe = numInterframes.getAndIncrement();
                        //log.trace("Buffering interframe #{}", lastInterframe);
                        if (lastInterframe < interframes.size()) {
                            interframes.get(lastInterframe).setData(data);
                        } else {
                            interframes.add(new FrameData(data));
                        }
                    } catch (Throwable e) {
                        log.warn("Failed to buffer interframe", e);
                    }
                    //log.trace("Interframes: {}", interframes.size());
                }
            } else {
                log.debug("Non-codec data, rejecting");
                // go back to where we started
                data.position(start);
                return false;
            }
            // go back to where we started
            data.position(start);
        }
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public IoBuffer getDecoderConfiguration() {
        return decoderConfiguration.getFrame();
    }
}
