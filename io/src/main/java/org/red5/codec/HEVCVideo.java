/*
 * RED5 Open Source Media Server - https://github.com/Red5/ Copyright 2006-2023 by respective authors (see below). All rights reserved. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless
 * required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.red5.codec;

import org.apache.mina.core.buffer.IoBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Red5 video codec for the HEVC (h265) video format. Stores DecoderConfigurationRecord and last keyframe.
 *
 * @author Paul Gregoire (mondain@gmail.com)
 */
public class HEVCVideo extends AbstractCodecVideo {

    private static Logger log = LoggerFactory.getLogger(HEVCVideo.class);

    private static boolean isDebug = log.isDebugEnabled();

    /** Constructs a new HEVCVideo. */
    public HEVCVideo() {
        super(VideoCodec.HEVC);
    }

}
