/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.communication.remote;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import terrastore.util.io.Serializer;

/**
 * @author Sergio Bossa
 */
public class SerializerDecoder extends FrameDecoder {

    private final Serializer serializer;

    public SerializerDecoder(Serializer serializer) {
        this.serializer = serializer;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        boolean hasLength = buffer.readableBytes() >= 4;
        if (hasLength) {
            buffer.markReaderIndex();
            int length = buffer.readInt();
            boolean hasContent = buffer.readableBytes() >= length;
            if (hasContent) {
                int afterReadIndex = buffer.readerIndex() + length;
                try {
                    Object result = serializer.deserialize(new ChannelBufferInputStream(buffer, length));
                    if (buffer.readerIndex() != afterReadIndex) {
                        throw new IllegalStateException("Errore while decoding data!");
                    }
                    return result;
                } catch (Exception ex) {
                    buffer.readerIndex(afterReadIndex);
                    throw new RuntimeException(ex.getMessage(), ex);
                }
            } else {
                buffer.resetReaderIndex();
                return null;
            }
        } else {
            return null;
        }
    }

}
