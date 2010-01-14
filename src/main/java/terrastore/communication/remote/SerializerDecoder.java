package terrastore.communication.remote;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import terrastore.communication.remote.serialization.Serializer;

/**
 * @param <T>
 * @author Sergio Bossa
 */
@ChannelPipelineCoverage("all")
public class SerializerDecoder<T> extends OneToOneDecoder {

    private final Serializer<T> serializer;

    public SerializerDecoder(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        ChannelBuffer buffer = (ChannelBuffer) msg;
        return serializer.deserialize(new ChannelBufferInputStream(buffer));
    }
}
