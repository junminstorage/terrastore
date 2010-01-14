package terrastore.communication.remote;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import terrastore.communication.remote.serialization.Serializer;

/**
 * @author Sergio Bossa
 */
@ChannelPipelineCoverage("all")
public class SerializerEncoder<T> extends OneToOneEncoder {

    private final Serializer<T> serializer;

    public SerializerEncoder(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        return ChannelBuffers.wrappedBuffer(serializer.serialize((T) msg));
    }
}
