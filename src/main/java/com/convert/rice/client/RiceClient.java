package com.convert.rice.client;

import static com.google.common.collect.Iterables.transform;
import static org.jboss.netty.channel.Channels.pipeline;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.ChannelGroupFutureListener;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.handler.logging.LoggingHandler;

import com.convert.rice.client.protocol.MapReduce;
import com.convert.rice.client.protocol.MapReduce.GroupBy;
import com.convert.rice.client.protocol.MapReduce.MapFunction;
import com.convert.rice.client.protocol.MapReduce.ReduceFunction;
import com.convert.rice.client.protocol.Request;
import com.convert.rice.client.protocol.Request.Get;
import com.convert.rice.client.protocol.Request.Increment;
import com.convert.rice.client.protocol.Request.Increment.Metric;
import com.convert.rice.client.protocol.Response;
import com.convert.rice.client.protocol.Response.GetResult;
import com.convert.rice.client.protocol.Response.IncResult;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class RiceClient {

    private final GenericObjectPool<Channel> pool;

    private final ClientBootstrap bootstrap;

    private final ChannelGroup group = new DefaultChannelGroup();

    public RiceClient(final String host, final int port) {
        this(host, port, new GenericObjectPool.Config());
    }

    public RiceClient(final String host, final int port, GenericObjectPool.Config config) {
        bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Configure the event pipeline factory.
        bootstrap.setPipelineFactory(new RiceClientPipelineFactory());
        pool = new GenericObjectPool<Channel>(new PoolableObjectFactory<Channel>() {

            @Override
            public Channel makeObject() throws Exception {
                Channel ch = bootstrap.connect(new InetSocketAddress(host, port)).awaitUninterruptibly().getChannel();
                group.add(ch);
                return ch;
            }

            @Override
            public void destroyObject(Channel ch) throws Exception {
                group.remove(ch);
                ch.close().awaitUninterruptibly();

            }

            @Override
            public boolean validateObject(Channel ch) {
                return ch.isConnected() && ch.isOpen();
            }

            @Override
            public void activateObject(Channel ch) throws Exception {
            }

            @Override
            public void passivateObject(Channel ch) throws Exception {
            }
        }, config);
    }

    public ListenableFuture<Response> send(Request req)
            throws Exception {
        Channel ch = pool.borrowObject();

        RiceClientHandler handler = ch.getPipeline().get(RiceClientHandler.class);
        return handler.send(req);
    }

    public ListenableFuture<Void> close() throws Exception {
        pool.close();
        bootstrap.releaseExternalResources();
        final SettableFuture<Void> future = SettableFuture.<Void> create();
        group.close().addListener(new ChannelGroupFutureListener() {

            @Override
            public void operationComplete(ChannelGroupFuture f) throws Exception {
                future.set(null);
            }
        });

        return future;

    }

    private class RiceClientPipelineFactory implements ChannelPipelineFactory {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline p = pipeline();
            p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
            p.addLast("protobufDecoder", new ProtobufDecoder(Response.getDefaultInstance()));
            p.addLast("log", new LoggingHandler());

            p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
            p.addLast("protobufEncoder", new ProtobufEncoder());

            p.addLast("handler", new RiceClientHandler());
            return p;
        }
    }

    private class RiceClientHandler extends SimpleChannelUpstreamHandler {

        private final Logger logger = Logger.getLogger(
                this.getClass().getName());

        // Stateful properties
        private volatile Channel channel;

        public SettableFuture<Response> send(Request request) {
            SettableFuture<Response> future = SettableFuture.create();
            channel.getPipeline().getContext(this).setAttachment(future);
            channel.write(request);
            return future;

        }

        @Override
        public void handleUpstream(
                ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
            if (e instanceof ChannelStateEvent) {
                logger.fine(e.toString());
            }
            super.handleUpstream(ctx, e);
        }

        @Override
        public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
                throws Exception {
            channel = e.getChannel();
            super.channelOpen(ctx, e);
        }

        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            SettableFuture<Response> future = (SettableFuture<Response>) ctx.getAttachment();
            if (future != null && future.cancel(false)) {
                logger.log(Level.WARNING, "closing channel before task was complete");
            }
            super.channelClosed(ctx, e);
        }

        @Override
        public void messageReceived(
                ChannelHandlerContext ctx, final MessageEvent event) {
            try {
                Response result = (Response) event.getMessage();
                SettableFuture<Response> future = (SettableFuture<Response>) ctx.getAttachment();
                future.set(result);
            } finally {
                try {
                    pool.returnObject(event.getChannel());
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void exceptionCaught(
                ChannelHandlerContext ctx, ExceptionEvent event) throws Exception {
            logger.log(
                    Level.WARNING,
                    "Unexpected exception from downstream. " + event.getCause(), event.getCause());
            try {
                pool.invalidateObject(event.getChannel());
                SettableFuture<Response> future = (SettableFuture<Response>) ctx.getAttachment();
                if (future != null && future.cancel(false)) {
                    logger.log(Level.WARNING, "closing channel before task was complete");
                }
            } catch (Exception exception) {
                if (event.getChannel().isOpen()) {
                    event.getChannel().close();
                }
            }
            super.exceptionCaught(ctx, event);
        }
    }

    /**
     * A handy function to do a single get request with some aggregation function.
     * 
     * @param type
     * @param key
     * @param start
     * @param end
     * @param step
     * @return
     * @throws Exception
     */
    public ListenableFuture<GetResult> get(String type, String key, long start, long end, int step) throws Exception {
        GroupBy gropBy = GroupBy.newBuilder().setStep(step).build();
        MapReduce aggregation = MapReduce.newBuilder().setMapFunction(MapFunction.newBuilder().setGroupBy(gropBy))
                .setReduceFunction(ReduceFunction.SUM).build();
        Get get = Get
                .newBuilder()
                .setKey(key)
                .setType(type)
                .setStart(start)
                .setEnd(end)
                .addMapReduce(aggregation)
                .build();

        return Futures.transform(this.send(Request.newBuilder().addGet(get).build()),
                new Function<Response, GetResult>() {

                    @Override
                    public GetResult apply(Response input) {
                        return input.getGetResult(0);
                    }
                });

    }

    /**
     * A handy function to do a single increment
     * 
     * @param type
     * @param key
     * @param metrics
     * @param timestamp
     * @return
     * @throws Exception
     */
    public ListenableFuture<IncResult> inc(String type, String key, Map<String, Long> metrics, long timestamp)
            throws Exception {
        Function<Entry<String, Long>, Metric> f = new Function<Entry<String, Long>, Increment.Metric>() {

            @Override
            public Metric apply(Entry<String, Long> input) {
                return Metric.newBuilder().setKey(input.getKey()).setValue(input.getValue()).build();
            }
        };
        Increment inc = Increment.newBuilder().setType(type).setTimestamp(timestamp).setKey(key)
                .addAllMetrics(transform(metrics.entrySet(), f)).build();

        return Futures.transform(this.send(Request.newBuilder().addInc(inc).build()),
                new Function<Response, IncResult>() {

                    @Override
                    public IncResult apply(Response input) {
                        return input.getIncResult(0);
                    }
                });

    }
}
