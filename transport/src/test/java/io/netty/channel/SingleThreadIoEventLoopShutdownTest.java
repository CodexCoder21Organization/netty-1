/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.jupiter.api.Test;

import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SingleThreadIoEventLoopShutdownTest {
    @Test
    void shutdownStartedInTaskClosesRegisteredChannel() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try (SocketChannel socket = SocketChannel.open()) {
            NioSocketChannel root = new NioSocketChannel(socket);
            try {
                ChannelFuture registered = group.register(root);
                assertTrue(registered.await(5, TimeUnit.SECONDS) && registered.isSuccess());
                assertTrue(root.eventLoop().submit(() -> group.shutdownGracefully(0, 5, TimeUnit.SECONDS))
                        .await(5, TimeUnit.SECONDS));
                assertTrue(group.terminationFuture().await(10, TimeUnit.SECONDS));
                assertFalse(root.isOpen(), "Termination must close a channel registered before shutdown");
                assertTrue(root.closeFuture().isDone());
            } finally {
                assertTrue(group.shutdownGracefully(0, 5, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void shutdownHookRegistrationIsClosedBeforeTermination() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try (SocketChannel socket = SocketChannel.open()) {
            NioSocketChannel root = new NioSocketChannel(socket);
            SingleThreadEventLoop loop = (SingleThreadEventLoop) group.next();
            CountDownLatch registered = new CountDownLatch(1);
            try {
                assertTrue(loop.submit(() -> {
                    loop.addShutdownHook(() -> loop.register(root).addListener(future -> registered.countDown()));
                    group.shutdownGracefully(0, 5, TimeUnit.SECONDS);
                }).await(5, TimeUnit.SECONDS));
                assertTrue(group.terminationFuture().await(10, TimeUnit.SECONDS));
                assertTrue(registered.await(5, TimeUnit.SECONDS));
                assertFalse(root.isOpen(), "Termination must close a channel registered by a shutdown hook");
                assertTrue(root.closeFuture().isDone());
            } finally {
                assertTrue(group.shutdownGracefully(0, 5, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void closeListenerCannotLeaveNewRegistrationAfterFinalCleanup() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try (SocketChannel firstSocket = SocketChannel.open(); SocketChannel nextSocket = SocketChannel.open()) {
            NioSocketChannel first = new NioSocketChannel(firstSocket);
            NioSocketChannel next = new NioSocketChannel(nextSocket);
            AtomicReference<ChannelFuture> registration = new AtomicReference<>();
            try {
                ChannelFuture registered = group.register(first);
                assertTrue(registered.await(5, TimeUnit.SECONDS) && registered.isSuccess());
                first.closeFuture().addListener(future -> registration.set(group.register(next)));
                assertTrue(first.eventLoop().submit(() -> group.shutdownGracefully(0, 5, TimeUnit.SECONDS))
                        .await(5, TimeUnit.SECONDS));
                assertTrue(group.terminationFuture().await(10, TimeUnit.SECONDS));
                assertFalse(first.isOpen());
                assertTrue(registration.get() != null, "Final cleanup must invoke the close listener");
                assertTrue(registration.get().isDone());
                assertFalse(registration.get().isSuccess(), "A fully shut down loop must refuse new IO registration");
                assertInstanceOf(RejectedExecutionException.class, registration.get().cause());
                assertEquals("Cannot register IO handle " + next.unsafe()
                        + " because the event loop has shut down", registration.get().cause().getMessage());
                assertFalse(next.isOpen(), "The refused channel must be closed by registration failure cleanup");
                assertTrue(next.closeFuture().isDone());
            } finally {
                assertTrue(group.shutdownGracefully(0, 5, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS));
            }
        }
    }
}
