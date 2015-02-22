/*
 * Copyright 2015 Shelf Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shelf.messagepack;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.handler.codec.TooLongFrameException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author t_yano
 */
public class MessagePackFrameDecoder extends ReplayingDecoder<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessagePackFrameDecoder.class);

    private static final int NIL = 0xc0 & 0xff;
    private static final int FALSE = 0xc2 & 0xff;
    private static final int TRUE = 0xc3 & 0xff;
    private static final int BIN8 = 0xc4 & 0xff;
    private static final int BIN16 = 0xc5 & 0xff;
    private static final int BIN32 = 0xc6 & 0xff;
    private static final int EXT8 = 0xc7 & 0xff;
    private static final int EXT16 = 0xc8 & 0xff;
    private static final int EXT32 = 0xc9 & 0xff;
    private static final int FLOAT32 = 0xca & 0xff;
    private static final int FLOAT64 = 0xcb & 0xff;
    private static final int UINT8 = 0xcc & 0xff;
    private static final int UINT16 = 0xcd & 0xff;
    private static final int UINT32 = 0xce & 0xff;
    private static final int UINT64 = 0xcf & 0xff;
    private static final int INT8 = 0xd0 & 0xff;
    private static final int INT16 = 0xd1 & 0xff;
    private static final int INT32 = 0xd2 & 0xff;
    private static final int INT64 = 0xd3 & 0xff;
    private static final int FIXEXT1 = 0xd4 & 0xff;
    private static final int FIXEXT2 = 0xd5 & 0xff;
    private static final int FIXEXT4 = 0xd6 & 0xff;
    private static final int FIXEXT8 = 0xd7 & 0xff;
    private static final int FIXEXT16 = 0xd8 & 0xff;
    private static final int STR8 = 0xd9 & 0xff;
    private static final int STR16 = 0xda & 0xff;
    private static final int STR32 = 0xdb & 0xff;
    private static final int ARRAY16 = 0xdc & 0xff;
    private static final int ARRAY32 = 0xdd & 0xff;
    private static final int MAP16 = 0xde & 0xff;
    private static final int MAP32 = 0xdf & 0xff;

    private final int maxFrameLength;
    private boolean discardingTooLongFrame;
    private long tooLongFrameLength;
    private long bytesToDiscard;
    private final boolean failFast;

    public MessagePackFrameDecoder(int maxFrameLength, boolean failFast) {
        this.maxFrameLength = maxFrameLength;
        this.failFast = failFast;
    }

    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        ByteBuf frame = ctx.alloc().buffer(length);
        frame.writeBytes(buffer, index, length);
        return frame;
    }

    private void fail(long frameLength) {
        if (frameLength > 0) {
            throw new TooLongFrameException(
                            "Adjusted frame length exceeds " + maxFrameLength +
                            ": " + frameLength + " - discarded");
        } else {
            throw new TooLongFrameException(
                            "Adjusted frame length exceeds " + maxFrameLength +
                            " - discarding");
        }
    }

    private void failIfNecessary(boolean firstDetectionOfTooLongFrame) {
        if (bytesToDiscard == 0) {
            // Reset to the initial state and tell the handlers that
            // the frame was too large.
            long tooLongFrameLength = this.tooLongFrameLength;
            this.tooLongFrameLength = 0;
            discardingTooLongFrame = false;
            if (!failFast ||
                failFast && firstDetectionOfTooLongFrame) {
                fail(tooLongFrameLength);
            }
        } else {
            // Keep discarding and notify handlers if necessary.
            if (failFast && firstDetectionOfTooLongFrame) {
                fail(tooLongFrameLength);
            }
        }
    }

    protected int getArraySize(ChannelHandlerContext ctx, ByteBuf in, int headerLength, int offset, long arraySize) throws Exception {
        long currentLength = 0;
        for(long i = 0; i < arraySize; i++) {
            currentLength += decodeLength(ctx, in, offset);
        }
        if(currentLength > Integer.MAX_VALUE) {
            throw new CorruptedFrameException("Length of Array or Map is too long (exceeded Integer.MAX_VALUE. Can not continue calculation of length.");
        }
        return (int)currentLength;
    }

    protected long decodeLength(ChannelHandlerContext ctx, ByteBuf in, int offset) throws Exception {
        if (discardingTooLongFrame) {
            long bytesToDiscard = this.bytesToDiscard;
            int localBytesToDiscard = (int) Math.min(bytesToDiscard, in.readableBytes());
            in.skipBytes(localBytesToDiscard);
            bytesToDiscard -= localBytesToDiscard;
            this.bytesToDiscard = bytesToDiscard;

            failIfNecessary(false);
        }

        int readerIndex = in.readerIndex() + offset;
        short b = in.getUnsignedByte(in.readerIndex());
        int ubyte = b & 0xff;

        LOGGER.trace("message: " + toHex(ubyte));

        switch (ubyte) {
        case NIL:
            return 1L;
        case FALSE:
            return 1L;
        case TRUE:
            return 1L;
        case BIN8: {
            short length = in.getUnsignedByte(readerIndex + 1);
            return 2L + length;
        }
        case BIN16: {
            int length = in.getUnsignedShort(readerIndex + 1);
            return 3L + length;
        }
        case BIN32: {
            long length = in.getUnsignedInt(readerIndex + 1);
            return 5L + length;
        }
        case EXT8: {
            short length = in.getUnsignedByte(readerIndex + 1);
            return 3L + length;
        }
        case EXT16: {
            int length = in.getUnsignedShort(readerIndex + 1);
            return 4L + length;
        }
        case EXT32: {
            long length = in.getUnsignedInt(readerIndex + 1);
            return 6L + length;
        }

        case FLOAT32:
            return 5L;
        case FLOAT64:
            return 9L;
        case UINT8:
            return 2L;
        case UINT16:
            return 3L;
        case UINT32:
            return 5L;
        case UINT64:
            return 9L;
        case INT8:
            return 2L;
        case INT16:
            return 3L;
        case INT32:
            return 5L;
        case INT64:
            return 9L;
        case FIXEXT1:
            return 3L;
        case FIXEXT2:
            return 4L;
        case FIXEXT4:
            return 6L;
        case FIXEXT8:
            return 10L;
        case FIXEXT16:
            return 18L;
        case STR8: {
            short length = in.getUnsignedByte(readerIndex + 1);
            return 2L + length;
        }
        case STR16: {
            int length = in.getUnsignedShort(readerIndex + 1);
            return 3L + length;
        }
        case STR32: {
            long length = in.getUnsignedInt(readerIndex + 1);
            return 5L + length;
        }
        case ARRAY16: {
            int elemCount = in.getUnsignedShort(readerIndex + 1);
            return getArraySize(ctx, in, 3, offset, elemCount);
        }
        case ARRAY32: {
            long elemCount = in.getUnsignedInt(readerIndex + 1);
            return getArraySize(ctx, in, 5, offset, elemCount);
        }
        case MAP16: {
            int elemCount = in.getUnsignedShort(readerIndex + 1);
            return getArraySize(ctx, in, 3, offset, elemCount * 2);
        }
        case MAP32: {
            long elemCount = in.getUnsignedInt(readerIndex + 1);
            return getArraySize(ctx, in, 5, offset, elemCount * 2);
        }
        default:
            if ((ubyte >> 7) == 0) { //positive fixint
                return 1L;
            } else if ((ubyte >> 4) == 0b1000) { //fixmap
                int elemCount = ubyte & 0b00001111;
                return getArraySize(ctx, in, 1, offset, elemCount * 2);
            } else if ((ubyte >> 4) == 0b1001) { //fixarray
                int elemCount = ubyte & 0b00001111;
                return getArraySize(ctx, in, 1, offset, elemCount);
            } else if ((ubyte >> 5) == 0b101) { //fixstr
                int length = ubyte & 0b00011111;
                return 1L + length;
            } else if ((ubyte >> 5) == 0b111) { //negative fixint
                return 1L;
            } else {
                throw new CorruptedFrameException("Unknown header byte of message: " + toHex(ubyte));
            }
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        long frameLength = decodeLength(ctx, in, 0);
        if(frameLength > maxFrameLength) {
            long discard = frameLength - in.readableBytes();
            tooLongFrameLength = frameLength;

            if (discard < 0) {
                // buffer contains more bytes then the frameLength so we can discard all now
                in.skipBytes((int) frameLength);
            } else {
                // Enter the discard mode and discard everything received so far.
                discardingTooLongFrame = true;
                bytesToDiscard = discard;
                in.skipBytes(in.readableBytes());
            }
            failIfNecessary(true);
        } else {
            int readerIndex = in.readerIndex();
            int actualFrameLength = (int)frameLength;
            ByteBuf buffer = extractFrame(ctx, in, readerIndex, actualFrameLength);
            in.readerIndex(readerIndex + actualFrameLength);
            out.add(buffer);
        }
    }

    private String toHex(int b) {
        StringBuilder sb = new StringBuilder();
        sb.append(Character.forDigit(b >> 4 & 0xF, 16));
        sb.append(Character.forDigit(b & 0xF, 16));
        return sb.toString();
    }

}
