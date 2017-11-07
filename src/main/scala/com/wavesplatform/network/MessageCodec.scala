package com.wavesplatform.network

import java.util

import com.wavesplatform.metrics.Metrics
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageCodec
import org.influxdb.dto.Point
import scorex.network.message._
import scorex.utils.ScorexLogging

import scala.util.{Failure, Success}

@Sharable
class MessageCodec(peerDatabase: PeerDatabase) extends MessageToMessageCodec[RawBytes, Message] with ScorexLogging {

  private val specs: Map[Byte, MessageSpec[_ <: AnyRef]] = BasicMessagesRepo.specs.map(s => s.messageCode -> s).toMap

  private val tryEncode: Message => Option[RawBytes] = {
    val f: PartialFunction[Message, RawBytes] = {
      case LocalScoreChanged(score) => RawBytes(ScoreMessageSpec.messageCode, ScoreMessageSpec.serializeData(score))
      case GetPeers => RawBytes(GetPeersSpec.messageCode, Array[Byte]())
      case k: KnownPeers => RawBytes(PeersSpec.messageCode, PeersSpec.serializeData(k))
      case gs: GetSignatures => RawBytes(GetSignaturesSpec.messageCode, GetSignaturesSpec.serializeData(gs))
      case s: Signatures => RawBytes(SignaturesSpec.messageCode, SignaturesSpec.serializeData(s))
      case g: GetBlock => RawBytes(GetBlockSpec.messageCode, GetBlockSpec.serializeData(g))
      case BlockForged(b) => RawBytes(BlockMessageSpec.messageCode, b.bytes())
      case m: MicroBlockInv => RawBytes(MicroBlockInvMessageSpec.messageCode, MicroBlockInvMessageSpec.serializeData(m))
      case m: MicroBlockRequest => RawBytes(MicroBlockRequestMessageSpec.messageCode, MicroBlockRequestMessageSpec.serializeData(m))
      case m: MicroBlockResponse => RawBytes(MicroBlockResponseMessageSpec.messageCode, MicroBlockResponseMessageSpec.serializeData(m))
      case r: RawBytes => r
    }
    f.lift
  }

  override def encode(ctx: ChannelHandlerContext, msg: Message, out: util.List[AnyRef]): Unit = {
    tryEncode(msg).foreach { raw =>
      val spec = specs(raw.code)
      Metrics.write(Point.measurement("message")
        .tag("dir", "outgoing")
        .tag("type", spec.messageName)
        .addField("type", spec.messageName)
        .addField("bytes", raw.data.length))
      out.add(raw)
    }
  }

  override def decode(ctx: ChannelHandlerContext, msg: RawBytes, out: util.List[AnyRef]): Unit = {
    val spec = specs(msg.code)
    spec.deserializeData(msg.data) match {
      case Success(x) =>
        Metrics.write(Point.measurement("message")
          .tag("dir", "incoming")
          .tag("type", spec.messageName)
          .addField("type", spec.messageName)
          .addField("bytes", msg.data.length))
        out.add(x)

      case Failure(e) => block(ctx, e)
    }
  }

  protected def block(ctx: ChannelHandlerContext, e: Throwable): Unit = {
    peerDatabase.blacklistAndClose(ctx.channel(), s"Invalid message. ${e.getMessage}")
  }

}
