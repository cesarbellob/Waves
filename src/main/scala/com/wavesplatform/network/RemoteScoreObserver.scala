package com.wavesplatform.network

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import com.wavesplatform.state2.ByteStr
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._
import monix.eval.Task
import monix.execution.Scheduler
import scorex.transaction.History
import scorex.utils.ScorexLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, blocking}
import scala.util.{Failure, Success}

@Sharable
class RemoteScoreObserver(scoreTtl: FiniteDuration, lastSignatures: => Seq[ByteStr], initialLocalScore: BigInt)
  extends ChannelDuplexHandler with ScorexLogging {

  private val scheduler = Scheduler.singleThread("remote-score-observer")

  private type ScorePair = (Channel, BigInt)

  private val scores = new ConcurrentHashMap[Channel, BigInt]

  @volatile private var localScore = initialLocalScore
  private var currentRequest = Option.empty[ScorePair]

  private def channelWithHighestScore: Option[ScorePair] = {
    Option(scores.reduceEntries(1000, (c1, c2) => if (c1.getValue > c2.getValue) c1 else c2))
      .map(e => e.getKey -> e.getValue)
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    ctx.channel().closeFuture().addListener { channelFuture: ChannelFuture =>
      val closedChannel = channelFuture.channel()
      Option(scores.remove(closedChannel)).foreach { removedScore =>
        log.debug(s"${id(ctx)} Closed, removing score $removedScore")
      }

      Task {
        if (currentRequest.exists(_._1 == ctx.channel())) {
          log.debug("Switching to second best channel, because the best one was closed")
          currentRequest = channelWithHighestScore
          currentRequest.foreach {
            case (c, s) if s > localScore => requestExtension(c)
            case _ => currentRequest = None
          }
        }
      }.runAsync(scheduler)
    }
  }

  override def write(ctx: ChannelHandlerContext, msg: AnyRef, promise: ChannelPromise): Unit = msg match {
    case LocalScoreChanged(newLocalScore, reason) =>
      localScore = newLocalScore
      if (reason == LocalScoreChanged.Reason.ForkApplied) {
        Task {
          if (currentRequest.exists(_._1 == ctx.channel())) {
            log.debug("Fork applied")
            val candidate = channelWithHighestScore
            if (candidate.exists(_._1 == ctx.channel())) {
              currentRequest = None
            } else {
              currentRequest = candidate
              currentRequest.foreach {
                case (c, s) if s > localScore => requestExtension(c)
                case _ => currentRequest = None
              }
            }
          }
        }.runAsync(scheduler)
      } else if (reason == LocalScoreChanged.Reason.Rollback) {
        Task {
          if (currentRequest.exists(_._1 == ctx.channel())) {
            log.debug("Rollback")
            currentRequest = channelWithHighestScore
            currentRequest.foreach {
              case (c, s) if s > localScore => requestExtension(c)
              case _ => currentRequest = None
            }
          }
        }.runAsync(scheduler)
      }
      super.write(ctx, msg, promise)

    case _ => super.write(ctx, msg, promise)
  }

  private def requestExtension(channel: Channel): Unit = Future(blocking(lastSignatures)).onComplete {
    case Success(sig) => channel.writeAndFlush(LoadBlockchainExtension(sig))
    case Failure(e) => log.warn("Error getting last signatures", e)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = msg match {
    case newScore: History.BlockchainScore =>
      val diff = newScore - Option(scores.put(ctx.channel(), newScore)).getOrElse(BigInt(0))
      if (diff != 0) {
        scheduleExpiration(ctx, newScore)
        log.trace(s"${id(ctx)} New score: $newScore (diff: $diff)")
        Task {
          if (currentRequest.isEmpty) {
            log.debug("New score")
            currentRequest = channelWithHighestScore
            currentRequest.foreach {
              case (c, s) if s > localScore => requestExtension(c)
              case _ => currentRequest = None
            }
          }
        }.runAsync(scheduler)
      }

    case ExtensionBlocks(blocks) =>
      Task {
        val isExpectedResponse = currentRequest.exists(_._1 == ctx.channel())
        if (!isExpectedResponse) {
          log.debug(s"${id(ctx)} Received blocks ${formatBlocks(blocks)} from non-pinned channel (could be from expired channel)")
        } else if (blocks.isEmpty) {
          log.debug(s"${id(ctx)} Blockchain is up to date with the remote node")
          currentRequest = channelWithHighestScore
          currentRequest.foreach {
            case (c, s) if s > localScore => requestExtension(c)
            case _ => currentRequest = None
          }
        } else {
          log.debug(s"${id(ctx)} Received extension blocks ${formatBlocks(blocks)}")
          super.channelRead(ctx, msg)
        }
      }.runAsync(scheduler)

    case _ => super.channelRead(ctx, msg)
  }

  private def scheduleExpiration(ctx: ChannelHandlerContext, score: BigInt): Unit = {
    Task {
      if (scores.remove(ctx.channel(), score)) {
        log.debug("Score expired")
        if (currentRequest.exists(_._1 == ctx.channel())) {
          currentRequest = channelWithHighestScore
          currentRequest.foreach {
            case (c, s) if s > localScore => requestExtension(c)
            case _ => currentRequest = None
          }
        }
      }
    }.delayExecution(scoreTtl).runAsync(scheduler)
  }

//  private def trySwitchToBestFrom(initiatorCtx: ChannelHandlerContext, reason: String): Unit = {
//    trySwitchToBestIf(initiatorCtx, reason)(_.exists(_._1 == initiatorCtx.channel()))
//  }
//
//  // what if best == prev?
//  private def trySwitchToBestIf(initiatorCtx: ChannelHandlerContext, reason: String)
//                               (shouldTry: Option[ScorePair] => Boolean): Unit = {
//    switchChannel { (prev: Option[ScorePair], best: Option[ScorePair]) =>
//      best.flatMap { case (_, bestRemoteScore) =>
//        if (shouldTry(prev)) {
//          if (bestRemoteScore > localScore) best else None
//        } else prev
//      }
//    }.foreach { case (prev, (bestRemoteChannel, bestRemoteScore)) =>
//      log.debug(
//        s"${id(initiatorCtx)} A new pinned channel ${id(bestRemoteChannel)} has score $bestRemoteScore " +
//          s"(diff with local: ${bestRemoteScore - localScore}), prev = $prev: requesting an extension. Reason: $reason"
//      )
//      requestExtension(bestRemoteChannel)
//    }
//  }

//  private def switchChannel(f: (Option[ScorePair], Option[ScorePair]) => Option[ScorePair]) = {
//    val newValue = channelWithHighestScore
//    var curr, next = Option.empty[ScorePair]
//
//    do {
//      curr = currentRequest.get()
//      next = f(curr, newValue)
//    } while (!currentRequest.compareAndSet(curr, next))
//
//    if (next == curr) None else next.map { x => (curr, x) }
//  }

}
