package com.wavesplatform.matcher.market

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.wavesplatform.matcher.market.OrderHistoryActor.GetOrderHistory
import com.wavesplatform.matcher.{MatcherSettings, MatcherTestData}
import com.wavesplatform.settings.WalletSettings
import com.wavesplatform.state2.ByteStr
import com.wavesplatform.state2.reader.StateReader
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import scorex.transaction.assets.exchange.AssetPair
import scorex.utils.{NTP, ScorexLogging}
import scorex.wallet.Wallet

class OrderHistoryActorSpecification extends TestKit(ActorSystem("MatcherTest"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ImplicitSender
  with MatcherTestData
  with BeforeAndAfterEach
  with ScorexLogging
  with PathMockFactory {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val settings: MatcherSettings = matcherSettings.copy(account = MatcherAccount.address)
  val pair = AssetPair(Some(ByteStr("BTC".getBytes)), Some(ByteStr("WAVES".getBytes)))
  val storedState: StateReader = stub[StateReader]
  val wallet = Wallet(WalletSettings(None, "matcher", Some(WalletSeed)))
  wallet.generateNewAccount()

  var actor: ActorRef = system.actorOf(Props(new OrderHistoryActor(settings, storedState, wallet)))

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    actor = system.actorOf(Props(new OrderHistoryActor(settings, storedState, wallet)))
  }
  "OrderHistoryActor" should {

    "not process expirable messages" in {
      val r = GetOrderHistory(pair, "address", NTP.correctedTime() - OrderHistoryActor.RequestTTL - 1)
      actor ! r
      expectNoMsg()
    }
  }
}
