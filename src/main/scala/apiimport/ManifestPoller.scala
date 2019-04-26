package apiimport

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import apiimport.persistence.ManifestPersistor
import apiimport.provider.ApiProviderLike
import com.typesafe.scalalogging.Logger

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class ManifestPoller(provider: ApiProviderLike, persistor: ManifestPersistor)
                    (implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) {
  val log: Logger = Logger(getClass)

  def startPollingForManifests(): Future[immutable.Seq[Seq[Int]]] = Source
    .tick(0 seconds, 1 minute, NotUsed)
    .mapAsync(1) { _ =>
      persistor.lastPersistedFileName
        .map {
          case Some(lpf) => lpf
          case None => ""
        }
        .flatMap { lpf =>
          persistor
            .addPersistence(provider.manifestsStream(lpf))
            .runWith(Sink.seq)
        }
        .recover {
          case t =>
            log.error("Failed to get the last persisted file name", t)
            Seq()
        }
    }
    .runWith(Sink.seq)
}
