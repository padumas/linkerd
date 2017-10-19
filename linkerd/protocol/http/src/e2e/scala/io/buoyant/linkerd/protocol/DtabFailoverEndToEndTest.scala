package io.buoyant.linkerd.protocol

import java.net.InetSocketAddress

import com.twitter.finagle.buoyant.linkerd.Headers
import com.twitter.finagle.http.Method._
import com.twitter.finagle.http.{param => _, _}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.{Annotation, BufferingTracer, NullTracer}
import com.twitter.finagle.{Http => FinagleHttp, Status => _, http => _, _}
import com.twitter.util._
import io.buoyant.linkerd.Linker
import io.buoyant.test.Awaits
import org.scalatest.{FunSuite, MustMatchers}

class DtabFailoverEndToEndTest extends FunSuite with Awaits with MustMatchers {

  case class Downstream(name: String, server: ListeningServer) {
    val address = server.boundAddress.asInstanceOf[InetSocketAddress]
    val port = address.getPort
    val dentry = Dentry(
      Path.read(s"/svs/$name"),
      NameTree.read(s"/$$/inet/127.1/$port")
    )
  }

  object Downstream {
    def mk(name: String, port: Option[Int])(f: Request=>Response): Downstream = {
      val service = Service.mk { req: Request => Future(f(req)) }
      val stack = FinagleHttp.server.stack.remove(Headers.Ctx.serverModule.role)
      val server = FinagleHttp.server.withStack(stack)
        .configured(param.Label(name))
        .configured(param.Tracer(NullTracer))
        .serve( port match {case Some(p) => ":"+p case None => ":*"}, service)
      Downstream(name, server)
    }

    def const(name: String, value: String, status: Status = Status.Ok, port: Option[Int]=None): Downstream =
      mk(name, port) { _ =>
        val rsp = Response()
        rsp.status = status
        rsp.contentString = value
        rsp
      }
  }

  def upstream(server: ListeningServer) = {
    val address = Address(server.boundAddress.asInstanceOf[InetSocketAddress])
    val name = Name.Bound(Var.value(Addr.Bound(address)), address)
    val stack = FinagleHttp.client.stack.remove(Headers.Ctx.clientModule.role)
    FinagleHttp.client.withStack(stack)
      .configured(param.Stats(NullStatsReceiver))
      .configured(param.Tracer(NullTracer))
      .newClient(name, "upstream").toService
  }

  def basicConfig(dtab: Dtab) =
    s"""|routers:
        |- protocol: http
        |  dtab: ${dtab.show}
        |  servers:
        |  - port: 0
        |  service:
        |    retries:
        |      budget:
        |        minRetriesPerSec: 10
        |        percentCanRetry: 1.0
        |      backoff:
        |        kind: constant
        |        ms: 90
        |  client:
        |    failFast: true
        |    failureAccrual:
        |      kind: io.l5d.consecutiveFailures
        |      failures: 5
        |      backoff:
        |        kind: constant
        |        ms: 90
        |    requeueBudget:
        |      minRetriesPerSec: 10
        |      percentCanRetry: 1.0
        |      ttlSecs: 10
        |""".stripMargin

  def annotationKeys(annotations: Seq[Annotation]): Seq[String] =
    annotations.collect {
      case Annotation.ClientSend() => "cs"
      case Annotation.ClientRecv() => "cr"
      case Annotation.ServerSend() => "ss"
      case Annotation.ServerRecv() => "sr"
      case Annotation.WireSend => "ws"
      case Annotation.WireRecv => "wr"
      case Annotation.BinaryAnnotation(k, _) if k == "l5d.success" => k
      case Annotation.Message(m) if Seq("l5d.retryable", "l5d.failure").contains(m) => m
    }

  test("dtab-precedence failover and failback") {
    val stats = new InMemoryStatsReceiver
    val tracer = new BufferingTracer
    def withAnnotations(f: Seq[Annotation] => Unit): Unit = {
      f(tracer.iterator.map(_.annotation).toSeq)
      tracer.clear()
    }

    val cat = Downstream.const("cat", "meow")
    var dog = Downstream.const("dog", "woof")
    val dogPort = dog.port


    def requeues = stats.counters.getOrElse(
      Seq("rt", "http", "client", s"$$/inet/127.1/${dog.port}", "retries", "requeues"),
      0
    )

    val dtab = Dtab.read(s"""
      /svc/animal => /$$/inet/127.1/${cat.port};
      /svc/animal => /$$/inet/127.1/${dog.port};
    """)

    val linker = Linker.load(basicConfig(dtab))
      .configured(param.Stats(stats))
      .configured(param.Tracer(tracer))
    val router = linker.routers.head.initialize()
    val server = router.servers.head.serve()

    val client = upstream(server)
    def get(host: String, path: String = "/")(f: Response => Unit): Unit = {
      val req = Request()
      req.host = host
      req.uri = path
      val rsp = await(client(req))
      f(rsp)
    }

    await(dog.server.close())

    try {

      // Priming the inet namer
      for (_ <- 1 to 10) get("animal") _

      // Dog is down, expect failover to Cat
      get("animal") { rsp =>
        println(stats.print(Console.out))
        assert(rsp.status == Status.Ok)
        assert(rsp.contentString == "meow")
        val path = "/svc/animal"
        val bound = s"/$$/inet/127.1/${cat.port}"
        withAnnotations { anns =>
          assert(annotationKeys(anns) == Seq("sr", "cs", "ws", "wr", "l5d.success", "cr", "ss"))
          assert(anns.contains(Annotation.BinaryAnnotation("service", path)))
          assert(anns.contains(Annotation.BinaryAnnotation("client", bound)))
          assert(anns.contains(Annotation.BinaryAnnotation("residual", "/")))
          ()
        }
      }

      // Start Dog server again, on same port as it originally was
      dog = Downstream.const("dog", "woof", Status.Ok, Some(dogPort))

      // Priming (it might take a few tries for the fallback to occur)
      for (_ <- 1 to 10) get("animal") _

      // Now that Dog is back, expect woof
      get("animal") { rsp =>
        assert(rsp.status == Status.Ok)
        assert(rsp.contentString == "woof")
        val path = "/svc/animal"
        val bound = s"/$$/inet/127.1/${dog.port}"
        withAnnotations { anns =>
          assert(annotationKeys(anns) == Seq("sr", "cs", "ws", "wr", "l5d.success", "cr", "ss"))
          assert(anns.contains(Annotation.BinaryAnnotation("service", path)))
          assert(anns.contains(Annotation.BinaryAnnotation("client", bound)))
          assert(anns.contains(Annotation.BinaryAnnotation("residual", "/")))
          ()
        }
      }

    } finally {
      await(client.close())
      await(cat.server.close())
      await(dog.server.close())
      await(server.close())
      await(router.close())
    }
  }
}
