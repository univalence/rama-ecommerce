package io.univalence.rama_ecommerce

import com.rpl.rama.{Depot, PState, Path, RamaModule, RamaSerializable}
import com.rpl.rama.RamaModule.{Setup, Topologies}
import com.rpl.rama.ops.{Ops, RamaFunction1}
import com.rpl.rama.test.{InProcessCluster, LaunchConfig}
import io.univalence.rama_ecommerce.model.{ChangeToStoreKey, Order, OrderItem, OrderKey, Projection, Stock, StockKey}

import java.time.Instant
import scala.util.Using
import java.util
import scala.jdk.CollectionConverters._

class EcommerceModule extends RamaModule {
  def define(setup: Setup, topologies: Topologies): Unit = {
    // Declaring depots with partitioning strategy by the first value.
    setup.declareDepot("*stockDepot", Depot.hashBy(Ops.FIRST))
    setup.declareDepot("*orderDepot", Depot.hashBy(Ops.FIRST))

    // Declaring projection topology and PState. The PState here represent the projections
    val projectionTopology = topologies.stream("projection")
    projectionTopology.pstate("$$rawProjections", PState.mapSchema(
      classOf[StockKey],
      PState.mapSchema(classOf[String], classOf[Object])
    ))
    projectionTopology.pstate("$$projections", PState.mapSchema(
      classOf[StockKey],
      classOf[Projection]
    ))

    // Stream of event from stocks
    projectionTopology.source("*stockDepot").out("*record")
      .each(Ops.EXPAND, "*record").out("*stockKey", "*stock") // Expand the record to key-value
      .localTransform("$$rawProjections", Path.key("*stockKey", "stock").termVal("*stock")) // Save the stock in the path stockKey > stock in the projections PState by replacing the old value
      // TODO: Merge stock and order before computing and sink to projection
      .localSelect("$$rawProjections", Path.key("*stockKey")).out("*rawProjection")
      .each(new ComputeProjection(), "*rawProjection").out("*projection")
      .localTransform("$$projections", Path.key("*stockKey").termVal("*projection"))

    // Stream of event from orders
    projectionTopology.source("*orderDepot").out("*record")
      .each(Ops.EXPAND, "*record").out("*orderKey", "*order") // Expand the record to key-value
      .each(new ChangeToStoreKey(), "*order").out("*listOrderWithStoreKey") // Change the key from orderKey to storeKey (meaning you get a list of values)
      .each(Ops.EXPLODE, "*listOrderWithStoreKey").out("*orderWithStoreKey") // Explode the list to get each element
      .each(Ops.EXPAND, "*orderWithStoreKey").out("*stockKey", "*orderValue") // Expand back to key-value
      .localTransform("$$rawProjections", Path.key("*stockKey").key("order").afterElem().termVal("*orderValue")) // Save the order in the path stockKey > order in the projections PState by adding the last element
      // TODO: Merge stock and order before computing and sink to projection
      .localSelect("$$rawProjections", Path.key("*stockKey")).out("*rawProjection")
      .each(new ComputeProjection(), "*rawProjection").out("*projection")
      .localTransform("$$projections", Path.key("*stockKey").termVal("*projection"))
  }
}

object EcommerceMain extends App {
    Using(InProcessCluster.create) { cluster =>
      // Initialize cluster
      cluster.launchModule(new EcommerceModule, new LaunchConfig(1, 1))

      val moduleName = classOf[EcommerceModule].getName
      val stockDepot = cluster.clusterDepot(moduleName, "*stockDepot")
      val orderDepot = cluster.clusterDepot(moduleName, "*orderDepot")
      val projectionState = cluster.clusterPState(moduleName, "$$projections")

      // Append the stock record
      val stockKey = StockKey("store1", "product1")
      val stock = Stock(stockKey.store, stockKey.product, Instant.now, 2.0)
      val stockRecord: util.List[RamaSerializable] = util.Arrays.asList(stockKey, stock)

      // Append the order records
      val orderKey = OrderKey("OrderId1")
      val orderItem = OrderItem("product1", 1.0)
      val order = Order("OrderId1", stockKey.store, List(orderItem))
      val orderRecord: util.List[RamaSerializable] =
      util.Arrays.asList(orderKey, order)
      val orderRecord2: util.List[RamaSerializable] =
      util.Arrays.asList(
          orderKey.copy(orderId = "OrderId2"),
          order.copy(orderId = "OrderId2", items = List(orderItem, orderItem.copy(product = "product2")))
      )

      def printProjection(): Unit = println(s"Output: ${projectionState.select(Path.key(stockKey)).asScala.toList}")
      printProjection()
      stockDepot.append(stockRecord)
      printProjection()
      orderDepot.append(orderRecord)
      printProjection()
      orderDepot.append(orderRecord2)
      printProjection()
    }.recover {
      case e: Exception => println(e)
    }
}

class ComputeProjection extends RamaFunction1[util.Map[String, Object], Projection] {
  override def invoke(projection: util.Map[String, Object]): Projection = {
    val p = Option(projection.asScala)
    (for {
      p <- p
      stockObj <- p.get("stock")
      stock = stockObj.asInstanceOf[Stock]
    } yield {
      val orderQuantity: Double =
        p
          .getOrElse("order", util.Arrays.asList())
          .asInstanceOf[java.util.List[Order]]
          .asScala
          .toList
          .map(order =>
            order
              .items
              .find(_.product == stock.product)
              .map(_.quantity)
              .getOrElse(0.0)
          ).sum


      Projection(stock.store, stock.product, stock.quantity - orderQuantity)
    }).orNull
  }
}