package io.univalence.rama_ecommerce.model

import com.rpl.rama.RamaSerializable
import com.rpl.rama.ops.RamaFunction1

import java.util
import scala.jdk.CollectionConverters._

case class OrderItem(
    product: String,
    quantity: Double
) extends RamaSerializable

case class Order(
    orderId: String,
    store: String,
    items: List[OrderItem]) extends RamaSerializable

case class OrderKey(orderId: String) extends RamaSerializable

class ChangeToStoreKey extends RamaFunction1[Order, util.List[util.List[RamaSerializable with Product]]] {
  override def invoke(order: Order): util.List[util.List[RamaSerializable with Product]] = {
    (for {
      item <- order.items
      product = item.product
    } yield {
      Seq(StockKey(order.store, product), order).asJava
    }).asJava
  }
}