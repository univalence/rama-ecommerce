package io.univalence.rama_ecommerce.model

import com.rpl.rama.RamaSerializable

import java.time.Instant

case class StockKey(store: String, product: String) extends RamaSerializable

case class Stock(
    store: String,
    product: String,
    checkedAt: Instant,
    quantity: Double
) extends RamaSerializable

