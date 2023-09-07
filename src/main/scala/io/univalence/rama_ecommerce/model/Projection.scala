package io.univalence.rama_ecommerce.model

import com.rpl.rama.RamaSerializable

case class Projection(store: String, product: String, projection: Double) extends RamaSerializable
