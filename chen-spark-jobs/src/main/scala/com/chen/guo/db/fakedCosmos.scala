package com.chen.guo.db

object fakedCosmos {
  val customer1 = "customer1"
  val customer2 = "customer2"

  private val cosmosId = Map(
    customer1 -> "6358f0cd-ce12-4e89-be99-66b16637880e",
    customer2 -> "a75cef49-07f3-4028-bd1b-38731cf1ff4f")

  def getId(clientName: String) = cosmosId(clientName)
}
