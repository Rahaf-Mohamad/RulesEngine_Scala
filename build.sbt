import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import java.time.LocalDate
import scala.io.Source

object DiscountCalculator extends App {
  // Define case classes for transactions and discounts
  case class Transaction(product: String, expiryDate: LocalDate, quantity: Int, unitPrice: Double)

  case class Discount(percent: Double, reason: String)

  // Define qualifying rules as functions returning Option[Discount]
  val qualifyingRules: List[Transaction => Option[Discount]] = List(
    // Rule: Less than 30 days remaining for the product to expire
    transaction =>
      if (LocalDate.now().until(transaction.expiryDate).getDays < 30)
        Some(Discount((30 - LocalDate.now().until(transaction.expiryDate).getDays) * 0.01, "Expiration discount"))
      else None,

    // Rule: Cheese and wine products are on sale
    transaction =>
      if (transaction.product.toLowerCase.contains("cheese")) Some(Discount(0.10, "Cheese discount"))
      else if (transaction.product.toLowerCase.contains("wine")) Some(Discount(0.05, "Wine discount"))
      else None,

    // Rule: Products sold on 23rd of March have a special discount
    transaction =>
      if (transaction.expiryDate.getMonthValue == 3 && transaction.expiryDate.getDayOfMonth == 23)
        Some(Discount(0.50, "Special discount"))
      else None,

    // Rule: Bought more than 5 of the same product
    transaction =>
      if (transaction.quantity > 5)
        Some(
          Discount(
            transaction.quantity match {
              case q if q >= 6 && q <= 9 => 0.05
              case q if q >= 10 && q <= 14 => 0.07
              case _ => 0.10
            },
            "Quantity discount"
          )
        )
      else None
  )

  // Apply qualifying rules to get applicable discounts for a transaction
  def applyQualifyingRules(transaction: Transaction): List[Discount] =
    qualifyingRules.flatMap(rule => rule(transaction))

  // Calculate final price based on discounts
  def calculateFinalPrice(transaction: Transaction, discounts: List[Discount]): Double = {
    val totalDiscount = if (discounts.isEmpty) 0.0 else discounts.map(_.percent).sum / discounts.length
    val discountedPrice = transaction.quantity * transaction.unitPrice
    discountedPrice - (discountedPrice * totalDiscount)
  }

  // Log events to a file
  def logEvent(level: String, message: String): Unit = {
    val logFile = new PrintWriter("rules_engine.log")
    logFile.write(s"${LocalDate.now()} $level $message\n")
    logFile.close()
  }

  // Read transactions from CSV file
  val csvFilePath = "D:/9Months-ITI/Apache Scala/Project/TRX1000.csv"
  val outputFile = "D:/9Months-ITI/Apache Scala/Project/discounted_transactions.csv"

  try {
    val transactions = Source.fromFile(csvFilePath).getLines().drop(1).map { line =>
      val Array(_, product, expiryDate, quantityStr, unitPriceStr, _, _) = line.split(",")
      val quantity = quantityStr.toInt
      val unitPrice = unitPriceStr.toDouble
      Transaction(product, LocalDate.parse(expiryDate), quantity, unitPrice)
    }.toList

    // Process transactions and log events
    val results = transactions.map { transaction =>
      val discounts = applyQualifyingRules(transaction)
      val finalPrice = calculateFinalPrice(transaction, discounts)
      logEvent("INFO", s"Transaction for ${transaction.product} with ${transaction.quantity} units on ${transaction.expiryDate}: Final price is $finalPrice")
      (transaction, finalPrice)
    }

    // Write results to a new file
    val writer = new PrintWriter(outputFile)
    writer.println("Product,Expiry Date,Quantity,Unit Price,Final Price")
    results.foreach { case (transaction, finalPrice) =>
      writer.println(s"${transaction.product},${transaction.expiryDate},${transaction.quantity},${transaction.unitPrice},$finalPrice")
    }
    writer.close()

    println("Discount calculation and logging completed successfully.")
  } catch {
    case e: Exception =>
      println(s"An error occurred: ${e.getMessage}")
      e.printStackTrace()
  }
}
