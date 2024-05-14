import java.util.logging.{Logger, FileHandler, SimpleFormatter}
import java.time.LocalDate
import scala.io.Source
import java.io.PrintWriter
import java.io.File
import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import scala.math.BigDecimal.RoundingMode
import java.time.temporal.ChronoUnit

object DiscountCalculator extends App {
  // Define the data model
  case class Order(timestamp: String, productCategory: String, productName: String, expiryDate: String, quantity: Int, unitPrice: BigDecimal, channel: String, paymentMethod: String, discount: BigDecimal, finalPrice: BigDecimal, daysToExpiry: Long)

  // Initialize logger
  val logger: Logger = Logger.getLogger("RulesEngineLogger")
  val fileHandler: FileHandler = new FileHandler("rules_engine.log", true) // Append mode
  val formatter: SimpleFormatter = new SimpleFormatter()

  // Set formatter for the file handler
  fileHandler.setFormatter(formatter)

  // Add file handler to the logger
  logger.addHandler(fileHandler)

  // Function to get the product category based on the product name
  def getProductCategory(product: String): (String, String) = {
    val productParts = product.split(" - ")
    if (productParts.length > 1) (productParts.head, productParts.tail.mkString(" - "))
    else ("Unknown", product)
  }

  // Define qualification rules for product category (Cheese & Wine ) discount
  def isQualifiedForProductCategoryDiscount(order: Order): Boolean = {
    order.productCategory.toLowerCase.contains("cheese") || order.productCategory.toLowerCase.contains("wine")
  }

  // Define qualification rules for days to expiry discount
  def isQualifiedForDaysToExpiryDiscount(order: Order): Boolean = {
    order.daysToExpiry <= 29 // Considered qualified if less than or equal to 29 days remaining
  }

  // Entry point of the program
  try {
    // Define input and output file paths
    val csvFilePath = "src/main/resources/TRX1000.csv"
    val outputFile = "src/main/resources/discounted_transactions.csv"

    // Read orders from CSV file
    val orders = readOrdersFromCSV(csvFilePath)

    // Process orders
    val processedOrders = processOrders(orders)

    // Write processed orders to database
    val writer = new PrintWriter(outputFile)
    write_to_db(processedOrders, writer)

    // Log success
    logEvent("INFO", s"Processed orders saved to database and CSV file")
  } catch {
    case e: Throwable =>
      logEvent("SEVERE", "An error occurred:")
      e.printStackTrace()
      throw e
  }

  // Read orders from CSV file
  def readOrdersFromCSV(filePath: String): List[Order] = {
    Source.fromFile(filePath).getLines().toList.tail.map { line =>
      val Array(timestamp, product, expiryDate, quantityStr, unitPriceStr, channel, paymentMethod) = line.split(",").map(_.trim)
      val quantity = quantityStr.toInt
      val unitPrice = BigDecimal(unitPriceStr)
      val (productCategory, actualProductName) = getProductCategory(product)
      val daysToExpiry = ChronoUnit.DAYS.between(LocalDate.parse(timestamp.split("T")(0)), LocalDate.parse(expiryDate))
      Order(timestamp, productCategory, actualProductName, expiryDate, quantity, unitPrice, channel, paymentMethod, BigDecimal(0), BigDecimal(0), daysToExpiry)
    }
  }

  // Process orders
  def processOrders(orders: List[Order]): List[Order] = {
    // Calculate discount and final price for each order
    orders.map(calculateDiscountAndFinalPrice)
  }

  // Calculate discount and final price for a single order
  def calculateDiscountAndFinalPrice(order: Order): Order = {
    val (discount, finalPrice) = calculateDiscount(order)
    order.copy(discount = discount, finalPrice = finalPrice)
  }

  // Calculate discount and final price based on order properties
  def calculateDiscount(order: Order): (BigDecimal, BigDecimal) = {
    val discount = {
      val discountForDaysToExpiry = if (isQualifiedForDaysToExpiryDiscount(order)) calculateDiscountForDaysToExpiry(order) else BigDecimal(0)
      val discountForProductCategory = if (isQualifiedForProductCategoryDiscount(order)) calculateDiscountForProductCategory(order) else BigDecimal(0)
      val discountForDate = if (isQualifiedForDateDiscount(order)) calculateDiscountForDate(order) else BigDecimal(0)
      val discountForQuantity = if (isQualifiedForQuantityDiscount(order)) calculateDiscountForQuantity(order) else BigDecimal(0)
      val discountForAppSale = if (isQualifiedForAppSaleDiscount(order)) calculateDiscountForAppSale(order) else BigDecimal(0)
      val discountForVisaSale = if (isQualifiedForVisaSaleDiscount(order)) calculateDiscountForVisaSale(order) else BigDecimal(0)
      val discounts = List(discountForDaysToExpiry, discountForProductCategory, discountForDate, discountForQuantity, discountForAppSale, discountForVisaSale)
      calculateFinalDiscount(discounts)
    }
    val totalDue = order.unitPrice * order.quantity * (1 - discount)
    logEvent("INFO", s"Discount calculated for order: ${order.productName}", Some(order))
    (discount, totalDue)
  }

  // Define discount calculation for days to expiry discount
  def calculateDiscountForDaysToExpiry(order: Order): BigDecimal = {
    // Discount calculation for days to expiry: 1% discount for each day remaining, capped at 10%
    val discountPercentage = order.daysToExpiry.min(10)
    BigDecimal(discountPercentage) / 100
  }

  // Define discount calculation for product category (Cheese & Wine )discount
  def calculateDiscountForProductCategory(order: Order): BigDecimal = {
    if (order.productCategory.toLowerCase.contains("cheese")) 0.1 // 10% discount for cheese
    else if (order.productCategory.toLowerCase.contains("wine")) 0.05 // 5% discount for wine
    else BigDecimal(0)
  }

  // Define qualification rules for date 23rd of March discount
  def isQualifiedForDateDiscount(order: Order): Boolean = {
    order.timestamp.split("T")(0) == "2023-03-23" // Products sold on 23rd of March
  }

  // Define discount calculation for date 23rd of March discount
  def calculateDiscountForDate(order: Order): BigDecimal = {
    0.5 // 50% discount
  }

  // Define qualification rules for quantity discount
  def isQualifiedForQuantityDiscount(order: Order): Boolean = {
    order.quantity > 5 // Bought more than 5 of the same product
  }

  // Define discount calculation for quantity discount
  def calculateDiscountForQuantity(order: Order): BigDecimal = {
    if (order.quantity >= 6 && order.quantity <= 9) 0.05 // 5% for 6-9 units
    else if (order.quantity >= 10 && order.quantity <= 14) 0.07 // 7% for 10-14 units
    else if (order.quantity > 15) 0.10 // 10% for more than 15 units
    else BigDecimal(0)
  }

  // Define qualification rules for App sale discount
  def isQualifiedForAppSaleDiscount(order: Order): Boolean = {
    order.channel.toLowerCase == "app"
  }

  // Define discount calculation for App sale discount
  def calculateDiscountForAppSale(order: Order): BigDecimal = {
    val roundedQuantity = Math.ceil(order.quantity.toDouble / 5).toInt * 5
    val discountPercentage = roundedQuantity match {
      case q if q <= 5 => 0.05 // 5% discount for quantity 1-5
      case q if q <= 10 => 0.10 // 10% discount for quantity 6-10
      case q if q <= 15 => 0.15 // 15% discount for quantity 11-15
      case _ => 0.20 // 20% discount for quantity > 15
    }
    BigDecimal(discountPercentage)
  }

  // Define qualification rules for Visa card sale discount
  def isQualifiedForVisaSaleDiscount(order: Order): Boolean = {
    order.paymentMethod.toLowerCase == "visa"
  }

  // Define discount calculation for Visa card sale discount
  def calculateDiscountForVisaSale(order: Order): BigDecimal = {
    0.05 // 5% discount for Visa card sales
  }

  // Calculate final discount considering all applicable discounts
  def calculateFinalDiscount(discounts: List[BigDecimal]): BigDecimal = {
    val sortedDiscounts = discounts.sorted.reverse
    val totalApplicableDiscounts = sortedDiscounts.take(2).sum
    totalApplicableDiscounts / 2
  }

  // Log events to a file with a custom format
  def logEvent(level: String, message: String, order: Option[Order] = None): Unit = {
    val logMessage = order match {
      case Some(ord) =>
        s"${LocalDate.now()} - $level - $message ${ord.quantity},${ord.unitPrice},${ord.channel},${ord.paymentMethod},${ord.discount},${ord.finalPrice}"
      case None =>
        s"${LocalDate.now()} - $level - $message"
    }
    logger.info(logMessage)
  }

  def write_to_db(orders: List[Order], writer: PrintWriter): Unit = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    val url = "jdbc:oracle:thin:@//localhost:1521/XE"
    val username = "HR"
    val password = "hr"

    val insertStatement =
      """
        |INSERT INTO Scala_Orders (order_date, expiry_date, days_to_expiry, product_category,
        |                   product_name, quantity, unit_price, channel, payment_method,
        |                   discount, final_price)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver") // Load the Oracle JDBC driver
      connection = DriverManager.getConnection(url, username, password)
      connection.setAutoCommit(false) // Set auto-commit to false

      // Prepare the INSERT statement
      preparedStatement = connection.prepareStatement(insertStatement)

      // Write header to CSV
      writer.println("order_date, expiry_date, days_to_expiry, product_category, product_name, quantity, unit_price, channel, payment_method, discount, final_price")

      // Insert data into the table and write to CSV
      orders.foreach { order =>
        // Insert into database
        preparedStatement.setDate(1, Date.valueOf(order.timestamp.split("T")(0)))
        preparedStatement.setDate(2, Date.valueOf(order.expiryDate))
        preparedStatement.setLong(3, order.daysToExpiry)
        preparedStatement.setString(4, order.productCategory)
        preparedStatement.setString(5, order.productName)
        preparedStatement.setInt(6, order.quantity)
        preparedStatement.setBigDecimal(7, order.unitPrice.bigDecimal)
        preparedStatement.setString(8, order.channel)
        preparedStatement.setString(9, order.paymentMethod)
        preparedStatement.setBigDecimal(10, order.discount.bigDecimal)
        preparedStatement.setBigDecimal(11, order.finalPrice.bigDecimal)
        preparedStatement.addBatch() // Add the current INSERT statement to the batch

        // Write to CSV
        writer.println(s"${order.timestamp.split("T")(0)}, ${order.expiryDate}, ${order.daysToExpiry}, ${order.productCategory}, ${order.productName}, ${order.quantity}, ${order.unitPrice}, ${order.channel}, ${order.paymentMethod}, ${order.discount}, ${order.finalPrice}")
      }

      // Execute the batch of INSERT statements
      preparedStatement.executeBatch()
      connection.commit() // Commit the transaction
      logEvent("INFO", "Successfully inserted into database")
    } catch {
      case e: Exception =>
        logEvent("ERROR", s"Failed to insert into database: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Close resources
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
      writer.close() // Close the CSV writer
      logEvent("DEBUG", "Closed database connection")
    }
  }
}
