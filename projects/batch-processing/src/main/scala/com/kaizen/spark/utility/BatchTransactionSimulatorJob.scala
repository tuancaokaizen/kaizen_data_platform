package com.kaizen.spark.utility

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp, Types}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone, UUID}
import scala.util.Random
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils

/**
 * BatchTransactionSimulatorJob
 *
 * Generates transactional records (Orders, OrderItems, Payments, Transactions, Returns, ReturnItems, OrderPayments, ReturnPayments)
 * bounded by --fromDate and --toDate, and saves them directly to PostgreSQL using optimized JDBC batch operations.
 */
object BatchTransactionSimulatorJob {

  case class ProductRef(id: String, retailPrice: Int)
  case class VoucherRef(code: String, startDate: Long, endDate: Long, amount: Int, minOrderValue: Int)
  case class PromotionRef(code: String, promoType: String, startDate: Long, endDate: Long)
  case class GiftRef(id: String, productId: String, promotionCode: String, quantity: Int)

  case class OrderItemLocal(
    id: String,
    orderId: String,
    productId: String,
    quantity: Int,
    unitPrice: Int,
    createdDate: Long,
    modifiedDate: Long
  )

  case class ReturnItemLocal(
    id: String,
    returnId: String,
    productId: String,
    quantity: Int,
    unitPrice: Int,
    orderItemId: String,
    orderId: String,
    totalAmount: Int,
    createdDate: Long,
    modifiedDate: Long
  )

  private val BATCH_SIZE = 500
  private val RETURN_PROBABILITY = 4 // 1 in 4 = 25%

  def main(args: Array[String]): Unit = {
    println("=== Starting Batch Transaction Simulator (Scala) ===")

    // Parse Arguments
    val parsedArgs = SparkIOUtils.parseArgs(args)
    val fromDateStr = parsedArgs.get("fromDate")
    val toDateStr = parsedArgs.get("toDate")
    val count = parsedArgs.get("count").flatMap(c => scala.util.Try(c.toInt).toOption).getOrElse(10000)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+7"))

    val fromDateMs = fromDateStr match {
      case Some(d) =>
        val fullFrom = if (d.trim.length == 10) d.trim + " 00:00:00" else d.trim
        sdf.parse(fullFrom).getTime
      case None =>
        val cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+7"))
        cal.add(Calendar.DAY_OF_MONTH, -1)
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        cal.getTimeInMillis
    }

    val toDateMs = toDateStr match {
      case Some(d) =>
        val fullTo = if (d.trim.length == 10) d.trim + " 23:59:59" else d.trim
        sdf.parse(fullTo).getTime
      case None =>
        val cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+7"))
        cal.set(Calendar.HOUR_OF_DAY, 23)
        cal.set(Calendar.MINUTE, 59)
        cal.set(Calendar.SECOND, 59)
        cal.set(Calendar.MILLISECOND, 999)
        cal.getTimeInMillis
    }

    if (fromDateMs > toDateMs) {
      throw new IllegalArgumentException("Invalid date range: --fromDate cannot be after --toDate")
    }

    println(s"Date Range: ${sdf.format(new Date(fromDateMs))} -> ${sdf.format(new Date(toDateMs))}")
    println(s"Target Transaction Count: $count")

    val config = new SparkConfig()
    val jdbcUrl = SparkIOUtils.getPostgresJdbcUrl(config)

    Class.forName("org.postgresql.Driver")
    val conn = DriverManager.getConnection(jdbcUrl, config.get("postgres", "username"), config.get("postgres", "password"))
    conn.setAutoCommit(false)

    try {
      // 1. Load Master Data from Postgres
      val customerIds = queryIds(conn, "SELECT \"Id\" FROM customer.\"Customer\"")
      val shopCodes = queryIds(conn, "SELECT \"Code\" FROM shop.\"Shop\"")
      val employeeIds = queryIds(conn, "SELECT \"Id\" FROM shop.\"Employee\" WHERE \"Status\"=true")

      var products = List.empty[ProductRef]
      val prodStmt = conn.createStatement()
      val prodRs = prodStmt.executeQuery("SELECT \"Id\", \"RetailPrice\" FROM product.\"Product\" WHERE \"Status\"=true AND \"IsPromotion\"=false")
      while (prodRs.next()) {
        products = ProductRef(prodRs.getString(1), prodRs.getInt(2)) :: products
      }
      prodRs.close()
      prodStmt.close()

      var vouchers = List.empty[VoucherRef]
      val vStmt = conn.createStatement()
      val vRs = vStmt.executeQuery("SELECT \"Code\", \"StartDate\", \"EndDate\", \"VoucherAmount\", \"MinOrderValue\" FROM promotion.\"Voucher\" WHERE \"Status\"=true")
      while (vRs.next()) {
        vouchers = VoucherRef(
          vRs.getString(1),
          vRs.getTimestamp(2).getTime,
          vRs.getTimestamp(3).getTime,
          vRs.getInt(4),
          vRs.getInt(5)
        ) :: vouchers
      }
      vRs.close()
      vStmt.close()

      var promotions = List.empty[PromotionRef]
      val pStmt = conn.createStatement()
      val pRs = pStmt.executeQuery("SELECT \"Code\", \"Type\", \"StartDate\", \"EndDate\" FROM promotion.\"Promotion\" WHERE \"Status\"=true")
      while (pRs.next()) {
        promotions = PromotionRef(pRs.getString(1), pRs.getString(2), pRs.getTimestamp(3).getTime, pRs.getTimestamp(4).getTime) :: promotions
      }
      pRs.close()
      pStmt.close()

      var gifts = List.empty[GiftRef]
      val gStmt = conn.createStatement()
      val gRs = gStmt.executeQuery("SELECT \"Id\", \"ProductId\", \"PromotionCode\", \"PromotionQuantity\" FROM promotion.\"Gift\" WHERE \"Status\"=true")
      while (gRs.next()) {
        gifts = GiftRef(gRs.getString(1), gRs.getString(2), gRs.getString(3), gRs.getInt(4)) :: gifts
      }
      gRs.close()
      gStmt.close()

      println(s"Master data loaded. Shops: ${shopCodes.size}, Products: ${products.size}, Customers: ${customerIds.size}, Employees: ${employeeIds.size}")

      if (customerIds.isEmpty || shopCodes.isEmpty || employeeIds.isEmpty || products.isEmpty) {
        throw new IllegalStateException("Required master data is empty. Make sure databases are seeded!")
      }

      // SQL statements
      val orderSql = "INSERT INTO \"order\".\"Order\" " +
              "(\"Id\",\"CustomerId\",\"ShopCode\",\"TransactionDate\",\"TotalBill\",\"TotalDiscount\",\"TotalPayment\",\"EmployeeSaleId\",\"Status\",\"CreatedDate\",\"ModifiedDate\",\"VoucherSeriesCode\") " +
              "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) " +
              "ON CONFLICT (\"Id\") DO UPDATE SET " +
              "\"CustomerId\"=EXCLUDED.\"CustomerId\", \"ShopCode\"=EXCLUDED.\"ShopCode\", \"TransactionDate\"=EXCLUDED.\"TransactionDate\", " +
              "\"TotalBill\"=EXCLUDED.\"TotalBill\", \"TotalDiscount\"=EXCLUDED.\"TotalDiscount\", \"TotalPayment\"=EXCLUDED.\"TotalPayment\", " +
              "\"EmployeeSaleId\"=EXCLUDED.\"EmployeeSaleId\", \"Status\"=EXCLUDED.\"Status\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\", \"VoucherSeriesCode\"=EXCLUDED.\"VoucherSeriesCode\""

      val orderItemSql = "INSERT INTO \"order\".\"OrderItem\" " +
              "(\"Id\",\"OrderId\",\"ProductId\",\"Quantity\",\"UnitPrice\",\"CreatedDate\",\"ModifiedDate\") " +
              "VALUES (?,?,?,?,?,?,?) " +
              "ON CONFLICT (\"Id\",\"OrderId\") DO UPDATE SET \"Quantity\"=EXCLUDED.\"Quantity\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\""

      val returnSql = "INSERT INTO \"order\".\"Return\" (\"Id\",\"OrderId\",\"ReturnAmount\",\"CreatedDate\",\"ModifiedDate\") " +
              "VALUES (?,?,?,?,?) ON CONFLICT (\"Id\",\"OrderId\") DO UPDATE SET \"ReturnAmount\"=EXCLUDED.\"ReturnAmount\""

      val returnItemSql = "INSERT INTO \"order\".\"ReturnItem\" " +
              "(\"Id\",\"ReturnId\",\"ProductId\",\"Quantity\",\"UnitPrice\",\"OrderItemId\",\"OrderId\",\"TotalAmount\",\"CreatedDate\",\"ModifiedDate\") " +
              "VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (\"Id\") DO UPDATE SET " +
              "\"Quantity\"=EXCLUDED.\"Quantity\", \"TotalAmount\"=EXCLUDED.\"TotalAmount\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\""

      val paymentSql = "INSERT INTO payment.\"Payment\" (\"Id\",\"PaymentType\",\"Status\",\"TotalPayment\",\"CreatedDate\",\"ModifiedDate\") " +
              "VALUES (?,?,?,?,?,?) ON CONFLICT (\"Id\") DO UPDATE SET \"Status\"=EXCLUDED.\"Status\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\""

      val txSql = "INSERT INTO payment.\"Transaction\" (\"Id\",\"PaymentId\",\"PaymentMethodId\",\"Amount\",\"CreatedDate\",\"ModifiedDate\") " +
              "VALUES (?,?,?,?,?,?) ON CONFLICT (\"Id\") DO UPDATE SET \"Amount\"=EXCLUDED.\"Amount\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\""

      val opSql = "INSERT INTO payment.\"OrderPayment\" (\"Id\",\"PaymentId\",\"OrderId\",\"CreatedDate\",\"ModifiedDate\") " +
              "VALUES (?,?,?,?,?) ON CONFLICT (\"Id\") DO UPDATE SET \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\""

      val rpSql = "INSERT INTO payment.\"ReturnPayment\" (\"Id\",\"PaymentId\",\"ReturnId\",\"OrderId\",\"CreatedDate\",\"ModifiedDate\") " +
              "VALUES (?,?,?,?,?,?) ON CONFLICT (\"Id\") DO UPDATE SET \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\""

      val orderPs = conn.prepareStatement(orderSql)
      val itemPs = conn.prepareStatement(orderItemSql)
      val returnPs = conn.prepareStatement(returnSql)
      val returnItemPs = conn.prepareStatement(returnItemSql)
      val paymentPs = conn.prepareStatement(paymentSql)
      val txPs = conn.prepareStatement(txSql)
      val opPs = conn.prepareStatement(opSql)
      val rpPs = conn.prepareStatement(rpSql)

      implicit val rnd: Random = new Random()
      val diff = toDateMs - fromDateMs

      var batchCount = 0

      for (_ <- 0 until count) {
        val orderDate = fromDateMs + (if (diff <= 0) 0L else (rnd.nextDouble() * diff).toLong)

        val customerId = pickPareto(customerIds)
        val orderId = "BORD-" + uuid12()
        val itemCount = rnd.nextInt(5) + 1
        var items = List.empty[OrderItemLocal]
        var totalBill = 0

        for (_ <- 0 until itemCount) {
          val itemId = "BOI-" + uuid12()
          val prod = pick(products)
          val qty = rnd.nextInt(9) + 1
          val unitPrice = prod.retailPrice
          totalBill += qty * unitPrice
          items = OrderItemLocal(itemId, orderId, prod.id, qty, unitPrice, orderDate, orderDate) :: items
        }

        // Gift promotion logic
        var applicableGifts = List.empty[GiftRef]
        for (p <- promotions) {
          if (p.promoType == "GIFT" && p.startDate <= orderDate && orderDate <= p.endDate) {
            for (g <- gifts) {
              if (g.promotionCode == p.code) {
                applicableGifts = g :: applicableGifts
              }
            }
          }
        }
        if (applicableGifts.nonEmpty) {
          val chosenGift = pick(applicableGifts)
          val giftItemId = "BOI-" + uuid12()
          items = OrderItemLocal(giftItemId, orderId, chosenGift.productId, 1, 0, orderDate, orderDate) :: items
        }

        // Voucher logic
        var appliedVoucher: Option[VoucherRef] = None
        if (rnd.nextDouble() < 0.3) {
          var activeVouchers = List.empty[VoucherRef]
          for (v <- vouchers) {
            if (v.startDate <= orderDate && orderDate <= v.endDate && totalBill >= v.minOrderValue) {
              activeVouchers = v :: activeVouchers
            }
          }
          if (activeVouchers.nonEmpty) {
            appliedVoucher = Some(pick(activeVouchers))
          }
        }

        var voucherDiscount = 0
        var appliedVoucherSeriesCode: String = null
        appliedVoucher.foreach { v =>
          if (totalBill >= v.minOrderValue) {
            voucherDiscount = v.amount
            appliedVoucherSeriesCode = s"SR_${v.code}_${String.format("%04d", Integer.valueOf(rnd.nextInt(50) + 1))}"
          }
        }

        val totalDiscount = voucherDiscount
        val totalPayment = Math.max(0, totalBill - totalDiscount)

        val shopCode = pickPareto(shopCodes)
        val empId = pick(employeeIds)

        // Determine Return details upfront
        val willBeReturned = rnd.nextInt(RETURN_PROBABILITY) == 0
        val returnableItems = items.filter(_.unitPrice > 0)

        var isReturned = false
        var finalStatus = "Completed"
        var orderModifiedDate = orderDate

        var returnId = ""
        var returnAmount = 0
        var returnItems = List.empty[ReturnItemLocal]
        var returnDate = orderDate

        if (willBeReturned && returnableItems.nonEmpty) {
          isReturned = true
          val fullReturn = rnd.nextBoolean()
          val numItemsToReturn = if (fullReturn) returnableItems.size else rnd.nextInt(returnableItems.size) + 1

          val selectedItems = rnd.shuffle(returnableItems).take(numItemsToReturn)
          val maxReturnDelay = 1000L * 60 * 60 * 24 * 5
          val returnDateLimit = Math.min(toDateMs, orderDate + maxReturnDelay)
          returnDate = orderDate + (if (returnDateLimit <= orderDate) 0L else (rnd.nextDouble() * (returnDateLimit - orderDate)).toLong)

          returnId = "BRET-" + uuid12()
          val isZeroReturn = totalPayment <= totalDiscount
          val discountRatio = totalDiscount.toDouble / totalBill

          for (src <- selectedItems) {
            val retQty = if (fullReturn) src.quantity else rnd.nextInt(src.quantity) + 1
            val itemAmt = if (isZeroReturn) 0 else {
              val discountedPrice = Math.max(0, src.unitPrice - (src.unitPrice * discountRatio).toInt)
              retQty * discountedPrice
            }
            returnAmount += itemAmt

            returnItems = ReturnItemLocal(
              "BRI-" + uuid12(), returnId, src.productId, retQty, src.unitPrice, src.id, orderId, itemAmt, returnDate, returnDate
            ) :: returnItems
          }

          finalStatus = if (fullReturn) "Returned" else "Completed"
          orderModifiedDate = returnDate
        }

        // Order mapping (exactly once)
        orderPs.setString(1, orderId)
        orderPs.setString(2, customerId)
        orderPs.setString(3, shopCode)
        orderPs.setTimestamp(4, new Timestamp(orderDate))
        orderPs.setInt(5, totalBill)
        orderPs.setInt(6, totalDiscount)
        orderPs.setInt(7, totalPayment)
        orderPs.setString(8, empId)
        orderPs.setString(9, finalStatus)
        orderPs.setTimestamp(10, new Timestamp(orderDate))
        orderPs.setTimestamp(11, new Timestamp(orderModifiedDate))
        orderPs.setString(12, appliedVoucherSeriesCode)
        orderPs.addBatch()

        for (item <- items) {
          itemPs.setString(1, item.id)
          itemPs.setString(2, item.orderId)
          itemPs.setString(3, item.productId)
          itemPs.setInt(4, item.quantity)
          itemPs.setInt(5, item.unitPrice)
          itemPs.setTimestamp(6, new Timestamp(item.createdDate))
          itemPs.setTimestamp(7, new Timestamp(item.modifiedDate))
          itemPs.addBatch()
        }

        // Payment (Receipt) - Always SUCCESS
        val payId = "BPAY-" + uuid12()
        val payDate = orderDate + (rnd.nextDouble() * 8000L).toLong + 2000L
        paymentPs.setString(1, payId)
        paymentPs.setShort(2, 1.toShort) // Receipt
        paymentPs.setString(3, "SUCCESS")
        paymentPs.setInt(4, totalPayment)
        paymentPs.setTimestamp(5, new Timestamp(payDate))
        paymentPs.setTimestamp(6, new Timestamp(payDate))
        paymentPs.addBatch()

        val splitPayment = rnd.nextInt(5) == 0
        if (splitPayment && totalPayment > 1000) {
          val part1 = (totalPayment * (rnd.nextDouble() * 0.4 + 0.3)).toInt
          val part2 = totalPayment - part1
          val m1 = pickShort(Array(1.toShort, 2.toShort, 3.toShort))
          val m2 = if (m1 == 1) 2.toShort else 1.toShort

          txPs.setString(1, "BTX-" + uuid12())
          txPs.setString(2, payId)
          txPs.setShort(3, m1)
          txPs.setInt(4, part1)
          txPs.setTimestamp(5, new Timestamp(payDate))
          txPs.setTimestamp(6, new Timestamp(payDate))
          txPs.addBatch()

          txPs.setString(1, "BTX-" + uuid12())
          txPs.setString(2, payId)
          txPs.setShort(3, m2)
          txPs.setInt(4, part2)
          txPs.setTimestamp(5, new Timestamp(payDate))
          txPs.setTimestamp(6, new Timestamp(payDate))
          txPs.addBatch()
        } else {
          val method = pickShort(Array(1.toShort, 2.toShort, 3.toShort))
          txPs.setString(1, "BTX-" + uuid12())
          txPs.setString(2, payId)
          txPs.setShort(3, method)
          txPs.setInt(4, totalPayment)
          txPs.setTimestamp(5, new Timestamp(payDate))
          txPs.setTimestamp(6, new Timestamp(payDate))
          txPs.addBatch()
        }

        opPs.setString(1, "BOP-" + uuid12())
        opPs.setString(2, payId)
        opPs.setString(3, orderId)
        opPs.setTimestamp(4, new Timestamp(payDate))
        opPs.setTimestamp(5, new Timestamp(payDate))
        opPs.addBatch()

        // Return simulation processing
        if (isReturned) {
          returnPs.setString(1, returnId)
          returnPs.setString(2, orderId)
          returnPs.setInt(3, returnAmount)
          returnPs.setTimestamp(4, new Timestamp(returnDate))
          returnPs.setTimestamp(5, new Timestamp(returnDate))
          returnPs.addBatch()

          for (ri <- returnItems) {
            returnItemPs.setString(1, ri.id)
            returnItemPs.setString(2, ri.returnId)
            returnItemPs.setString(3, ri.productId)
            returnItemPs.setInt(4, ri.quantity)
            returnItemPs.setInt(5, ri.unitPrice)
            returnItemPs.setString(6, ri.orderItemId)
            returnItemPs.setString(7, ri.orderId)
            returnItemPs.setInt(8, ri.totalAmount)
            returnItemPs.setTimestamp(9, new Timestamp(ri.createdDate))
            returnItemPs.setTimestamp(10, new Timestamp(ri.modifiedDate))
            returnItemPs.addBatch()
          }

          val refundPayId = "BPAY-REF-" + uuid12()
          val refundPayDate = returnDate + (rnd.nextDouble() * 8000L).toLong + 2000L

          paymentPs.setString(1, refundPayId)
          paymentPs.setShort(2, 2.toShort) // Refund
          paymentPs.setString(3, "SUCCESS")
          paymentPs.setInt(4, returnAmount)
          paymentPs.setTimestamp(5, new Timestamp(refundPayDate))
          paymentPs.setTimestamp(6, new Timestamp(refundPayDate))
          paymentPs.addBatch()

          val refundMethod = pick(List(1.toShort, 2.toShort, 3.toShort))
          txPs.setString(1, "BTX-" + uuid12())
          txPs.setString(2, refundPayId)
          txPs.setShort(3, refundMethod)
          txPs.setInt(4, returnAmount)
          txPs.setTimestamp(5, new Timestamp(refundPayDate))
          txPs.setTimestamp(6, new Timestamp(refundPayDate))
          txPs.addBatch()

          rpPs.setString(1, "BRP-" + uuid12())
          rpPs.setString(2, refundPayId)
          rpPs.setString(3, returnId)
          rpPs.setString(4, orderId)
          rpPs.setTimestamp(5, new Timestamp(refundPayDate))
          rpPs.setTimestamp(6, new Timestamp(refundPayDate))
          rpPs.addBatch()
        }

        batchCount += 1
        if (batchCount % BATCH_SIZE == 0) {
          orderPs.executeBatch()
          itemPs.executeBatch()
          returnPs.executeBatch()
          returnItemPs.executeBatch()
          paymentPs.executeBatch()
          txPs.executeBatch()
          opPs.executeBatch()
          rpPs.executeBatch()
          conn.commit()
          println(s"Generated & Committed: $batchCount transactions...")
        }
      }

      if (batchCount % BATCH_SIZE != 0) {
        orderPs.executeBatch()
        itemPs.executeBatch()
        returnPs.executeBatch()
        returnItemPs.executeBatch()
        paymentPs.executeBatch()
        txPs.executeBatch()
        opPs.executeBatch()
        rpPs.executeBatch()
        conn.commit()
        println(s"Generated & Committed final batch. Total transactions: $batchCount")
      }

      orderPs.close()
      itemPs.close()
      returnPs.close()
      returnItemPs.close()
      paymentPs.close()
      txPs.close()
      opPs.close()
      rpPs.close()

    } catch {
      case e: Exception =>
        conn.rollback()
        System.err.println("Transaction failed, rolled back changes.")
        throw e
    } finally {
      conn.close()
    }

    println("✔ Batch Simulation completed successfully!")
  }

  private def queryIds(conn: Connection, sql: String): List[String] = {
    var list = List.empty[String]
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sql)
    while (rs.next()) {
      list = rs.getString(1) :: list
    }
    rs.close()
    stmt.close()
    list
  }

  private def pick[T](list: List[T])(implicit rnd: Random): T = {
    list(rnd.nextInt(list.size))
  }

  private def pickPareto[T](list: List[T])(implicit rnd: Random): T = {
    val size = list.size
    if (rnd.nextInt(10) < 8) {
      val limit = Math.max(1, (size * 0.2).toInt)
      list(rnd.nextInt(limit))
    } else {
      val limit = Math.max(1, (size * 0.2).toInt)
      if (size > limit) {
        list(limit + rnd.nextInt(size - limit))
      } else {
        list(rnd.nextInt(size))
      }
    }
  }

  private def pickShort(arr: Array[Short])(implicit rnd: Random): Short = {
    arr(rnd.nextInt(arr.length))
  }

  private def uuid12(): String = {
    UUID.randomUUID().toString.replace("-", "").substring(0, 12).toUpperCase
  }
}
