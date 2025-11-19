package io.syspulse.ext.sentinel

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.{Duration,FiniteDuration}
import com.typesafe.scalalogging.Logger
import scala.util.{Try,Success,Failure}
import java.util.UUID
import java.time.{Instant, ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import os._

import io.syspulse.skel.plugin.{Plugin,PluginDescriptor}

import io.syspulse.skel.blockchain.Blockchain
import io.syspulse.skel.blockchain.Token
import io.syspulse.skel.crypto.eth.SolidityTuple
import io.syspulse.skel.crypto.eth.Web3jTrace
import io.syspulse.skel.crypto.Eth

import io.syspulse.haas.ingest.eth.{Block}
import io.syspulse.haas.ingest.eth.etl.{Tx}

import io.hacken.ext.core.Severity
import io.hacken.ext.sentinel.SentryRun
import io.hacken.ext.sentinel.Sentry
import io.hacken.ext.sentinel.util.EventUtil
import io.hacken.ext.detector.DetectorConfig
import io.syspulse.skel.crypto.eth.TokenUtil
import io.hacken.ext.sentinel.util.TokenData
import io.hacken.ext.core.Event
import io.hacken.ext.sentinel.WithWeb3
import io.hacken.ext.sentinel.ThresholdDouble
import io.syspulse.skel.blockchain.Blockchains
import io.syspulse.skel.blockchain.BlockchainRpc

// --------------------------------------------------------------------------------------------------------------------------
case class User(addr:String,chain:String,uid:Option[String]=None)

object User {
  private val log = Logger(getClass.getName)

  def apply(s:String):User = create(s, Map.empty)
  def apply(s:String, chainMapping: Map[String, String]):User = create(s, chainMapping)

  def create(s:String, chainMapping: Map[String, String]):User = {
    val (addrChain,uid) = s.split("=").toList match {
      case addr :: uid :: Nil => (addr.trim,Some(uid))
      case addr :: Nil => (addr.trim,None)
      case _ => throw new Exception(s"Invalid user format: '${s}'")
    }

    val (addr,chainPrefix) = addrChain.split(":").toList match {
      case chain :: addr :: Nil => (addr.trim,chain.trim)
      case addr :: Nil => (addr.trim,"")
      case _ => throw new Exception(s"Invalid address format: '${s}'")
    }

    // Map chain prefix (kuid) to actual chain name
    val chain = if (chainPrefix.nonEmpty) {
      if (!chainMapping.contains(chainPrefix)) {
        log.warn(s"${addrChain}: Chain mapping not found for prefix: '${chainPrefix}'")
      }
      chainMapping.getOrElse(chainPrefix, chainPrefix)
    } else {
      ""
    }

    new User(addr,chain,uid)
  }
}

case class Balance(
  addr:String,
  chain:String,
  token:String,
  tokenAddress:String,
  value:BigInt,
  amount:Double,
  blockHeight:Option[Long],
  ts:Long,
  err:Option[String]=None
)

case class Job(id:String,ts:Long)

// --------------------------------------------------------------------------------------------------------------------------
trait BalanceSource {
  def getBalance(addr:String,chain:String,token:Token,blockHeight:Option[Long]):Try[Balance]
}

// EVM-specific implementation
class BalanceSourceEvm(rpcUrl: String, chainName: String, snapshotTs: Long) extends BalanceSource {

  private val web3Trace: Web3jTrace = Eth.web3(rpcUrl)

  // Calculate and cache block height for the snapshot timestamp
  private val cachedBlockHeight: Long = {
    // TODO: Implement block height lookup by timestamp for the chain
    // Options:
    // 1. Binary search through blocks using web3.ethGetBlockByNumber
    // 2. Use external API (e.g., Etherscan API: ?module=block&action=getblocknobytime)
    // 3. Use pre-built timestamp-to-block index
    // For now, get current block
    web3Trace.ethBlockNumber().send().getBlockNumber.longValue()
  }

  def getBalance(addr:String,chain:String,token:Token,blockHeight:Option[Long]):Try[Balance] = {
    val actualBlockHeight = blockHeight.getOrElse(cachedBlockHeight)

    Try {
      val isNativeToken = token.addr.isEmpty

      val balanceWei: BigInt = if (isNativeToken) {
        // Native token (ETH) balance
        // val blockParam = org.web3j.protocol.core.DefaultBlockParameter.valueOf(java.math.BigInteger.valueOf(actualBlockHeight))
        // val ethBalance = web3Trace.ethGetBalance(addr, blockParam).send().getBalance
        // BigInt(ethBalance)
        Eth.getBalance(addr,Some(actualBlockHeight))(web3Trace) match {
          case Success(balance) => balance
          case Failure(e) => throw e
        }
      } else {
        // ERC20 token balance
        TokenUtil.askErc20Balance(token.addr, addr, Some(token.dec), Some(actualBlockHeight))(web3Trace) match {
          case Success(bal) => bal
          case Failure(e) => throw e
        }
      }

      // Convert from wei/token units to decimal based on token decimals
      val value = balanceWei
      val amount = BigDecimal(value) / BigDecimal(10).pow(token.dec)

      Balance(addr, chain, token.sym, token.addr, value, amount.toDouble, Some(actualBlockHeight), snapshotTs, None)
    }.recoverWith {
      case e: Exception =>
        Failure(new Exception(s"Failed to get EVM balance for ${addr}/${token.sym} at block ${actualBlockHeight}: ${e.getMessage}"))
    }
  }
}

// Solana-specific implementation
class BalanceSourceSolana(rpcUrl: String, chainName: String, snapshotTs: Long) extends BalanceSource {

  // TODO: Initialize sol3j or similar Solana client
  // private val sol3j = new Sol3j(rpcUrl)

  // Calculate and cache block/slot height for the snapshot timestamp
  private val cachedBlockHeight: Long = {
    // TODO: Implement Solana slot/block lookup by timestamp
    0L
  }

  def getBalance(addr:String,chain:String,token:Token,blockHeight:Option[Long]):Try[Balance] = {
    val actualBlockHeight = blockHeight.getOrElse(cachedBlockHeight)

    Try {
      // TODO: Implement Solana balance retrieval
      // For native SOL: use getBalance RPC call
      // For SPL tokens: use getTokenAccountsByOwner
      val value = BigInt(0)
      val amount = 0.0
      Balance(addr, chain, token.sym, token.addr, value, amount, Some(actualBlockHeight), snapshotTs, Some("Solana not implemented"))
    }
  }
}

// Router that delegates to appropriate chain-specific implementation
class BalanceSourceChain(rpc: Blockchains, snapshotTs: Long) extends BalanceSource {
  val log = Logger(this.getClass.getSimpleName)

  // Map chain_id -> BalanceSource implementation
  // Each source calculates and caches its block height during construction
  private val chainSources: Map[String, BalanceSource] = {
    rpc.all().flatMap { r =>
      val name = r.name
      val id = r.id
      val rpcUrl = r.rpcUri

      name match {
        case _ if DetectorKuba.isEvmChain(name) =>
          Some(name -> new BalanceSourceEvm(rpcUrl, name, snapshotTs))
        case "sol" | "solana" =>
          Some(name -> new BalanceSourceSolana(rpcUrl, name, snapshotTs))
        case _ =>
          log.warn(s"unsupported chain: ${name} (${id})")
          None
      }
    }
    .toMap
  }

  def getBalance(addr:String,chain:String,token:Token,blockHeight:Option[Long]):Try[Balance] = {
    chainSources.get(chain.toLowerCase) match {
      case Some(source) => source.getBalance(addr, chain, token, blockHeight)
      case None => Failure(new Exception(s"No balance source configured for chain: ${chain}"))
    }
  }
}

class BalanceSourceDune(apiKey: String, snapshotTs: Long) extends BalanceSource {

  // TODO: Calculate block heights for all chains at snapshot timestamp
  // private val blockHeights: Map[String, Long] = queryBlockHeights(snapshotTs)

  def getBalance(addr:String,chain:String,token:Token,blockHeight:Option[Long]):Try[Balance] = {
    Try {
      // TODO: Implement Dune Analytics API query
      // Query historical balances from Dune at specific block height
      // Use blockHeight if provided, otherwise use cached block height for the chain
      val value = BigInt(0)
      val amount = 0.0
      Balance(addr, chain, token.sym, token.addr, value, amount, blockHeight, snapshotTs, None)
    }
  }
}

// --------------------------------------------------------------------------------------------------------------------------
object DetectorKuba {
  val DEF_DESC = "{addr} {value}{err}"
  val DEF_WHEN = "cron"
  val DEF_SRC = "chain" // chain, dune
  val DEF_COND = "> 0.0"
  val DEF_CURRENCY = "" // native, not usd
  val DEF_USERS = ""
  val DEF_TOKENS = ""
  val DEF_TIMEZONE = "UTC-8" // Hong Kong Time (UTC-8)
  val DEF_OUTPUT_CSV = "./output-1.csv" // If empty, CSV output is disabled
  val DEF_CSV_PREVIEW = 0 // Number of CSV lines to include in Event metadata (0 = disabled)
  val DEF_BALANCE_QUERY_DELAY_MS = 1000L // Delay between balance queries in milliseconds

  val DEF_TRACK_ERR = true
  val DEF_TRACK_ERR_ALWAYS = true

  val DEF_SEV_OK = Severity.INFO
  val DEF_SEV_WARN = Severity.MEDIUM
  val DEF_SEV_ERR = Severity.ERROR

  // Timezone utilities
  val HONG_KONG_TZ = ZoneId.of("Asia/Hong_Kong") // UTC+8 (not UTC-8)
  val UTC_TZ = ZoneId.of("UTC")

  def parseTimezone(tz: String): ZoneId = tz.toLowerCase match {
    case "utc-8" | "hkt" | "hk" => HONG_KONG_TZ
    case "utc" => UTC_TZ
    case _ => ZoneId.of(tz)
  }

  def formatTimestamp(ts: Long, timezone: ZoneId = HONG_KONG_TZ): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z")
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), timezone).format(formatter)
  }

  def parseTimestamp(dateStr: String, timezone: ZoneId = HONG_KONG_TZ): Long = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss") 
    val dt = if(dateStr.contains(":")) 
      dateStr
    else
      dateStr + " 00:00:00"

    val localDateTime = java.time.LocalDateTime.parse(dt, formatter)
    ZonedDateTime.of(localDateTime, timezone).toInstant.toEpochMilli    
  }

  def balancesToCsv(balances: Seq[Balance], timezone: ZoneId = HONG_KONG_TZ, chainMapping: Map[String, String] = Map.empty): String = {
    // Create reverse mapping (chain_name -> kuid)
    val reverseMapping = chainMapping.map { case (k, v) => (v, k) }

    val header = "Address,Network,Token,Token Contract Address,Balance,Block Height,Timestamp,Timestamp (Formatted),Error"
    val rows = balances.map { b =>
      val formattedTs = formatTimestamp(b.ts, timezone)
      val blockHeightStr = b.blockHeight.map(_.toString).getOrElse("")
      val errStr = b.err.map(e => s"\"${e.replace("\"", "\"\"")}\"").getOrElse("")
      // Map chain name back to kuid for CSV output
      val chainKuid = reverseMapping.getOrElse(b.chain, b.chain)
      s"${b.addr},${chainKuid},${b.token},${b.tokenAddress},${b.value},${blockHeightStr},${b.ts},${formattedTs},${errStr}"
    }
    (header +: rows).mkString("\n") + "\n"
  }

  def saveCsv(csv: String, outputPath: String): Try[String] = {
    Try {
      os.write.over(os.Path(outputPath, os.pwd), csv)
      outputPath
    }
  }
  
  def loadUsers(uri:String, chainMapping: Map[String, String] = Map.empty):Seq[User] = {
    val ss = if(uri.startsWith("file://"))
      os.read(os.Path(uri.substring(7),os.pwd))
    else
      uri

    ss.split("[\n,]")
      .map(s => s.trim)
      .filter(s => s.nonEmpty)
      .filter(s => !s.startsWith("#"))
      .map(s => User(s, chainMapping))
  }

  def loadTokens(uri:String):Seq[Token] = {
    val ss = if(uri.startsWith("file://"))
      os.read(os.Path(uri.substring(7),os.pwd))
    else
      uri

    ss.split("[\n,]")
      .map(s => s.trim)
      .filter(s => s.nonEmpty)
      .filter(s => !s.startsWith("#"))
      .map(s => s.split("=").toList match {
        case chain :: sym :: addr :: Nil => 
          Token(addr = if(addr==chain) "" else addr, bid = chain.trim,sym = sym.trim,dec = 0)
        case chain :: sym :: addr :: dec :: Nil => 
          Token(addr = if(addr==chain) "" else addr, bid = chain.trim,sym = sym.trim,dec = dec.toInt)
        case _ => throw new Exception(s"Invalid token format: '${s}'")
      })
  }

  def loadChainTokensMap(csvPath:String): Map[String, Seq[String]] = {
    val csvContent = if(csvPath.startsWith("file://"))
      os.read(os.Path(csvPath.substring(7),os.pwd))
    else
      os.read(os.Path(csvPath,os.pwd))

    csvContent.split("\n")
      .drop(1) // Skip header
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { line =>
        // Parse CSV line handling quoted values
        val parts = line.split(",", 2)
        if(parts.length == 2) {
          val chainId = parts(0).trim
          // Remove quotes and split by comma
          val tokensStr = parts(1).trim.stripPrefix("\"").stripSuffix("\"")
          val tokens = tokensStr.split(",").map(_.trim).toSeq
          chainId -> tokens
        } else {
          throw new Exception(s"Invalid CSV line: '${line}'")
        }
      }
      .toMap
  }

  def getTokensForChain(chainTokensMap: Map[String, Seq[String]], chainId: String): Seq[String] = {
    chainTokensMap.getOrElse(chainId.toLowerCase, Seq.empty)
  }

  // EVM-compatible chains (based on Chain-Tokens-Map.csv)
  val EVM_CHAINS = Set(
    "eth", "ethereum", "arbitrum", "arb", "base", "optimism", "op", "polygon", "matic",
    "bsc", "bnb", "avax", "avaxc", "fantom", "ftm", "aurora", "boba", "celo", "cfx",
    "cronos", "dogechain", "evmos", "fuse", "gnosis", "harmony", "one", "heco", "kava",
    "kavaevm", "kcc", "klaytn", "klay", "linea", "manta", "metis", "moonbeam", "glmr",
    "moonriver", "movr", "oasis", "palm", "ronin", "scroll", "zksync", "zksync2",
    "zkl", "zrc", "blast", "mode", "mantle", "mnt", "taiko", "zeta", "immutable",
    "etc", "ethw", "hype", "hyperevm"
  )

  def isEvmChain(chainId: String): Boolean = {
    EVM_CHAINS.contains(chainId.toLowerCase)
  }
}

class DetectorKuba(pd: PluginDescriptor) extends Sentry with Plugin {  
  override def did = pd.name
  override def toString = s"${this.getClass.getSimpleName}(${did})"
  override val log = Logger(this.getClass.getSimpleName)
  @volatile private var job: Option[Job] = None

  override def getSettings(rx:SentryRun): Map[String,Any] = {
    rx.getConfig().env match {
      case "test" => Map()
      case "dev" =>
        Map( "_cron_rate_limit" -> 1 * 10 * 1000L ) // 10 sec
      case _ =>
        Map( "_cron_rate_limit" -> 1 * 60 * 1000L ) // 1 min
    }
  }
      
  override def onInit(rx:SentryRun,conf: DetectorConfig): Int = {

    //onUpdate(rx,conf)
    val r = super.onInit(rx,conf)
    if(r != SentryRun.SENTRY_INIT) {
      return r
    }

    // Load blockchain configurations
    val blockchains = rx.getConfiguration()(c => Some(c.getListString("blockchains"))).getOrElse(Seq.empty)

    if(blockchains.size == 0) {
      log.error(s"${rx.getExtId()}: blockchains: ${blockchains}")
      return SentryRun.SENTRY_STOPPED
    }

    val rpc = Blockchains(blockchains)
    rx.set("rpc",rpc)

    // Load chain mapping (kuid -> chain_name) from configuration
    val chainMapping = rx.getConfiguration()(c => {
      val mappingConfig = c.getMap("mapping")
      Some(mappingConfig.toMap)
    }).getOrElse(Map.empty[String, String])

    log.info(s"${rx.getExtId()}: chain mapping: ${chainMapping}")
    rx.set("chain_mapping", chainMapping)

    SentryRun.SENTRY_INIT
  }

  override def onStart(rx:SentryRun,conf:DetectorConfig):Int = {    

    val r = onUpdate(rx,conf)
    if(r != SentryRun.SENTRY_RUNNING) {
      return r
    }

    super.onStart(rx,conf)
  }

  override def onUpdate(rx:SentryRun,conf: DetectorConfig): Int = {

    // Get chain mapping from context
    val chainMapping = rx.get("chain_mapping") match {
      case Some(m: Map[_, _]) => m.asInstanceOf[Map[String, String]]
      case _ => Map.empty[String, String]
    }

    val users = DetectorKuba.loadUsers(DetectorConfig.getString(rx.conf,"users",DetectorKuba.DEF_USERS), chainMapping)
    val tokens = DetectorKuba.loadTokens(DetectorConfig.getString(rx.conf,"tokens",DetectorKuba.DEF_TOKENS))

    rx.set("users",users)
    rx.set("tokens",tokens)

    log.info(s"${rx.getExtId()}: users=${users.size}")
    log.info(s"${rx.getExtId()}: tokens=${tokens.size}")

    // val apiKey = {
    //   val key = DetectorConfig.getString(conf,"api_key","")
    //   if(key.isEmpty()) {
    //     sys.env.get("CG_API_KEY").getOrElse("")
    //   } else 
    //     key      
    // }

    // rx.set("api_key",apiKey)

    // set state    
    rx.set("desc",DetectorConfig.getString(rx.conf,"desc",DetectorKuba.DEF_DESC))

    val source = DetectorConfig.getString(rx.conf,"source",DetectorKuba.DEF_SRC)

    // Get snapshot timestamp (will be used to calculate block heights)
    val snapshotTs = rx.get("snapshot_timestamp") match {
      case Some(Some(ts: Long)) => ts
      case Some(ts: Long) => ts
      case _ => System.currentTimeMillis() // Default to current time if not specified
    }

    val balanceSource: BalanceSource = source match {
      case "chain" =>
        // Get blockchains from configuration
        val rpc = rx.get("rpc").asInstanceOf[Option[Blockchains]].get
        new BalanceSourceChain(rpc, snapshotTs)
      case "dune" =>
        val apiKey = DetectorConfig.getString(rx.conf,"dune_api_key","")
        new BalanceSourceDune(apiKey, snapshotTs)
      case _ =>
        log.error(s"${rx.getExtId()}: invalid source: '${source}'")
        return SentryRun.SENTRY_STOPPED
    }

    rx.set("balance_source",balanceSource)

    rx.set("source",source)
    rx.set("currency",DetectorConfig.getString(rx.conf,"currency",DetectorKuba.DEF_CURRENCY))

    // Timezone configuration
    val timezoneStr = DetectorConfig.getString(rx.conf,"timezone",DetectorKuba.DEF_TIMEZONE)
    val timezone = DetectorKuba.parseTimezone(timezoneStr)
    rx.set("timezone",timezone)

    // Optional: User-specified snapshot timestamp (in configured timezone)
    // If not specified, will use current time when cron triggers
    val snapshotTimestampStr = DetectorConfig.getString(rx.conf,"snapshot_timestamp","")
    if(snapshotTimestampStr.nonEmpty) {
      try {
        val snapshotTs = DetectorKuba.parseTimestamp(snapshotTimestampStr, timezone)
        rx.set("snapshot_timestamp",Some(snapshotTs))
        log.info(s"${rx.getExtId()}: snapshot_timestamp=${snapshotTimestampStr} (${snapshotTs} ms)")
      } catch {
        case e: Exception =>
          log.error(s"${rx.getExtId()}: invalid snapshot_timestamp format: '${snapshotTimestampStr}': ${e.getMessage()}")
          return SentryRun.SENTRY_STOPPED
      }
    } else {
      rx.set("snapshot_timestamp",None)
    }

    // CSV output path configuration
    val outputCsv = DetectorConfig.getString(rx.conf,"output_csv",DetectorKuba.DEF_OUTPUT_CSV)
    rx.set("output_csv",outputCsv)
    if(outputCsv.nonEmpty) {
      log.info(s"${rx.getExtId()}: CSV output enabled: ${outputCsv}")
    }

    // CSV preview in Event metadata
    val csvPreview = DetectorConfig.getInt(rx.conf,"csv_preview",DetectorKuba.DEF_CSV_PREVIEW)
    rx.set("csv_preview",csvPreview)
    log.info(s"${rx.getExtId()}: CSV preview lines in Event: ${csvPreview}")

    // Balance query throttling configuration
    val balanceQueryDelayMs = DetectorConfig.getLong(rx.conf,"balance_delay",DetectorKuba.DEF_BALANCE_QUERY_DELAY_MS)
    rx.set("balance_delay",balanceQueryDelayMs)
    log.info(s"${rx.getExtId()}: balance query delay: ${balanceQueryDelayMs}ms")

    SentryRun.SENTRY_RUNNING
  }

  def start(rx:SentryRun,job:Job):Seq[Event] = {
    log.info(s"${rx.getExtId()}: [START]: ${job}")
    
    val balanceSource = rx.get("balance_source") match {
      case Some(bs: BalanceSource) => bs
      case _ =>
        log.warn(s"${rx.getExtId()}: balance source undefined")
        return Seq.empty
    }
    val currency = rx.get("currency") match {
      case Some(c:String) => c
      case Some(cOpt: Option[_]) => cOpt.asInstanceOf[Option[String]].getOrElse(DetectorKuba.DEF_CURRENCY)
      case _ => DetectorKuba.DEF_CURRENCY
    }
    val users = rx.get("users") match {
      case Some(us: Seq[_]) => us.asInstanceOf[Seq[User]]
      case _ =>
        log.warn(s"${rx.getExtId()}: users undefined")
        return Seq.empty
    }
    val tokens = rx.get("tokens") match {
      case Some(ts: Seq[_]) => ts.asInstanceOf[Seq[Token]]
      case _ =>
        log.warn(s"${rx.getExtId()}: tokens undefined")
        return Seq.empty
    }
    if(users.isEmpty || tokens.isEmpty) {
      log.warn(s"${rx.getExtId()}: empty users or tokens")
      return Seq.empty
    }
    
    // Use configured snapshot timestamp if provided, otherwise use job start time
    val ts = rx.get("snapshot_timestamp") match {
      case Some(Some(snapshotTs: Long)) => snapshotTs
      case Some(snapshotTs: Long) => snapshotTs
      case _ => job.ts
    }

    val timezone = rx.get("timezone") match {
      case Some(tz: ZoneId) => tz
      case _ => DetectorKuba.HONG_KONG_TZ
    }

    log.info(s"${rx.getExtId()}: snapshot(${ts}) (${DetectorKuba.formatTimestamp(ts, timezone)})")

    // Get throttling delay
    val delayMs = rx.get("balance_delay") match {
      case Some(d: Long) => d
      case _ => DetectorKuba.DEF_BALANCE_QUERY_DELAY_MS
    }

    var errors = 0L

    // Block heights are already calculated and cached in BalanceSource during construction
    val balances: Seq[Balance] = users.flatMap { user =>
      tokens
        .filter(token => token.bid == user.chain)
        .map { token => {

          val balance = balanceSource.getBalance(user.addr, user.chain, token, None) match {
              case Success(balance: Balance) =>
              val humanAmount =  TokenUtil.toHuman(balance.amount.toDouble)            
              log.info(s"${rx.getExtId()}: ${user.addr}: chain=${user.chain}, token=${token.addr} (${token.sym}): ${balance.value} (${humanAmount})")

              // Throttle to avoid overwhelming RPC endpoints
              if (delayMs > 0) {
                Thread.sleep(delayMs)
              }

              balance
            case Failure(e) =>
              errors = errors + 1
              log.warn(s"${rx.getExtId()}: ${user.addr}: chain=${user.chain}, token=${token.addr} (${token.sym}): failed to get balance: ${e.getMessage()}")
              Balance(user.addr, user.chain, token.sym, token.addr, BigInt(0), 0.0, None, ts, Some(e.getMessage()))
          }        

          balance
        }}
    }

    // Generate CSV output if configured
    val outputCsv = rx.get("output_csv") match {
      case Some(path: String) if path.nonEmpty => Some(path)
      case _ => None
    }

    // Get chain mapping for CSV output
    val chainMapping = rx.get("chain_mapping") match {
      case Some(m: Map[_, _]) => m.asInstanceOf[Map[String, String]]
      case _ => Map.empty[String, String]
    }

    val csv = DetectorKuba.balancesToCsv(balances, timezone, chainMapping)

    val csvPath = outputCsv.map { path =>      
      DetectorKuba.saveCsv(csv, path) match {
        case Success(savedPath) =>
          log.info(s"${rx.getExtId()}: CSV saved to: ${savedPath} (${balances.size} rows)")
          savedPath
        case Failure(e) =>
          log.error(s"${rx.getExtId()}: failed to save CSV: ${e.getMessage()}")
          ""
      }
    }

    // Check if CSV preview should be included in Event
    val csvPreviewLines = rx.get("csv_preview") match {
      case Some(n: Int) => n
      case _ => DetectorKuba.DEF_CSV_PREVIEW
    }

    // Take specified number of lines from CSV for preview
    val csvPreviewData = if (csvPreviewLines > 0) {
      val lines = csv.split("\n")
      val previewLines = lines.take(csvPreviewLines)
      Some(previewLines.mkString("\n"))
    } else {
      None
    }

    val metadata = Map(
      "mappings" -> chainMapping.size.toString,
      "tokens" -> tokens.size.toString,
      "job_id" -> job.id,
      "job_ts" -> job.ts.toString,
      "snapshot_ts" -> ts.toString,
      "timezone" -> timezone.getId,
      "snapshot_time" -> DetectorKuba.formatTimestamp(ts, timezone),
      "users" -> users.size.toString,
      "balances" -> balances.size.toString,
      "errors" -> errors.toString,
      "csv" -> csvPath.getOrElse("")
    ) ++ (csvPreviewData.map(preview => Map("csv_preview" -> preview)).getOrElse(Map.empty))

    val ee = Seq(EventUtil.createEvent(
      did,tx = None,None,conf = Some(rx.getConf()),
      meta = metadata,
      sev = Some(DetectorKuba.DEF_SEV_OK)
    ))


    log.info(s"${rx.getExtId()}: [FINISH]: ${job}")
    ee
  }

  override def onCron(rx:SentryRun,elapsed:Long):Seq[Event] = {
    val newJob = this.synchronized {
      job match {
        case Some(job) =>
          log.warn(s"${rx.getExtId()}: job=${job}: still running")
          None
        case None =>
          val job0 = Job(UUID.randomUUID().toString,System.currentTimeMillis())          
          job = Some(job0)
          rx.set("job",job0)
          Some(job0)
      }
    }

    newJob match {
      case None => Seq.empty
      case Some(job0) =>
        try {
          start(rx,job0)
        } finally {
          this.synchronized {
            job match {
              case Some(current) if current.id == job0.id => job = None
              case _ => ()
            }
          }
        }
    }
  }
    
}

