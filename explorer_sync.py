#!/usr/bin/env python3

import argparse
import jsmin
import json
import logging
import logging.handlers
import os
import pymongo
import shutil
import subprocess
import time

from bitcoinrpc.authproxy import AuthServiceProxy
from configobj import ConfigObj


parser = argparse.ArgumentParser(description='explorer sync parameters')
parser.add_argument('--explorer-config', dest='explorer_config', type=str,
                    help='explorer config file', required=True)
parser.add_argument('--log-level', dest='loglevel', type=str, default="INFO",
                    help='set log level',
                    choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"])
parser.add_argument('--log-file', dest='logfile', type=str,
                    help='log file location')


NUM_UNITS = 100000000


class ReorgException(Exception):
    pass


def get_explorer_config(cfg_file):
    if os.path.isfile(cfg_file) is False:
        raise IOError("Config file %s not found" % cfg_file)

    with open(cfg_file, 'r') as fp:
        return json.loads(jsmin.jsmin(fp.read()))


class Database(object):

    def __init__(self, cfg, coin):
        self._coin = coin
        db_cfg = self._validate_db_cfg(cfg["dbsettings"])
        self._db_uri = 'mongodb://%s:%s@%s:%s/%s' % (
            db_cfg[0], db_cfg[1], db_cfg[2], db_cfg[3], db_cfg[4])
        self._db_conn = pymongo.MongoClient(self._db_uri)
        self.db = self._db_conn[db_cfg[4]]
        self._ensure_collections_and_indexes()
        self._txcount = cfg.get("txcount", 200)

    def _validate_db_cfg(self, cfg):
        database = cfg.get("database")
        user = cfg.get("user")
        password = cfg.get("password")
        addr = cfg.get("address")
        port = cfg.get("port")
        if None in (database, user, password, addr, port):
            raise ValueError("Invalid mongo config")
        return (user, password, addr, port, database)

    def get_stats(self):
        stats = self.db.coinstats.find_one({"coin": self._coin})
        if stats is None:
            return None
        return stats

    def get_last_recorded_block(self):
        count = self.db.blocks.find().count()
        if count == 0:
            return None
        record = self.db.blocks.find().sort(
            [("height", pymongo.DESCENDING)]).limit(1)
        return record[0]

    def _process_vout(self, vouts, txid, addrs):
        if type(addrs) is not dict:
            raise ValueError("Invalid addrs dics")
        for out in vouts:
            address = out["addresses"]
            amount = out["amount"]
            if addrs.get(address) is None:
                addrs[address] = {
                    "received": amount,
                    "sent": 0,
                    "txs": [
                        {
                            "type": "vout",
                            "addresses": txid
                        }
                    ]
                }
            else:
                details = addrs[address]
                details["received"] += amount
                details["txs"].append({
                    "type": "vin",
                    "addresses": txid
                })
                addrs[address] = details
        return addrs

    def _process_vin(self, vins, txid, addrs):
        if type(addrs) is not dict:
            raise ValueError("Invalid addrs dics")
        for vin in vins:
            address = vin["addresses"]
            amount = vin["amount"]
            if addrs.get(address) is None:
                addrs[address] = {
                    "sent": amount,
                    "received": 0,
                    "txs": [
                        {
                            "type": "vin",
                            "addresses": txid
                        }
                    ]
                }
            else:
                details = addrs[address]
                details["sent"] += amount
                details["txs"].append({
                    "type": "vin",
                    "addresses": txid
                })
                addrs[address] = details
        return addrs

    def get_address_info(self, address):
        addr = self.db.addresses.find_one({"a_id": addr})
        if addr is None:
            raise ValueError("No such address %s" % address)
        return addr

    def _prepare_ins_outs(self, transactions):
        addrs = {}
        for tx in transactions:
            vout = tx["vout"]
            vin = tx["vin"]
            addrs = self._process_vout(vout, tx["txid"], addrs)
            addrs = self._process_vin(vin, tx["txid"], addrs)
        return addrs

    def update_addresses(self, transactions):
        addrs = self._prepare_ins_outs(transactions)
        for addr in addrs:
            info = self.db.addresses.find_one({"a_id": addr})
            if info:
                sent = info.get("sent", 0) + addrs[addr].get("sent", 0)
                received = info.get("received", 0) + addrs[addr].get("received", 0)
                txs = info.get("txs", [])
                for tx in addrs[addr].get("txs", []):
                    if tx in txs:
                        continue
                    txs.append(tx)
                balance = received - sent
                self.db.addresses.update_one(
                    {"a_id": addr},
                    {
                        "$set": {
                            "sent": sent,
                            "received": received,
                            "balance": balance,
                            "txs": txs[-self._txcount:],
                        }
                    })
            else:
                sent = addrs[addr].get("sent", 0)
                received = addrs[addr].get("received", 0)
                txs = addrs[addr].get("txs", [])
                balance = received - sent
                self.db.addresses.insert_one(
                    {
                        "a_id": addr,
                        "sent": sent,
                        "received": received,
                        "balance": balance,
                        "txs": txs[-self._txcount:],
                    }
                )

    def rollback_addresses(self, transactions):
        if type(transactions) is not list:
            raise ValueError("transactions object must be list")
        addrs = self._prepare_ins_outs(transactions)
        for addr in addrs:
            info = self.db.addresses.find_one({"a_id": addr})
            if info is None:
                continue
            sent = info.get("sent", 0) - addrs[addr].get("sent", 0)
            received = info.get("received", 0) - addrs[addr].get("received", 0)
            for tx in addrs[addr].get("txs", []):
                if tx in info.get("txs", []):
                    info["txs"].remove(tx)
            balance = received - sent
            self.db.addresses.update_one(
                {"a_id": addr},
                {
                    "$set": {
                        "sent": sent,
                        "received": received,
                        "balance": balance,
                        "txs": info["txs"],
                    }
                })

    def rollback(self, blockhash):
        """Rollback changes made for a particular blockhash"""
        txs = self.db.txes.find({"blockhash": blockhash})
        transactions = []
        if txs:
            for i in txs:
                transactions.append(i)
        self.rollback_addresses(transactions)
        self.db.txes.delete_many({"blockhash": blockhash})
        self.db.blocks.delete_many({"hash": blockhash})

    def update_transactions(self, transactions):
        self.db.txes.insert_many(transactions)

    def update_richlist(self):
        balance = list(self.db.addresses.find().sort(
            [("balance", pymongo.DESCENDING)]).limit(100))
        received = list(self.db.addresses.find().sort(
            [("received", pymongo.DESCENDING)]).limit(100))
        self.db.richlists.update_one(
            {"coin": self._coin},
            {
                "$set": {
                    "balance": balance,
                    "received": received,
                }
            }, upsert=True)

    def update_stats(self, stats):
        if type(stats) is not dict:
            raise ValueError("Invalid stats object")
        supply = stats.get("supply")
        lastHeight = stats.get("block")
        count = stats.get("count")
        if None in (supply, lastHeight, count):
            # Invalid stats given. Should log something here
            raise ValueError("Invalid stats object")
        self.db.coinstats.find_one_and_update(
            {"coin": self._coin},
            {
                "$set": {
                    "supply": supply,
                    "last": lastHeight,
                    "count": count,
                 }
            })

    def _ensure_collections_and_indexes(self):
        names = self.db.list_collection_names()
        if "blocks" not in names:
            self.db.create_collection("blocks")
            self.db.blocks.create_index("height", unique=True)
            self.db.blocks.create_index("hash")
        if "txes" in names:
            self.db.txes.create_index("blockhash")


class TxIn(object):

    def __init__(self, txin, version):
        self._in = txin
        self._vers = version

    def is_valid(self):
        script = self._in.get("scriptSig", {})
        asm = script.get("asm")
        if asm:
            if asm.startswith("OP_RETURN"):
                return False
        return True

    def is_coinbase(self):
        return self._in.get("coinbase", False) is not False

    def input(self):
        if self.is_coinbase():
            return None
        return {"vout": self._in["vout"], "txid": self._in["txid"]}


class Tx(object):

    def __init__(self, tx, cli, height):
        self._tx = tx
        self._cli = cli
        self._height = height
        self._vin = None
        self._vout = None

    def tx_id(self):
        return self._tx["txid"]

    def _output_is_valid(self, out):
        script = out.get("scriptPubKey", None)
        if script is None:
            return False
        asm = script.get("asm")
        if asm is None or asm.startswith("OP_RETURN"):
            return False
        return True

    def _get_input_details(self, vinInfo):
        vin = self._cli.get_transaction(vinInfo["txid"])
        voutIdx = vinInfo.get("vout")
        vouts = vin.get("vout")
        if vouts is None:
            return
        for i in vouts:
            n = i.get("n")
            if n is not None and int(n) == int(vinInfo["vout"]):
                scrypt = i.get("scriptPubKey")
                if scrypt is None:
                    return
                addr = scrypt.get("addresses")
                if addr is None:
                    return
                return {
                    "addresses": addr[0],
                    "amount": int(i["value"] * NUM_UNITS),
                }
        return

    def _get_coinbase_vin(self):
        vout = self._tx.get("vout")
        total = sum([int(i["value"] * NUM_UNITS) for i in vout])
        return {
            "addresses": "coinbase",
            "amount": total,
        }

    def _is_coinbase(self):
        vin = self._tx["vin"]
        tx = TxIn(vin[0], self._tx["version"])
        return tx.is_coinbase()

    def inputs(self):
        if self._vin:
            return self._vin
        addr_map = {}
        for i in self._tx.get("vin"):
            tx = TxIn(i, self._tx["version"])
            if tx.is_coinbase():
                details = self._get_coinbase_vin()
                return [details,]
            if tx.is_valid() is False:
                continue

            details = self._get_input_details(tx.input())
            if addr_map.get(details["addresses"]) is None:
                addr_map[details["addresses"]] = details["amount"]
            else:
                addr_map[details["addresses"]] += details["amount"]

        ret = [{"addresses": k, "amount": v} for k, v in addr_map.items()]
        self._vin = ret
        return ret

    def outputs(self):
        if self._vout:
            return self._vout
        ret = []
        vout = self._tx.get("vout", [])
        if len(vout) == 0:
            return ret
        tx = vout[0]

        is_nonstandard = tx["scriptPubKey"]["type"] == "nonstandard"
        is_stake = False

        if is_nonstandard:
            vout.pop(0)
            if len(vout) == 0:
                return ret
            addr = vout[0]["scriptPubKey"].get("addresses", [""])[0]
            vin = self.inputs()
            if len(vin):
                is_stake = addr == vin[0]["addresses"]

        addrs = {}

        for i in vout:
            if self._output_is_valid(i) is False:
                continue
            script = i.get("scriptPubKey", {})
            addr = script["addresses"][0]
            if addrs.get(addr):
                addrs[addr] += int(i["value"] * NUM_UNITS)
            else:
                addrs[addr] = int(i["value"] * NUM_UNITS)

        for addr in addrs:
            if is_stake:
                val = addrs[addr] - vin[0]["amount"]
            else:
                val = addrs[addr]
            ret.append(
                {
                    "amount": val,
                    "addresses": addr,
                    #"type": script.get("type"),
                    "is_stake": is_stake,
                }
            )
        self._vout = ret
        return ret

    def _get_total(self, vin, vout, is_coinbase):
        voutTotal = sum([i["amount"] for i in vout])
        if is_coinbase:
            vinAmount = vin[0]["amount"]
            if vinAmount != voutTotal:
                raise ValueError("Coinbase amount != vout Amount")
            return vinAmount

        vinTotal = sum([i["amount"] for i in vin])

        fee = (vinTotal - voutTotal)
        amount = vinTotal - fee 
        return amount

    def details(self):
        is_coinbase = self._is_coinbase()
        outs = self.outputs()
        ins = self.inputs()
        total = self._get_total(ins, outs, is_coinbase)
        if len(outs) and outs[0]["is_stake"]:
            ins = []

        for i in outs:
            if i.get("is_stake") is not None:
                del i["is_stake"]
        ret = {
            "vin": ins,
            "vout": outs,
            "txid": self.tx_id(),
            "total": total,
            "is_coinbase": is_coinbase, 
            "timestamp": self._tx["time"],
        }
        return ret


class Daemon(object):

    def __init__(self, cfg):
        self._cfg_path = cfg
        self._explorer_cfg = get_explorer_config(self._cfg_path)
        self._cfg = self._explorer_cfg["wallet"]
        (self._addr, self._port,
         self._user, self._password) = self._validate_daemon_cfg(self._cfg)
        self._url = "http://%s:%s@%s:%s" % (self._user, self._password,
                                            self._addr, self._port)
        self._conn = AuthServiceProxy(self._url)
        self._db = Database(self._explorer_cfg, self._explorer_cfg["coin"])
        self._set_cwd()

    def _get_explorer_working_directory(self):
        here = os.path.abspath(os.path.dirname(self._cfg_path))
        return here

    def _set_cwd(self):
        wd = self._get_explorer_working_directory()
        os.chdir(wd)

    def _validate_daemon_cfg(self, cfg):
        addr = cfg.get("host")
        port = cfg.get("port")
        user = cfg.get("user")
        password = cfg.get("pass")
        if None in (addr, port, user, password):
            raise ValueError("Invalid config")
        return addr, port, user, password

    def call_method(self, method, *args):
        meth = getattr(self._conn, method)
        return meth(*args)

    def blockchain_height(self):
        besthash = self.call_method("getbestblockhash")
        blk = self.call_method("getblock", besthash)
        return blk["height"]

    def get_block_hash(self, height):
        return self.call_method("getblockhash", height)

    def get_block(self, blkHash):
        blkDetails = self.call_method("getblock", blkHash, True)
        return blkDetails

    def get_block_at_height(self, height):
        blkhash = self.get_block_hash(height)
        return self.get_block(blkhash)

    def get_transaction(self, txid):
        tx = self.call_method("getrawtransaction", txid, 1)
        return tx

    def _get_coin_supply_coinbase(self):
        sent = self._db.get_address_info("coinbase")["sent"]
        return sent / NUM_UNITS 

    def _get_coin_supply_getinfo(self):
        return self.call_method("getinfo")["moneysupply"] 

    def get_coin_supply(self):
        supply_source = self._explorer_cfg.get("supply")
        if supply_source is None:
            return 0
        meth = getattr(self, "_get_coin_supply_%s" % supply_source.lower())
        if meth is None:
            raise ValueError(
                "No method to get coin supply for %s" % supply_source)
        return float(meth())

    def get_block_transactions(self, blk):
        transactions = []
        trx = blk.get("tx", [])
        if len(trx) == 0:
            return transactions
        
        for tx in trx:
            tpayTx = Tx(tx, self, blk["height"])
            details = tpayTx.details()
            txInfo = {
                "txid" : details["txid"],
                "blockhash" : blk["hash"],
                "blockindex" : blk["height"],
                "timestamp" : details["timestamp"],
                "total" : details["total"],
                "vout" : details["vout"],
                "vin" : details["vin"],
                "__v" : 0
            } 
            transactions.append(txInfo)
        return transactions

    def _wait_for_blockchain_sync(self):
        chain_height = self.blockchain_height()
        stats = self._db.get_stats()
        while int(stats["last"]) > int(chain_height):
            chain_height = self.blockchain_height()
            stats = self.db.get_stats()
            time.sleep(1)

    def _prepare_block(self, block):
        if block["height"] == 0:
            # Genesis
            prevhash = None
        else:
            prevhash = block["previousblockhash"]
        return {
            "height": block["height"],
            "hash": block["hash"],
            "prevhash": prevhash,
            "tx": [i["txid"] for i in block["tx"]],
        }


    def _ensure_blocks_collection_in_sync(self, last_height):
        """We added this collection. The explorer app
        does not have it. So we sync blocks up to the
        height the explorer managed to sync previously"""
        if last_height <= 1:
            return

        if last_height < 2000:
            start_block = 1
        else:
            start_block = last_height - 2000

        self._db.db.blocks.remove({})
        toInsert = []

        for i in range(start_block, last_height + 1):
            block = self.get_block_at_height(i)
            toInsert.append(self._prepare_block(block))
            if len(toInsert) >= 1000 or i == last_height:
                # flush to db
                logger.info(
                    "Flushing at height %s. "
                    "Chain height: %s" % (block["height"], last_height))
                self._db.db.blocks.insert_many(toInsert)
                toInsert = []

    def _update_stats(self, height, supply):
        stats = {
            "supply": supply,
            "block": height,
            "count": height - 1,
        }
        self._db.update_stats(stats)

    def _process_blocks(self):
        stats = self._db.get_stats()
        chain_height = self.blockchain_height()
        if int(stats["last"]) == int(chain_height):
            return
        diff = int(chain_height) - int(stats["last"])
        last_blk = self._db.get_last_recorded_block()
        last_height = stats["last"]
        logger.info("Last height is %d" % last_height)
        coin_supply = self.get_coin_supply()
        blks = []
        txes = []
        if last_height > 1:
            last_height += 1
        while last_height <= chain_height:
            blk = self.get_block_at_height(last_height)
            prev_blk = blk.get("previousblockhash")
            if last_blk and last_blk["hash"] != prev_blk:
                logger.info(
                    "Recorded block (height: %s) hash: %s "
                    "Current chain block (height: %s) hash: %s" % (
                        last_blk["height"], last_blk["hash"],
                        blk["height"], prev_blk))
                # chain reorg detected
                logger.info(
                    "Reorg detected. Rolling back "
                    "block %s" % last_blk["height"])
                self._db.rollback(last_blk["hash"])
                self._update_stats(last_blk["height"] - 1, coin_supply)
                raise ReorgException("Chain reorg detected")
            blks.append(self._prepare_block(blk))
            txes.extend(self.get_block_transactions(blk))
            if last_height % 1000 == 0 or last_height == chain_height:
                logger.info("flushing at block %r" % blk["height"])
                self._db.db.blocks.insert_many(blks)
                self._db.update_transactions(txes)
                self._db.update_addresses(txes)
                self._update_stats(blk["height"], coin_supply)
                self._db.update_richlist()
                blks = []
                txes = []
            if last_height % 4000 == 0:
                logger.info(
                    "Prunning blocks collection older "
                    "than: %d" % (last_height - 2000))
                self._db.db.blocks.remove(
                    {"height": {"$lt": last_height - 2000}})
            last_blk = blk
            last_height += 1
        self._db.update_richlist()

    def _has_node(self):
        return shutil.which("node") is not None

    def _run_peers_sync(self):
        peers = "scripts/peers.js"
        if self._has_node:
            try:
                subprocess.check_call(
                    ["node", peers], stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL)
            except Exception as err:
                logger.error("Failed to sync peers: %s" % err)
        else:
            logger.warning("nodejs not found. Skipping peers sync")
    
    def _run_markets_sync(self):
        sync = "scripts/sync.js"
        if self._has_node:
            try:
                subprocess.check_call(
                    ["node", sync, "market"], stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL)
            except Exception as err:
                logger.error("Failed to sync markets: %s" % err)
        else:
            logger.warning("nodejs not found. Skipping market sync")

    def run(self):
        stats = self._db.get_stats()
        self._wait_for_blockchain_sync()
        self._ensure_blocks_collection_in_sync(stats["last"])
        while True:
            try:
                logger.info("Processing blocks")
                self._process_blocks()
                logger.info("Updating peers information")
                self._run_peers_sync()
                logger.info("Updating markets information")
                self._run_markets_sync()
            except ReorgException:
                continue
            except Exception as err:
                logger.exception("got exception processing blocks")
            time.sleep(10)


if __name__ == "__main__":
    args = parser.parse_args()
    numeric_level = getattr(logging, args.loglevel, logging.INFO)
    log_format = '%(asctime)s %(name)s %(levelname)s %(message)s'
    logger = logging.getLogger('sync')
    logger.setLevel(numeric_level)
    formatter = logging.Formatter(log_format)
    if args.logfile:
        handler = logging.handlers.RotatingFileHandler(
              args.logfile, maxBytes=100*1024*1024, backupCount=2)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    daemon = Daemon(args.explorer_config)
    daemon.run()
