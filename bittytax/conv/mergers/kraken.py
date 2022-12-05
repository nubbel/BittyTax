# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2021

import copy
from decimal import Decimal
import sys

from colorama.ansi import Back, Fore
from bittytax.conv.exceptions import UnexpectedContentError

from bittytax.conv.out_record import TransactionOutRecord
from bittytax.conv.parsers import kraken
from ..datamerge import DataMerge
from bittytax.conv.parsers.kraken import STAKING_WALLET, kraken_ledgers, kraken_trades


def merge_kraken(data_files):
    return merge_trades(data_files['ledgers'], data_files['trades'])

def merge_trades(ledgers_file, trades_file):
    merge = True

    for data_row in ledgers_file.data_rows:
        if not data_row.t_record or data_row.parsed:
            continue
        
        if data_row.t_record.t_type != TransactionOutRecord.TYPE_TRADE:
            continue

        data_row.parsed = True

        matching_row = find_matching_row(
            ledgers_file.data_rows,
            line_num=data_row.line_num,
            refid=data_row.row_dict['refid'])

        trade_row = find_trade_row(trades_file.data_rows, data_row.row_dict['refid'])
        if trade_row:
            sys.stderr.write("%sfound matching trade (%s) for ledger (%s)\n" % (
                Fore.LIGHTGREEN_EX,
                trade_row.row_dict['txid'], data_row.row_dict['txid']))
            trade_row.parsed = True

        if not matching_row:
            # check if it's a weird remainder trade (small quantities)
            if trade_row and trade_row.row_dict['ledgers'] == data_row.row_dict['txid']:
                data_row.t_record = trade_row.t_record
                data_row.t_record.note = "remainder"
                
                sys.stderr.write("%sremainder TR imported from trades (%s)\n" % (
                    Fore.GREEN, trade_row.row_dict['txid']))
            else:
                sys.stderr.write("%sWARNING%s Merge failure for refid: %s\n" % (
                    Back.YELLOW+Fore.BLACK, Back.RESET+Fore.YELLOW, data_row.row_dict['refid']))

                data_row.failure = UnexpectedContentError(
                    ledgers_file.parser.in_header.index('refid'),
                    'refid', data_row.row_dict['refid'])
            
                merge = False
            continue

        if data_row.t_record.buy_asset:
            assert(matching_row.t_record.sell_asset)
            assert(matching_row.t_record.sell_quantity)

            data_row.t_record.sell_asset = matching_row.t_record.sell_asset
            data_row.t_record.sell_quantity = matching_row.t_record.sell_quantity
        else:
            assert(matching_row.t_record.buy_asset)
            assert(matching_row.t_record.buy_quantity)

            data_row.t_record.buy_asset = matching_row.t_record.buy_asset
            data_row.t_record.buy_quantity = matching_row.t_record.buy_quantity

        if matching_row.t_record.fee_quantity > 0:
            # handle multiple fees
            if data_row.t_record.fee_quantity > 0:
                sys.stderr.write("%sWARNING%s Multiple fees: %s (%s%s and %s%s)\n" % (
                    Back.YELLOW+Fore.BLACK, Back.RESET+Fore.YELLOW, data_row.row_dict['refid'],
                    data_row.t_record.fee_quantity, data_row.t_record.fee_asset,
                    matching_row.t_record.fee_quantity, matching_row.t_record.fee_asset
                    ))

                matching_row.t_record.buy_quantity = Decimal(0)
                matching_row.t_record.buy_asset = data_row.t_record.buy_asset

                matching_row.t_record.sell_quantity = Decimal(0)
                matching_row.t_record.sell_asset = data_row.t_record.sell_asset

                matching_row.t_record.note = "fee only"

                # mark to avoid that the TR gets removed
                matching_row.parsed = True

                sys.stderr.write("%sfee only TR created: %d and %d\n" % (Fore.GREEN, data_row.line_num, matching_row.line_num))
                
            else:
                data_row.t_record.fee_asset = matching_row.t_record.fee_asset
                data_row.t_record.fee_quantity = matching_row.t_record.fee_quantity

        sys.stderr.write("%sfound match: %d and %d\n" % (Fore.GREEN, data_row.line_num, matching_row.line_num))

        # Remove TR as it was now merged
        if not matching_row.parsed:
            matching_row.t_record = None
            matching_row.parsed = True

    # remove all matched TRs from trades
    for data_row in trades_file.data_rows:
        if data_row.parsed:
            data_row.t_record = None

    virtual_trades = []
    for data_row in ledgers_file.data_rows:
        if (data_row.parsed or not data_row.t_record or 
            data_row.t_record.wallet != STAKING_WALLET or
            data_row.t_record.t_type != TransactionOutRecord.TYPE_DEPOSIT or 
            data_row.t_record.buy_asset != 'ETH2'):
            continue

        data_row.t_record.buy_asset = 'ETH'

        trade = copy.copy(data_row)
        trade.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                 trade.timestamp,
                                                 buy_quantity=data_row.t_record.buy_quantity,
                                                 buy_asset='ETH2',
                                                 sell_quantity=data_row.t_record.buy_quantity,
                                                 sell_asset='ETH',
                                                 wallet=data_row.t_record.wallet,
                                                 note='conversion')
        virtual_trades.append(trade)
    
    ledgers_file.data_rows += virtual_trades

    return merge

def find_matching_row(data_rows, line_num, refid):
    # usually the matching row is the next row, so let's try that one first
    # note that line_num is 1-indexed so data_rows[line_num] is the next row
    if line_num < len(data_rows) and is_matching_row(data_rows[line_num], line_num, refid):
        return data_rows[line_num]

    for data_row in data_rows:
        if is_matching_row(data_row, line_num, refid):
            return data_row

    return None

def is_matching_row(data_row, line_num, refid):
    return (not data_row.parsed and 
        data_row.line_num != line_num and 
        data_row.t_record and 
        data_row.row_dict['refid'] == refid)

def find_trade_row(data_rows, refid):
    for data_row in data_rows:
        if not data_row.parsed and data_row.row_dict['txid'] == refid:
            return data_row

    return None

DataMerge("Kraken T",
          {'ledgers': {'req': DataMerge.MAN, 'obj': kraken_ledgers},
              'trades': {'req': DataMerge.MAN, 'obj': kraken_trades}},
          merge_kraken)
