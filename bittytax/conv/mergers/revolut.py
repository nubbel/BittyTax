# -*- coding: utf-8 -*-

import sys
import copy

import sys
from decimal import Decimal

from colorama import Fore, Back

from ...config import config
from ..out_record import TransactionOutRecord
from ..datamerge import DataMerge
from ..exceptions import UnexpectedContentError
from ..parsers.revolut import revolut_txns

PRECISION = Decimal('0.' + '0' * 18)


def merge_revolut(data_files):
    merge = False

    tx_groups = {}
    for dr in data_files['txns'].data_rows:
        if not dr.t_record or dr.parsed or dr.row_dict['Type'] != 'EXCHANGE':
            continue

        g_id = "[%s] %s" % (dr.timestamp, dr.t_record.note)

        if g_id in tx_groups:
            tx_groups[g_id].append(dr)
        else:
            tx_groups[g_id] = [dr]

    for g_id in tx_groups:
        txns = tx_groups[g_id]

        if len(txns) == 1:
            if txns[0].t_record.buy_asset in config.fiat_list or txns[0].t_record.sell_asset in config.fiat_list:
                # we don't care about fiat moving around
                txns[0].t_record = None
            else:
                sys.stderr.write("%sWARNING%s Merge failure for group (just one TR): %s\n" % (
                    Back.YELLOW+Fore.BLACK, Back.RESET+Fore.YELLOW, g_id))
            continue

        if len(txns) > 2:
            sys.stderr.write("%sWARNING%s Merge failure for group (more than 2 TR): %s\n" % (
                Back.YELLOW+Fore.BLACK, Back.RESET+Fore.YELLOW, g_id))
            return merge


        if (txns[0].t_record.t_type == TransactionOutRecord.TYPE_DEPOSIT and 
            txns[1].t_record.t_type == TransactionOutRecord.TYPE_WITHDRAWAL):
            deposit, withdrawal = txns
        elif (txns[1].t_record.t_type == TransactionOutRecord.TYPE_DEPOSIT and 
            txns[0].t_record.t_type == TransactionOutRecord.TYPE_WITHDRAWAL):
            withdrawal, deposit = txns
        else:
            sys.stderr.write("%sWARNING%s Merge failure for group (expected exactly 1 deposit and withdrawal each): %s\n" % (
                Back.YELLOW+Fore.BLACK, Back.RESET+Fore.YELLOW, g_id))
            return merge

        if (deposit.t_record.buy_asset in config.fiat_list and 
            withdrawal.t_record.sell_asset in config.fiat_list):
            # we're not interested in fiat-to-fiat trades
            deposit.t_record = withdrawal.t_record = None
            merge = True
            continue

        if deposit.t_record.buy_asset == withdrawal.t_record.sell_asset:
            # just an internal transfer, not a trade
            continue

        deposit.t_record.t_type = TransactionOutRecord.TYPE_TRADE
        deposit.t_record.sell_quantity = withdrawal.t_record.sell_quantity
        deposit.t_record.sell_asset = withdrawal.t_record.sell_asset

        if withdrawal.t_record.fee_quantity > 0:
            deposit.t_record.fee_quantity += withdrawal.t_record.fee_quantity
            deposit.t_record.fee_asset = withdrawal.t_record.fee_asset

        # TR was merged so we can remove it now
        withdrawal.t_record = None

        merge = True

    return merge


DataMerge("revolut",
          {'txns': revolut_txns},
          merge_revolut)
