# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2021

import sys
import copy

from decimal import Decimal

from colorama import Fore, Back

from ...config import config
from ..out_record import TransactionOutRecord
from ..datamerge import DataMerge, MergeDataRow
from ..exceptions import UnexpectedContentError
from ..parsers.etherscan import etherscan_txns, etherscan_tokens, etherscan_internal_txns, get_note

PRECISION = Decimal('0.' + '0' * 18)

TXNS = 'txn'
TOKENS = 'token'
NFTS = 'nft'
INTERNAL_TXNS = 'int'

def merge_etherscan(data_files):
    return do_merge_etherscan(data_files, {
        # SnowSwap StakingRewards
        '0x86d0e7ad012bf743b660235a531dfc45608f4dc5': [
            # SNOW
            '0xfe9a29ab92522d14fc65880d817214261d8479ae'
        ], 
        # Bancor StakingRewards
        '0x457fe44e832181e1d3ecee0fc5be72cd9b36859f':[
            # vBNT
            '0x48fb253446873234f2febbf9bdeaa72d9d387f94'
        ], 
        # Bancor StakingRewards
        '0xb443dea978b39178cb05ae005074227a4390dfce':[
            # vBNT
            '0x48fb253446873234f2febbf9bdeaa72d9d387f94'
        ],
    })

def do_merge_etherscan(data_files, staking_addresses):
    merge = False
    tx_ids = {}

    for file_id in data_files:
        for dr in data_files[file_id].data_rows:
            if not dr.t_record:
                continue

        t_internal_txns = find_tx_tokens(data_files['internal_txns'].data_rows,
                                   data_row.row_dict['Txhash'])

        if t_internal_txns:
            merge_internal_tx(data_row, t_internal_txns)

        if t_tokens:
            if config.debug:
                sys.stderr.write("%smerge: txn:  %s\n" % (Fore.GREEN, data_row))

            if dr.row_dict['Txhash'] not in tx_ids[wallet]:
                tx_ids[wallet][dr.row_dict['Txhash']] = []

            tx_ids[wallet][dr.row_dict['Txhash']]. \
                    append(MergeDataRow(dr, data_files[file_id], file_id))

    for wallet in tx_ids:
        for txn in tx_ids[wallet]:
            if len(tx_ids[wallet][txn]) == 1:
                if config.debug:
                    sys.stderr.write("%smerge: token:%s\n" % (Fore.GREEN, t.t_record))

            for t in tx_ids[wallet][txn]:
                if config.debug:
                    sys.stderr.write("%smerge: %s:%s\n" % (
                        Fore.GREEN,
                        t.data_file_id.ljust(5),
                        t.data_row))

            t_ins, t_outs, t_fee = get_ins_outs(tx_ids[wallet][txn])

            if config.debug:
                output_records(t_ins, t_outs, t_fee)
                sys.stderr.write("%smerge:     consolidate:\n" % (Fore.YELLOW))

            consolidate(tx_ids[wallet][txn], [TXNS, INTERNAL_TXNS])

        t_ins = [t for t in t_tokens + [data_row] if t.t_record and
                 t.t_record.t_type == TransactionOutRecord.TYPE_DEPOSIT]
        t_outs = [t for t in t_tokens + [data_row] if t.t_record and
                  t.t_record.t_type == TransactionOutRecord.TYPE_WITHDRAWAL]

        if config.debug:
            output_records(data_row, t_ins, t_outs)

            t_ins_orig = copy.copy(t_ins)
            if t_fee:
                method_handling(t_ins, t_fee, staking_addresses)

            # Make trades
            if len(t_ins) == 1 and t_outs:
                do_etherscan_multi_sell(t_ins, t_outs, t_fee)
            elif len(t_outs) == 1 and t_ins:
                do_etherscan_multi_buy(t_ins, t_outs, t_fee)
            elif len(t_ins) > 1 and len(t_outs) > 1:
                # multi-sell to multi-buy trade not supported
                sys.stderr.write("%sWARNING%s Merge failure for Txhash: %s\n" % (
                    Back.YELLOW+Fore.BLACK, Back.RESET+Fore.YELLOW, txn))

                for mdr in tx_ids[wallet][txn]:
                    mdr.data_row.failure = UnexpectedContentError(
                        mdr.data_file.parser.in_header.index('Txhash'),
                        'Txhash', mdr.data_row.row_dict['Txhash'])
                    sys.stderr.write("%srow[%s] %s\n" % (
                        Fore.YELLOW,
                        mdr.data_file.parser.in_header_row_num + mdr.data_row.line_num,
                        mdr.data_row))
                continue

            if t_fee:
                # Split fees
                t_all = [t for t in t_ins_orig + t_outs if t.t_record]
                do_fee_split(t_all, t_fee, fee_quantity, fee_asset)

            merge = True

            if config.debug:
                output_records(t_ins_orig, t_outs, t_fee)

    return merge

def merge_internal_tx(data_row, t_internal_txns):
    quantity = Decimal(0)
    if data_row.t_record.buy_quantity:
        quantity += data_row.t_record.buy_quantity
    if data_row.t_record.sell_quantity:
        quantity -= data_row.t_record.sell_quantity

    # this is ususally an ETH refund when providing liquidity to an ETH/TOKEN pool
    for t in t_internal_txns:
        if t.t_record.t_type == TransactionOutRecord.TYPE_DEPOSIT:
            quantity += t.t_record.buy_quantity
        elif t.t_record.t_type == TransactionOutRecord.TYPE_WITHDRAWAL:
            quantity -= t.t_record.sell_quantity

        t.t_record = None

    if quantity > 0:
        data_row.t_record.t_type = TransactionOutRecord.TYPE_DEPOSIT
        data_row.t_record.buy_asset = 'ETH'
        data_row.t_record.buy_quantity = quantity
        data_row.t_record.sell_asset = ''
        data_row.t_record.sell_quantity = None
    elif quantity < 0:
        data_row.t_record.t_type = TransactionOutRecord.TYPE_WITHDRAWAL
        data_row.t_record.sell_asset = 'ETH'
        data_row.t_record.sell_quantity = abs(quantity)
        data_row.t_record.buy_asset = ''
        data_row.t_record.buy_quantity = None
    else:
        data_row.t_record.t_type = TransactionOutRecord.TYPE_SPEND
        data_row.t_record.sell_asset = 'ETH'
        data_row.t_record.sell_quantity = quantity
        data_row.t_record.buy_asset = ''
        data_row.t_record.buy_quantity = None

def find_tx_tokens(data_rows, tx_hash):
    return [data_row for data_row in data_rows
            if data_row.row_dict['Txhash'] == tx_hash and not data_row.parsed]

    if len(t_fees) == 0:
        t_fee = None
    elif len(t_fees) == 1:
        t_fee = t_fees[0]
    else:
        raise Exception

    return t_ins, t_outs, t_fee

def consolidate(tx_ids, file_ids):
    tx_assets = {}

    for txn in list(tx_ids):
        if txn.data_file_id not in file_ids:
            return

        asset = txn.data_row.t_record.get_asset()
        if asset not in tx_assets:
            tx_assets[asset] = txn
            tx_assets[asset].quantity += txn.data_row.t_record.get_quantity()
        else:
            tx_assets[asset].quantity += txn.data_row.t_record.get_quantity()
            txn.data_row.t_record = None
            tx_ids.remove(txn)

    for asset in tx_assets:
        txn = tx_assets[asset]
        if txn.quantity > 0:
            txn.data_row.t_record.t_type = TransactionOutRecord.TYPE_DEPOSIT
            txn.data_row.t_record.buy_asset = asset
            txn.data_row.t_record.buy_quantity = txn.quantity
            txn.data_row.t_record.sell_asset = ''
            txn.data_row.t_record.sell_quantity = None
        elif tx_assets[asset].quantity < 0:
            txn.data_row.t_record.t_type = TransactionOutRecord.TYPE_WITHDRAWAL
            txn.data_row.t_record.buy_asset = ''
            txn.data_row.t_record.buy_quantity = None
            txn.data_row.t_record.sell_asset = asset
            txn.data_row.t_record.sell_quantity = abs(txn.quantity)
        else:
            if txn.data_row.t_record.fee_quantity:
                txn.data_row.t_record.t_type = TransactionOutRecord.TYPE_SPEND
                txn.data_row.t_record.buy_asset = ''
                txn.data_row.t_record.buy_quantity = None
                txn.data_row.t_record.sell_asset = asset
                txn.data_row.t_record.sell_quantity = Decimal(0)
            else:
                tx_ids.remove(txn)

def output_records(t_ins, t_outs, t_fee):
    dup = bool(t_fee and t_fee in t_ins + t_outs)

    if t_fee:
        sys.stderr.write("%smerge:   TR-F%s: %s\n" % (
            Fore.YELLOW, '*' if dup else '', t_fee.t_record))

    for t_in in t_ins:
        sys.stderr.write("%smerge:   TR-I%s: %s\n" % (
            Fore.YELLOW, '*' if t_fee is t_in else '', t_in.t_record))
    for t_out in t_outs:
        sys.stderr.write("%smerge:   TR-O%s: %s\n" % (
            Fore.YELLOW, '*' if t_fee is t_out else '', t_out.t_record))

def method_handling(t_ins, data_row, staking_addresses):
    if not t_ins:
        return

    t_rewards = [t for t in t_ins if
        data_row.row_dict.get('To') in staking_addresses and
        t.row_dict['ContractAddress'] in staking_addresses[data_row.row_dict.get('To')]]

    if not t_rewards and 'reward' in data_row.row_dict.get('Method').lower():
        sys.stderr.write("%sWARNING%s Potential missing staking reward for Txhash: %s (address %s)\n" % (
            Back.YELLOW+Fore.BLACK, Back.RESET+Fore.YELLOW, data_row.row_dict['Txhash'], data_row.row_dict['To']))

    for t_reward in t_rewards:
        t_reward.t_record.t_type = TransactionOutRecord.TYPE_STAKING
        t_ins.remove(t_reward)

        if config.debug:
            sys.stderr.write("%smerge:     staking\n" % (Fore.YELLOW))

def do_etherscan_multi_sell(t_ins, t_outs, t_fee):
    if config.debug:
        sys.stderr.write("%smerge:     trade sell(s):\n" % (Fore.YELLOW))

    tot_buy_quantity = 0

    buy_quantity = t_ins[0].t_record.buy_quantity
    buy_asset = t_ins[0].t_record.buy_asset

    if config.debug:
        sys.stderr.write("%smerge:       buy_quantity=%s buy_asset=%s\n" % (
            Fore.YELLOW,
            TransactionOutRecord.format_quantity(buy_quantity), buy_asset))

    for t_out in t_outs:
        if (t_out.t_record.sell_quantity == 0):
            t_out.t_record = None

    t_outs[:] = [t_out for t_out in t_outs if t_out.t_record]

    for cnt, t_out in enumerate(t_outs):
        if cnt < len(t_outs) - 1:
            split_buy_quantity = (buy_quantity / len(t_outs)).quantize(PRECISION)
            tot_buy_quantity += split_buy_quantity
        else:
            # Last t_out, use up remainder
            split_buy_quantity = buy_quantity - tot_buy_quantity

        if config.debug:
            sys.stderr.write("%smerge:       split_buy_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_buy_quantity)))

        t_out.t_record.t_type = TransactionOutRecord.TYPE_TRADE
        t_out.t_record.buy_quantity = split_buy_quantity
        t_out.t_record.buy_asset = buy_asset
        if t_fee:
            t_out.t_record.note = get_note(t_fee.row_dict)

    # Remove TR for buy now it's been added to each sell
    if t_ins[0] == data_row:
        data_row.t_record.t_type = TransactionOutRecord.TYPE_SPEND
        data_row.t_record.buy_quantity = None
        data_row.t_record.buy_asset = ''
    else:
        t_ins[0].t_record = None

def do_etherscan_multi_buy(t_ins, t_outs, t_fee):
    if config.debug:
        sys.stderr.write("%smerge:     trade buy(s):\n" % (Fore.YELLOW))

    tot_sell_quantity = 0

    sell_quantity = t_outs[0].t_record.sell_quantity
    sell_asset = t_outs[0].t_record.sell_asset

    if config.debug:
        sys.stderr.write("%smerge:       sell_quantity=%s sell_asset=%s\n" % (
            Fore.YELLOW,
            TransactionOutRecord.format_quantity(sell_quantity), sell_asset))


    for t_in in t_ins:
        if (t_in.t_record.buy_quantity == 0):
            t_in.t_record = None

    t_ins[:] = [t_in for t_in in t_ins if t_in.t_record]

    for cnt, t_in in enumerate(t_ins):
        if cnt < len(t_ins) - 1:
            split_sell_quantity = (sell_quantity / len(t_ins)).quantize(PRECISION)
            tot_sell_quantity += split_sell_quantity
        else:
            # Last t_in, use up remainder
            split_sell_quantity = sell_quantity - tot_sell_quantity

        if config.debug:
            sys.stderr.write("%smerge:       split_sell_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_sell_quantity)))

        t_in.t_record.t_type = TransactionOutRecord.TYPE_TRADE
        t_in.t_record.sell_quantity = split_sell_quantity
        t_in.t_record.sell_asset = sell_asset
        if t_fee:
            t_in.t_record.note = get_note(t_fee.row_dict)

    # Remove TR for sell now it's been added to each buy
    if t_outs[0] == data_row:
        data_row.t_record.t_type = TransactionOutRecord.TYPE_SPEND
        data_row.t_record.sell_quantity = None
        data_row.t_record.sell_asset = ''
    else:
        t_outs[0].t_record = None

def do_fee_split(t_all, t_fee, fee_quantity, fee_asset):
    if config.debug:
        sys.stderr.write("%smerge:     split fees:\n" % (Fore.YELLOW))
        sys.stderr.write("%smerge:       fee_quantity=%s fee_asset=%s\n" % (
            Fore.YELLOW,
            TransactionOutRecord.format_quantity(fee_quantity), fee_asset))

    tot_fee_quantity = 0

    for cnt, t in enumerate(t_all):
        if cnt < len(t_all) - 1:
            split_fee_quantity = (fee_quantity / len(t_all)).quantize(PRECISION)
            tot_fee_quantity += split_fee_quantity
        else:
            # Last t, use up remainder
            split_fee_quantity = fee_quantity - tot_fee_quantity if fee_quantity else None

        if config.debug:
            sys.stderr.write("%smerge:       split_fee_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_fee_quantity)))

        t.t_record.fee_quantity = split_fee_quantity
        t.t_record.fee_asset = fee_asset
        t.t_record.note = get_note(t_fee.row_dict)

    # Remove TR for fee now it's been added to each withdrawal
    if t_fee.t_record and t_fee not in t_all:
        if t_fee.t_record.t_type == TransactionOutRecord.TYPE_SPEND:
            t_fee.t_record = None
        else:
            t_fee.t_record.fee_quantity = None
            t_fee.t_record.fee_asset = ''

DataMerge("Etherscan fees & multi-token transactions",
          {'txns': etherscan_txns, 'tokens': etherscan_tokens, 'internal_txns': etherscan_internal_txns},
          merge_etherscan)
