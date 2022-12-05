# -*- coding: utf-8 -*-

import sys
import copy

from decimal import Decimal

from colorama import Fore, Back

from ...config import config
from ..out_record import TransactionOutRecord
from ..datamerge import DataMerge
from ..exceptions import UnexpectedContentError
from ..parsers.blockscout import blockscout_txns, blockscout_tokens, blockscout_internal_txns, get_note

PRECISION = Decimal('0.' + '0' * 18)

STAKING = {
    # HNY-WXDAI Farm
    '0x8520fc4c282342f8e746b881b9b60c14f96a0fab': [
        # HNY
        '0x71850b7e9ee3f13ab46d67167341e4bdc905eef9',
    ],
    # HNY-STAKE Farm
    '0xa6c55971f21cc1c35ea617f47980d669a0c09cf3': [
        # HNY
        '0x71850b7e9ee3f13ab46d67167341e4bdc905eef9',
    ],
    # Honey Farm V2
    '0xb44825cf0d8d4dd552f2434056c41582415aaaa1': [
        # xCOMB
        '0x38fb649ad3d6ba1113be5f57b927053e97fc5bf7',
    ],
    # Celeste
    '0x8c9968a2b16bc1cd0ead74f5eef25e899e795501': [
        # no rewards
    ],
    # 1Hive Staking
    '0x0e25b918c9fb2fea5d42011d1f4b9f8c61b453e7': [
        # no rewards
    ],
}

AIRDROPS = {
    # Honey Faucet
    '0x967ebb4343c442d19a47b9196d121bd600600911': [
        # HNY
        '0x71850b7e9ee3f13ab46d67167341e4bdc905eef9',
    ],
    # xCOMB Airdrop
    '0xdd36008685108afafc11f88bbc66c39a851df843': [
        # xCOMB
        '0x38fb649ad3d6ba1113be5f57b927053e97fc5bf7'
    ],
    # Freedom Reserve Airdrop
    '0xa5025faba6e70b84f74e9b1113e5f7f4e7f4859f': [
        # FR
        '0x270de58f54649608d316faa795a9941b355a2bd0',
    ],
    # Agave Airdeop
    '0xfd97188bcaf9fc0df5ab0a6cca263c3aada1f382': [
        # AGVE
        '0x3a97704a1b25f08aa230ae53b352e2e72ef52843',
    ]
}

def merge_blockscout(data_files):
    return do_merge_blockscout(data_files, STAKING, AIRDROPS)

def do_merge_blockscout(data_files, staking_addresses, airdrop_addresses):
    merge = False

    for data_row in data_files['txns'].data_rows:
        t_tokens = find_tx_tokens(data_files['tokens'].data_rows,
                                  data_row.row_dict['TxHash'])

        t_internal_txns = find_tx_tokens(data_files['internal_txns'].data_rows,
                                   data_row.row_dict['TxHash'])

        if t_internal_txns:
            merge_internal_tx(data_row, t_internal_txns)

        if t_tokens:
            if config.debug:
                sys.stderr.write("%smerge: txn:  %s\n" % (Fore.GREEN, data_row))

            for t in t_tokens:
                if config.debug:
                    sys.stderr.write("%smerge: token:%s\n" % (Fore.GREEN, t.t_record))

                data_row.parsed = True

        else:
            if config.debug:
                sys.stderr.write("%smerge: txn:  %s\n" % (Fore.BLUE, data_row))

            continue

        t_ins = [t for t in t_tokens + [data_row] if t.t_record and
                 t.t_record.t_type == TransactionOutRecord.TYPE_DEPOSIT]
        t_outs = [t for t in t_tokens + [data_row] if t.t_record and
                  t.t_record.t_type == TransactionOutRecord.TYPE_WITHDRAWAL]
        t_stakings = []

        if config.debug:
            output_records(data_row, t_ins, t_outs)

        t_ins_orig = copy.copy(t_ins)

        if not t_outs:
            do_handle_airdrops(data_row, t_ins, airdrop_addresses)
            do_handle_staking_rewards(data_row, t_ins, staking_addresses)

        # Make trades
        if len(t_ins) == 1 and t_outs:
            do_blockscout_multi_sell(t_ins, t_outs, data_row)
        elif len(t_outs) == 1 and t_ins:
            do_blockscout_multi_buy(t_ins, t_outs, data_row)
        elif not t_ins:
            do_enter_staking(data_row, t_outs, staking_addresses, t_stakings)
        elif not t_outs:
            do_exit_staking(data_row, t_ins, staking_addresses, t_stakings)
        elif len(t_ins) > 1 and len(t_outs) > 1:
            # multi-sell to multi-buy trade not supported
            sys.stderr.write("%sWARNING%s Merge failure for TxHash: %s\n" % (
                Back.YELLOW+Fore.BLACK, Back.RESET+Fore.YELLOW, data_row.row_dict['TxHash']))

            for t in t_tokens:
                t.failure = UnexpectedContentError(
                    data_files['tokens'].parser.in_header.index('TxHash'),
                    'TxHash', t.row_dict['TxHash'])
                sys.stderr.write("%srow[%s] %s\n" % (
                    Fore.YELLOW, data_files['tokens'].parser.in_header_row_num + t.line_num, t))

        # Split fees
        t_tokens = [t for t in t_tokens if t.t_record]
        do_fee_split(t_tokens, data_row)

        # Add enter/exist staking txns
        data_files['tokens'].data_rows += t_stakings

        merge = True

        if config.debug:
            output_records(data_row, t_ins_orig, t_outs)


    # handle token transactions with no parent tx, e.g. an airdrop sent directly to my wallet
    t_ins = [t for t in data_files['tokens'].data_rows if not t.parsed and t.t_record and
                 t.t_record.t_type == TransactionOutRecord.TYPE_DEPOSIT]
    do_handle_airdrops(None, t_ins, airdrop_addresses)

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
        data_row.t_record.buy_asset = 'xDAI'
        data_row.t_record.buy_quantity = quantity
        data_row.t_record.sell_asset = ''
        data_row.t_record.sell_quantity = None
    elif quantity < 0:
        data_row.t_record.t_type = TransactionOutRecord.TYPE_WITHDRAWAL
        data_row.t_record.sell_asset = 'xDAI'
        data_row.t_record.sell_quantity = abs(quantity)
        data_row.t_record.buy_asset = ''
        data_row.t_record.buy_quantity = None
    else:
        data_row.t_record.t_type = TransactionOutRecord.TYPE_SPEND
        data_row.t_record.sell_asset = 'xDAI'
        data_row.t_record.sell_quantity = quantity
        data_row.t_record.buy_asset = ''
        data_row.t_record.buy_quantity = None

def find_tx_tokens(data_rows, tx_hash):
    return [data_row for data_row in data_rows
            if data_row.row_dict['TxHash'] == tx_hash and not data_row.parsed]

def output_records(data_row, t_ins, t_outs):
    dup = bool(data_row in t_ins + t_outs)

    sys.stderr.write("%smerge:   TR-F%s: %s\n" % (
        Fore.YELLOW, '*' if dup else '', data_row.t_record))

    for t_in in t_ins:
        sys.stderr.write("%smerge:   TR-I%s: %s\n" % (
            Fore.YELLOW, '*' if data_row is t_in else '', t_in.t_record))
    for t_out in t_outs:
        sys.stderr.write("%smerge:   TR-O%s: %s\n" % (
            Fore.YELLOW, '*' if data_row is t_out else '', t_out.t_record))

def do_handle_staking_rewards(data_row, t_ins, staking_addresses):
    t_rewards = [t for t in t_ins if
        (t.row_dict['FromAddress'] in staking_addresses and
            t.row_dict['TokenContractAddress'] in staking_addresses[t.row_dict['FromAddress']]) or
        (data_row.row_dict['ToAddress'] in staking_addresses and
            t.row_dict['TokenContractAddress'] in staking_addresses[data_row.row_dict['ToAddress']])
    ]

    for t_reward in t_rewards:
        t_reward.t_record.t_type = TransactionOutRecord.TYPE_STAKING
        t_reward.t_record.note = 'Staking Rewards'
        t_ins.remove(t_reward)

        if config.debug:
            sys.stderr.write("%smerge:     staking_rewards_quantity=%s staking_rewards_asset=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(t_reward.t_record.buy_quantity), t_reward.t_record.buy_asset))

def do_handle_airdrops(data_row, t_ins, airdrop_addresses):
    t_airdrops = [t for t in t_ins if
        (t.row_dict['FromAddress'] in airdrop_addresses and
            t.row_dict['TokenContractAddress'] in airdrop_addresses[t.row_dict['FromAddress']]) or
        (data_row and data_row.row_dict['ToAddress'] in airdrop_addresses and
            t.row_dict['TokenContractAddress'] in airdrop_addresses[data_row.row_dict['ToAddress']])
    ]

    for t_airdrop in t_airdrops:
        t_airdrop.t_record.t_type = TransactionOutRecord.TYPE_AIRDROP
        t_ins.remove(t_airdrop)

        if config.debug:
            sys.stderr.write("%smerge:     airdrop_quantity=%s airdrop_asset=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(t_airdrop.t_record.buy_quantity), t_airdrop.t_record.buy_asset))

def do_blockscout_multi_sell(t_ins, t_outs, data_row):
    if config.debug:
        sys.stderr.write("%smerge:     trade sell(s)\n" % (Fore.YELLOW))

    tot_buy_quantity = 0

    buy_quantity = t_ins[0].t_record.buy_quantity
    buy_asset = t_ins[0].t_record.buy_asset

    if config.debug:
        sys.stderr.write("%smerge:     buy_quantity=%s buy_asset=%s\n" % (
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
            sys.stderr.write("%smerge:     split_buy_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_buy_quantity)))

        t_out.t_record.t_type = TransactionOutRecord.TYPE_TRADE
        t_out.t_record.buy_quantity = split_buy_quantity
        t_out.t_record.buy_asset = buy_asset
        t_out.t_record.note = get_note(data_row.row_dict)

    # Remove TR for buy now it's been added to each sell
    if t_ins[0] == data_row:
        data_row.t_record.t_type = TransactionOutRecord.TYPE_SPEND
        data_row.t_record.buy_quantity = None
        data_row.t_record.buy_asset = ''
    else:
        t_ins[0].t_record = None

def do_blockscout_multi_buy(t_ins, t_outs, data_row):
    if config.debug:
        sys.stderr.write("%smerge:     trade buy(s)\n" % (Fore.YELLOW))

    tot_sell_quantity = 0

    sell_quantity = t_outs[0].t_record.sell_quantity
    sell_asset = t_outs[0].t_record.sell_asset

    if config.debug:
        sys.stderr.write("%smerge:     sell_quantity=%s sell_asset=%s\n" % (
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
            sys.stderr.write("%smerge:     split_sell_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_sell_quantity)))

        t_in.t_record.t_type = TransactionOutRecord.TYPE_TRADE
        t_in.t_record.sell_quantity = split_sell_quantity
        t_in.t_record.sell_asset = sell_asset
        t_in.t_record.note = get_note(data_row.row_dict)

    # Remove TR for sell now it's been added to each buy
    if t_outs[0] == data_row:
        data_row.t_record.t_type = TransactionOutRecord.TYPE_SPEND
        data_row.t_record.sell_quantity = None
        data_row.t_record.sell_asset = ''
    else:
        t_outs[0].t_record = None


def do_enter_staking(data_row, t_outs, staking_addresses, t_stakings):
    for t_out in t_outs:
        if t_out.row_dict['ToAddress'] in staking_addresses or data_row.row_dict['ToAddress'] in staking_addresses:
            # add a deposit tx on the virtual staking wallet to match the withdrawal
            deposit_t = copy.copy(t_out)
            deposit_t.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                    deposit_t.timestamp,
                                                    buy_quantity=t_out.t_record.sell_quantity,
                                                    buy_asset=t_out.t_record.sell_asset,
                                                    buy_value=t_out.t_record.sell_value,
                                                    wallet=get_staking_wallet(t_out.t_record.wallet),
                                                    note='Enter Staking')
            t_stakings.append(deposit_t)

def do_exit_staking(data_row, t_ins, staking_addresses, t_stakings):
    for t_in in t_ins:
        if t_in.row_dict['FromAddress'] in staking_addresses or data_row.row_dict['ToAddress'] in staking_addresses:
            # add a withdrawal tx from the virtual staking wallet to match the deposit
            withdrawal_t = copy.copy(t_in)
            withdrawal_t.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                    withdrawal_t.timestamp,
                                                    sell_quantity=t_in.t_record.buy_quantity,
                                                    sell_asset=t_in.t_record.buy_asset,
                                                    sell_value=t_in.t_record.buy_value,
                                                    wallet=get_staking_wallet(t_in.t_record.wallet),
                                                    note='Exit Staking')
            t_stakings.append(withdrawal_t)

def get_staking_wallet(wallet):
    return "%s:Staking" % wallet

def do_fee_split(t_tokens, data_row):
    if config.debug:
        sys.stderr.write("%smerge:     split fees\n" % (Fore.YELLOW))

    tot_fee_quantity = 0

    fee_quantity = data_row.t_record.fee_quantity
    fee_asset = data_row.t_record.fee_asset

    for cnt, t in enumerate(t_tokens):
        if cnt < len(t_tokens) - 1:
            split_fee_quantity = (fee_quantity / len(t_tokens)).quantize(PRECISION)
            tot_fee_quantity += split_fee_quantity
        else:
            # Last t, use up remainder
            split_fee_quantity = fee_quantity - tot_fee_quantity if fee_quantity else None

        if config.debug:
            sys.stderr.write("%smerge:     split_fee_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_fee_quantity)))

        t.t_record.fee_quantity = split_fee_quantity
        t.t_record.fee_asset = fee_asset
        t.t_record.note = get_note(data_row.row_dict)

    # Remove TR for fee now it's been added to each withdrawal
    if data_row.t_record and data_row not in t_tokens:
        if data_row.t_record.t_type == TransactionOutRecord.TYPE_SPEND:
            data_row.t_record = None
        else:
            data_row.t_record.fee_quantity = None
            data_row.t_record.fee_asset = ''


DataMerge("blockscout fees & multi-token transactions",
          {'txns': {'req': DataMerge.MAN, 'obj': blockscout_txns},
           'tokens': {'req': DataMerge.MAN, 'obj': blockscout_tokens},
           'internal_txns': {'req': DataMerge.MAN, 'obj': blockscout_internal_txns}},
          merge_blockscout)
