# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2021

import sys
import copy

from decimal import Decimal

from colorama import Fore, Back

from ...config import config
from ..out_record import TransactionOutRecord
from ..datamerge import DataMerge
from ..exceptions import UnexpectedContentError
from ..parsers.etherscan import etherscan_txns, etherscan_tokens, etherscan_internal_txns, get_note

PRECISION = Decimal('0.' + '0' * 18)

STAKING = {
    # SnowSwap ySNOW pool
    '0xc672e12bb517876b3cd0d83290927d34eb5e5c24': [
        # SNOW
        '0xfe9a29ab92522d14fc65880d817214261d8479ae',
    ],
    # SnowSwap ySNOW pool v2
    '0x86d0e7ad012bf743b660235a531dfc45608f4dc5': [
        # SNOW
        '0xfe9a29ab92522d14fc65880d817214261d8479ae',
    ], 
    # SnowSwap SNOW pool
    '0x7d2c8b58032844f222e2c80219975805dce1921c': [
        # staking and reward token are both SNOW
    ], 
    # Bancor LiquidityProtection
    '0x135742e6c5d70fc960090eddbd9f3c6d0d494e99': [
        # vBNT
        '0x48fb253446873234f2febbf9bdeaa72d9d387f94',
    ],
    # Bancor LiquidityProtection
    '0xeead394a017b8428e2d5a976a054f303f78f3c0c': [
        # vBNT
        '0x48fb253446873234f2febbf9bdeaa72d9d387f94',
    ],
    # Bancor LiquidityProtection
    '0x853c2d147a1bd7eda8fe0f58fb3c5294db07220e': [
        # vBNT
        '0x48fb253446873234f2febbf9bdeaa72d9d387f94',
    ],
    # Bancor StakingRewards
    '0x457fe44e832181e1d3ecee0fc5be72cd9b36859f': [
        # vBNT
        '0x48fb253446873234f2febbf9bdeaa72d9d387f94',
    ], 
    # Bancor StakingRewards
    '0xb443dea978b39178cb05ae005074227a4390dfce': [
        # vBNT
        '0x48fb253446873234f2febbf9bdeaa72d9d387f94',
    ],
    # Bancor Governance Staking
    '0x892f481bd6e9d7d26ae365211d9b45175d5d00e4': [
        # staking token is vBNT, but no rewards
    ],
    # Ampleforth Beehive V1 Geyser (Uniswap ETH-AMPL)
    '0x23796bc856ed786dcc505984fd538f91dad3194a': [
        # AMPL
        '0xd46ba6d942050d489dbd938a2c909a5d5039a161',
    ],
    # Ampleforth Beehive V2 Geyser (Uniswap ETH-AMPL)
    '0x075bb66a472ab2bbb8c215629c77e8ee128cc2fc': [
        # AMPL
        '0xd46ba6d942050d489dbd938a2c909a5d5039a161',
    ],
    # ETH/DAI pool on Uniswap V2
    '0xa1484c3aa22a66c62b77e0ae78e15258bd0cb711': [
        # UNI
        '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984',
    ],
    # Benchmark Launchpool
    '0x6544b1cd2d28c6c53b52a1ffb8e547740e426b33': [
        # MARK
        '0x67c597624b17b16fb77959217360b7cd18284253',
    ],
    # Benchmark The Press
    '0x5d9972dd3ba5602574abea6bf9e1713568d49903': [
        # MARK
        '0x67c597624b17b16fb77959217360b7cd18284253',
    ],
    # EasyStaking
    '0xecbcd6d7264e3c9eac24c7130ed3cd2b38f5a7ad': [
        # staking and reward token are both STAKE
    ],
    # Indexcoop MVI Liquidity Program
    '0x5bc4249641b4bf4e37ef513f3fa5c63ecab34881': [
        # INDEX
        '0x0954906da0Bf32d5479e25f46056d22f08464cab',
    ],
    # Pickle
    '0xda481b277dce305b97f4091bd66595d57cf31634': [
        # PICKLE
        '0x429881672b9ae42b8eba0e26cd9c73711b891ca5',
    ],
    # dYdX
    '0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e': [
    ]
}

AIRDROPS = {
    # SnowSwap
    '0x2011a0c8437b735dd7a7f2987006ed692a24994b': [
        # SNOW
        '0xfe9a29ab92522d14fc65880d817214261d8479ae',
    ],
    # Ampleforth
    '0xf497b83cfbd31e7ba1ab646f3b50ae0af52d03a1': [
        # FORTH
        '0x77fba179c79de5b7653f68b5039af940ada60ce0',
    ],
    # Multisender.app
    '0xa5025faba6e70b84f74e9b1113e5f7f4e7f4859f': [
        # xICHI
        '0x70605a6457b0a8fbf1eee896911895296eab467e',
    ],
    # 1Inch
    '0xe295ad71242373c37c5fda7b57f26f9ea1088afe': [
        # 1INCH
        '0x111111111117dc0aa78b770fa6a738034120c302',
    ],
    # Force DAO
    '0x42b38ca3e09c92ca82fa3659ca039188a6a076ac': [
        # FORCE
        '0x2c31b10ca416b82cec4c5e93c615ca851213d48d',
    ]
}

def merge_etherscan(data_files):
    return do_merge_etherscan(data_files, STAKING, AIRDROPS)

def do_merge_etherscan(data_files, staking_addresses, airdrop_addresses):
    merge = False

    for data_row in data_files['txns'].data_rows:
        t_tokens = find_tx_tokens(data_files['tokens'].data_rows,
                                  data_row.row_dict['Txhash'])

        t_internal_txns = find_tx_tokens(data_files['internal_txns'].data_rows,
                                   data_row.row_dict['Txhash'])

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
            output_records(data_row, t_ins, t_outs, t_stakings)

        t_ins_orig = copy.copy(t_ins)

        if not t_outs:
            do_handle_airdrops(data_row, t_ins, airdrop_addresses)
            do_handle_staking_rewards(data_row, t_ins, staking_addresses)

        # Make trades
        if len(t_ins) == 1 and t_outs:
            do_etherscan_multi_sell(t_ins, t_outs, data_row)
        elif len(t_outs) == 1 and t_ins:
            do_etherscan_multi_buy(t_ins, t_outs, data_row)
        elif not t_ins:
            do_enter_staking(data_row, t_outs, staking_addresses, t_stakings)
        elif not t_outs:
            do_exit_staking(data_row, t_ins, staking_addresses, t_stakings)
        elif len(t_ins) > 1 and len(t_outs) > 1:
            # multi-sell to multi-buy trade not supported
            sys.stderr.write("%sWARNING%s Merge failure for Txhash: %s\n" % (
                Back.YELLOW+Fore.BLACK, Back.RESET+Fore.YELLOW, data_row.row_dict['Txhash']))

            for t in t_tokens:
                t.failure = UnexpectedContentError(
                    data_files['tokens'].parser.in_header.index('Txhash'),
                    'Txhash', t.row_dict['Txhash'])
                sys.stderr.write("%srow[%s] %s\n" % (
                    Fore.YELLOW, data_files['tokens'].parser.in_header_row_num + t.line_num, t))

        # Split fees
        t_tokens = [t for t in t_tokens if t.t_record]
        do_fee_split(t_tokens, data_row)

        # Add enter/exist staking txns
        data_files['tokens'].data_rows += t_stakings

        merge = True

        if config.debug:
            output_records(data_row, t_ins_orig, t_outs, t_stakings)

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

def output_records(data_row, t_ins, t_outs, t_stakings):
    dup = bool(data_row in t_ins + t_outs)

    sys.stderr.write("%smerge:   TR-F%s: %s\n" % (
        Fore.YELLOW, '*' if dup else '', data_row.t_record))

    for t_in in t_ins:
        sys.stderr.write("%smerge:   TR-I%s: %s\n" % (
            Fore.YELLOW, '*' if data_row is t_in else '', t_in.t_record))
    for t_out in t_outs:
        sys.stderr.write("%smerge:   TR-O%s: %s\n" % (
            Fore.YELLOW, '*' if data_row is t_out else '', t_out.t_record))
    for t_staking in t_stakings:
        sys.stderr.write("%smerge:   TR-S%s: %s\n" % (
            Fore.YELLOW, '*' if data_row is t_staking else '', t_staking.t_record))

def do_handle_staking_rewards(data_row, t_ins, staking_addresses):
    t_rewards = [t for t in t_ins if
        (t.row_dict['From'] in staking_addresses and
            t.row_dict['ContractAddress'] in staking_addresses[t.row_dict['From']]) or
        (data_row.row_dict['To'] in staking_addresses and
            t.row_dict['ContractAddress'] in staking_addresses[data_row.row_dict['To']])
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
        (t.row_dict['From'] in airdrop_addresses and
            t.row_dict['ContractAddress'] in airdrop_addresses[t.row_dict['From']]) or
        (data_row and data_row.row_dict['To'] in airdrop_addresses and
            t.row_dict['ContractAddress'] in airdrop_addresses[data_row.row_dict['To']])
    ]

    for t_airdrop in t_airdrops:
        t_airdrop.t_record.t_type = TransactionOutRecord.TYPE_AIRDROP
        t_ins.remove(t_airdrop)

        if config.debug:
            sys.stderr.write("%smerge:     airdrop_quantity=%s airdrop_asset=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(t_airdrop.t_record.buy_quantity), t_airdrop.t_record.buy_asset))

def do_etherscan_multi_sell(t_ins, t_outs, data_row):
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

def do_etherscan_multi_buy(t_ins, t_outs, data_row):
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
        if t_out.row_dict['To'] in staking_addresses or data_row.row_dict['To'] in staking_addresses:
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
        if t_in.row_dict['From'] in staking_addresses or data_row.row_dict['To'] in staking_addresses:
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
        t.t_record.note = get_note(data_row.row_dict) if not t.t_record.note else t.t_record.note

    # Remove TR for fee now it's been added to each withdrawal
    if data_row.t_record and data_row not in t_tokens:
        if data_row.t_record.t_type == TransactionOutRecord.TYPE_SPEND:
            data_row.t_record = None
        else:
            data_row.t_record.fee_quantity = None
            data_row.t_record.fee_asset = ''

DataMerge("Etherscan fees & multi-token transactions",
          {'txns': etherscan_txns, 'tokens': etherscan_tokens, 'internal_txns': etherscan_internal_txns},
          merge_etherscan)
