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
from ..parsers.etherscan import etherscan_txns, etherscan_tokens, etherscan_nfts, etherscan_int, \
                                get_note

PRECISION = Decimal('0.' + '0' * 18)

TXNS = 'txn'
TOKENS = 'token'
NFTS = 'nft'
INTERNAL_TXNS = 'int'

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
    # Bancor StakingRewards
    '0x4b90695c2013fc60df1e168c2bcd4fd12f5c9841': [
        # BNT
        '0x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c',
    ],
    # Bancor StakingRewardsClaim
    '0x6248e4118818c9075a142ef8a12e09d49888af58': [
        # vBNT
        '0x48fb253446873234f2febbf9bdeaa72d9d387f94',
        # bnBNT
        '0xab05cf7c6c3a288cd36326e4f7b8600e7268e344',
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
    ],
    # Indexcoop MVI Liquidity Program
    '0x5bc4249641b4bf4e37ef513f3fa5c63ecab34881': [
        # INDEX
        '0x0954906da0bf32d5479e25f46056d22f08464cab',
    ],
    # Pickle Gauge
    '0xda481b277dce305b97f4091bd66595d57cf31634': [
        # PICKLE
        '0x429881672b9ae42b8eba0e26cd9c73711b891ca5',
    ],
     # Pickle Voting Escrow (DILL)
    '0xbbcf169ee191a1ba7371f30a1c344bfc498b29cf': [
        # PICKLE
        '0x429881672b9ae42b8eba0e26cd9c73711b891ca5',
    ],
    # dYdX
    '0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e': [
    ],
    # 1Inch GovernanceRewards
    '0x0f85a912448279111694f4ba4f85dc641c54b594': [
        # 1INCH
        '0x111111111117dc0aa78b770fa6a738034120c302',
    ],
    # AAVE Ecosystem Reserve
    '0x25f2226b597e8f9514b3f68f00f494cf4f286491': [
        # AAVE
        '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
    ],
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

    # SushiSwap: MiniChefV2 (Polygon)
    '0x0769fd68dfb93167989c6f7254cd0d766fb2841f': [
        # WMATIC
        '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270',
        # SUSHI
        '0x0b3f868e0be5597d5db7feb59e1cadbb0fdda50a',
    ],

    # StakingRewards (Polygon)
    '0x7ca29f0db5db8b88b332aa1d67a2e89dfec85e7e': [
        # QUICK
        '0x831753dd7087cac61ab5644b308642cc1c33dc13',
    ],

    # Adamant lock (Polygon)
    '0x920f22e1e5da04504b765f8110ab96a20e6408bd': [
        # WMATIC
        '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270',
        # QUICK
        '0x831753dd7087cac61ab5644b308642cc1c33dc13',
        # ADDY
        '0xc3fdbadc7c795ef1d6ba111e06ff8f16a20ea539',
        # dQUICK
        '0xf28164a485b0b2c90639e47b0f377b4a438a16b1',
    ],

    # Adamant Finance: Locked ADDY Boost
    '0xc5bcd23f21b6288417eb6c760f8ac0fbb4bb8a56': [
        # WMATIC
        '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270',
        # QUICK
        '0x831753dd7087cac61ab5644b308642cc1c33dc13',
        # ADDY
        '0xc3fdbadc7c795ef1d6ba111e06ff8f16a20ea539',
        # dQUICK
        '0xf28164a485b0b2c90639e47b0f377b4a438a16b1',
    ],

    # Adamant Vault
    '0x80e0378911d3f2e9ef9bbe782b4c2556c79d7fd3': [],

    # Adamant Vault
    '0x5c40cd282c6c80ab0d80bfb9367a4ad877fd2a44': [],

    # Adamant Vault
    '0x4dbd2ca9870f8d43116fc234c9f5ddbb373b0a43': [],

    # Adamant Vault
    '0x5c40cd282c6c80ab0d80bfb9367a4ad877fd2a44': [],

    # Adamant Vault
    '0x80e0378911d3f2e9ef9bbe782b4c2556c79d7fd3': [],

    # Adamant Vault
    '0x625252d89cb775ede460a1b03ef1efc33c72b068': [],

    # Adamant Vault
    '0x079eabca6696a7c755b2436f38eba89019fbb39d': [],

    # SpookySwap: MasterChef LP Staking Pool
    '0x2b2929e785374c651a81a63878ab22742656dcdd': [
        # BOO
        '0x841fad6eae12c286d1fd18d1d525dffa75c7effe',
    ],

    # SpiritSwap: MasterChef LP Staking Poo
    '0x9083ea3756bde6ee6f27a6e996806fbd37f6f093': [
        # SPIRIT
        '0x5cc61a78f164885776aa610fb0fe1257df78e59b',
    ],

    # Spooky Swap: IFO
    '0xacaca07e398d4946ad12232f40f255230e73ca72': [
        # BOO
        '0x841fad6eae12c286d1fd18d1d525dffa75c7effe',
    ],

    # SpiritSwap: inSpirit Token lock
    '0x2fbff41a9efaeae77538bd63f1ea489494acdc08': [
        # inSPIRIT
        '0x2fbff41a9efaeae77538bd63f1ea489494acdc08',
    ],

    # SpiritSwap: Gauge
    '0xefe02cb895b6e061fa227de683c04f3ce19f3a62': [
        # SPIRIT
        '0x5cc61a78f164885776aa610fb0fe1257df78e59b',
    ],

    # SpiritSwap: Fee Distributor
    '0x18cef75c2b032d7060e9cf96f29adf74a9a17ce6': [
        # SPIRIT
        '0x5cc61a78f164885776aa610fb0fe1257df78e59b',
    ],

    # Adamant (Arbitrum)
    '0x9d0eb05dd7a62860b280f1a0034b6396c596eff5': [],
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
    ],
    # CoinTool: MultiSender
    '0x6ab037288582c4994c19bfe6190f9c523f81b9df': [
        # ZCN
        '0xb9ef770b6a5e12e45983c5d80545258aa38f3b78',
    ],
    # dYdX: Claims Proxy
    '0x0fd829c3365a225fb9226e75c97c3a114bd3199e': [
        # DYDX
        '0x92d6c1e31e14520e676a687f0a93788b716beff5',
    ],
    # DappRadar: Airdrop
    '0x2e424a4953940ae99f153a50d0139e7cd108c071': [
        # RADAR
        '0x44709a920fccf795fbc57baa433cc3dd53c44dbe',
    ],
    # Disperse.app
    '0xd152f549545093347a162dce210e7293f1452150': [
        # GRT
        '0xc944e90c64b2c07662a292be6244bdf05cda44a7',
        # YEL
        '0x7815bda662050d84718b988735218cffd32f75ea',
    ],
    # Arable Protocol
    '0x095934290af634e613d898e39c190163b36887c0': [
        # ACRE
        '0xb2cabf797bc907b049e4ccb5b84d13be3a8cfc21',
    ],
    # Betswap
    '0xedd1cb10d6dde82c805f7fc9988ee3d89c115e34': [
        # BSGG
        '0x63682bdc5f875e9bf69e201550658492c9763f89',
    ],
    '0xff8f089128f53d6c54f769843defaaf5fbf02198': [
        # MEMO
        '0x136acd46c134e8269052c62a67042d6bdedde3c9',
    ],
    # xDAI Faucet
    '0x97AAE423C9A1Bc9cf2D81f9f1299b117A7b01136': [
        # xDAI
    ],
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
    ],

    # Fantom faucet
    '0xc2fd070eb72547d763399bd5c2ef172c0d9acff4': [],

    # Fantom
    '0x52569b7a98fcf73c77249e73437980af04b52802': [
        # LQDR
        '0xecdbe3937cf6ff27f70480855cfe03254f915b48',
    ],
}

def merge_etherscan(data_files):
    return do_merge_etherscan(data_files, STAKING, AIRDROPS)

def do_merge_etherscan(data_files, staking_addresses, airdrop_addresses):
    merge = False

    tx_ids = {}

    for file_id in data_files:
        for dr in data_files[file_id].data_rows:
            if not dr.t_record:
                continue

            wallet = dr.t_record.wallet[- abs(TransactionOutRecord.WALLET_ADDR_LEN):]
            if wallet not in tx_ids:
                tx_ids[wallet] = {}

            if dr.row_dict['Txhash'] not in tx_ids[wallet]:
                tx_ids[wallet][dr.row_dict['Txhash']] = []

            tx_ids[wallet][dr.row_dict['Txhash']]. \
                    append(MergeDataRow(dr, data_files[file_id], file_id))


    for wallet in tx_ids:
        for txn in tx_ids[wallet]:
            for t in tx_ids[wallet][txn]:
                if config.debug:
                    sys.stderr.write("%smerge: %s:%s\n" % (
                        Fore.GREEN,
                        t.data_file_id.ljust(5),
                        t.data_row))

            t_ins, t_outs, t_fee = get_ins_outs(tx_ids[wallet][txn])

            if len(tx_ids[wallet][txn]) == 1:
                # handle airdrops sent directly to my wallet
                do_handle_airdrops(None, t_ins, airdrop_addresses)
                
                continue
            
            t_stakings = []

            if config.debug:
                output_records(t_ins, t_outs, t_fee, t_stakings)
                sys.stderr.write("%smerge:     consolidate:\n" % (Fore.YELLOW))

            consolidate(tx_ids[wallet][txn], [TXNS, INTERNAL_TXNS, TOKENS, NFTS])

            t_ins, t_outs, t_fee = get_ins_outs(tx_ids[wallet][txn])

            if config.debug:
                output_records(t_ins, t_outs, t_fee, t_stakings)
                sys.stderr.write("%smerge:     merge:\n" % (Fore.YELLOW))

            if t_fee:
                fee_quantity = t_fee.t_record.fee_quantity
                fee_asset = t_fee.t_record.fee_asset

            t_ins_orig = copy.copy(t_ins)

            if not t_outs:
                do_handle_airdrops(t_fee, t_ins, airdrop_addresses)
                do_handle_staking_rewards(t_fee, t_ins, staking_addresses)


            # Make trades
            if len(t_ins) == 1 and t_outs:
                do_etherscan_multi_sell(t_ins, t_outs, t_fee)
            elif len(t_outs) == 1 and t_ins:
                do_etherscan_multi_buy(t_ins, t_outs, t_fee)
            elif not t_ins and t_fee:
                do_enter_staking(t_fee, t_outs, staking_addresses, t_stakings)
            elif not t_outs and t_fee:
                do_exit_staking(t_fee, t_ins, staking_addresses, t_stakings)
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

            # Add enter/exit staking txns
            if TOKENS in data_files:
                data_files[TOKENS].data_rows += t_stakings

            merge = True

            if config.debug:
                output_records(t_ins_orig, t_outs, t_fee, t_stakings)

    return merge

def get_ins_outs(tx_ids):
    t_ins = [t.data_row for t in tx_ids if t.data_row.t_record and
             t.data_row.t_record.t_type == TransactionOutRecord.TYPE_DEPOSIT]
    t_outs = [t.data_row for t in tx_ids if t.data_row.t_record and
              t.data_row.t_record.t_type == TransactionOutRecord.TYPE_WITHDRAWAL]
    t_fees = [t.data_row for t in tx_ids if t.data_row.t_record and
              t.data_row.t_record.fee_quantity]

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
            continue

        asset = txn.data_row.t_record.get_asset()
        if asset not in tx_assets:
            tx_assets[asset] = txn
            tx_assets[asset].quantity += txn.data_row.t_record.get_quantity()
        else:
            tx_assets[asset].quantity += txn.data_row.t_record.get_quantity()
            txn.data_row.t_record = None
            tx_ids.remove(txn)

        if txn.data_row.row_dict['Txhash'] == '0x9d804af552cd7515dce8a41261e33906ce5bb26576a11fc2d53b6c22931d72d3':
            print('[DEBUG]', asset, tx_assets[asset].quantity)

    for asset in tx_assets:
        txn = tx_assets[asset]
        if txn.quantity > 0:
            txn.data_row.t_record.t_type = TransactionOutRecord.TYPE_DEPOSIT
            txn.data_row.t_record.buy_asset = asset
            txn.data_row.t_record.buy_quantity = txn.quantity
            txn.data_row.t_record.sell_asset = ''
            txn.data_row.t_record.sell_quantity = None
        elif txn.quantity < 0:
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
                txn.data_row.t_record = None
                tx_ids.remove(txn)

def output_records(t_ins, t_outs, t_fee, t_stakings):
    dup = bool(t_fee and t_fee in t_ins + t_outs + t_stakings)

    if t_fee:
        sys.stderr.write("%smerge:   TR-F%s: %s\n" % (
            Fore.YELLOW, '*' if dup else '', t_fee.t_record))

    for t_in in t_ins:
        sys.stderr.write("%smerge:   TR-I%s: %s\n" % (
            Fore.YELLOW, '*' if t_fee is t_in else '', t_in.t_record))
    for t_out in t_outs:
        sys.stderr.write("%smerge:   TR-O%s: %s\n" % (
            Fore.YELLOW, '*' if t_fee is t_out else '', t_out.t_record))
    for t_staking in t_stakings:
        sys.stderr.write("%smerge:   TR-S%s: %s\n" % (
            Fore.YELLOW, '*' if t_fee is t_staking else '', t_staking.t_record))

def do_handle_staking_rewards(t_fee, t_ins, staking_addresses):
    t_rewards = [t for t in t_ins if
        (t.row_dict['From'] in staking_addresses and
            t.row_dict['ContractAddress'] in staking_addresses[t.row_dict['From']]) or
        (t_fee and t_fee.row_dict['To'] in staking_addresses and
            t.row_dict['ContractAddress'] in staking_addresses[t_fee.row_dict['To']])
    ]

    for t_reward in t_rewards:
        t_reward.t_record.t_type = TransactionOutRecord.TYPE_STAKING
        t_reward.t_record.note = 'Staking Rewards'
        t_ins.remove(t_reward)

        if config.debug:
            sys.stderr.write("%smerge:     staking_rewards_quantity=%s staking_rewards_asset=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(t_reward.t_record.buy_quantity), t_reward.t_record.buy_asset))

def do_handle_airdrops(t_fee, t_ins, airdrop_addresses):
    t_airdrops = [t for t in t_ins if
        (t.row_dict['From'] in airdrop_addresses and
            t.row_dict['ContractAddress'] in airdrop_addresses[t.row_dict['From']]) or
        (t_fee and t_fee.row_dict['To'] in airdrop_addresses and
            t.row_dict['ContractAddress'] in airdrop_addresses[t_fee.row_dict['To']])
    ]

    for t_airdrop in t_airdrops:
        t_airdrop.t_record.t_type = TransactionOutRecord.TYPE_AIRDROP
        t_ins.remove(t_airdrop)

        if config.debug:
            sys.stderr.write("%smerge:     airdrop_quantity=%s airdrop_asset=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(t_airdrop.t_record.buy_quantity), t_airdrop.t_record.buy_asset))

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

    for cnt, t_out in enumerate(t_outs):
        if cnt < len(t_outs) - 1:
            split_buy_quantity = (buy_quantity / len(t_outs)).quantize(PRECISION)
        else:
            # Last t_out, use up remainder
            split_buy_quantity = buy_quantity - tot_buy_quantity
        
        tot_buy_quantity += split_buy_quantity

        if config.debug:
            sys.stderr.write("%smerge:       split_buy_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_buy_quantity)))

        t_out.t_record.t_type = TransactionOutRecord.TYPE_TRADE
        t_out.t_record.buy_quantity = split_buy_quantity
        t_out.t_record.buy_asset = buy_asset
        if t_fee:
            t_out.t_record.note = get_note(t_fee.row_dict)

    assert(tot_buy_quantity == buy_quantity)

    # Remove TR for buy now it's been added to each sell
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

    for cnt, t_in in enumerate(t_ins):
        if cnt < len(t_ins) - 1:
            split_sell_quantity = (sell_quantity / len(t_ins)).quantize(PRECISION)
        else:
            # Last t_in, use up remainder
            split_sell_quantity = sell_quantity - tot_sell_quantity
        
        tot_sell_quantity += split_sell_quantity

        if config.debug:
            sys.stderr.write("%smerge:       split_sell_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_sell_quantity)))

        t_in.t_record.t_type = TransactionOutRecord.TYPE_TRADE
        t_in.t_record.sell_quantity = split_sell_quantity
        t_in.t_record.sell_asset = sell_asset
        if t_fee:
            t_in.t_record.note = get_note(t_fee.row_dict)

    assert(tot_sell_quantity == sell_quantity)

    # Remove TR for sell now it's been added to each buy
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

def do_fee_split(t_all, t_fee, fee_quantity, fee_asset):
    if not t_all:
        assert(t_fee.t_record.fee_quantity == fee_quantity)
        assert(t_fee.t_record.fee_asset == fee_asset)
        return

    if config.debug:
        sys.stderr.write("%smerge:     split fees:\n" % (Fore.YELLOW))
        sys.stderr.write("%smerge:       fee_quantity=%s fee_asset=%s\n" % (
            Fore.YELLOW,
            TransactionOutRecord.format_quantity(fee_quantity), fee_asset))

    tot_fee_quantity = 0

    for cnt, t in enumerate(t_all):
        if cnt < len(t_all) - 1:
            split_fee_quantity = (fee_quantity / len(t_all)).quantize(PRECISION)
        else:
            # Last t, use up remainder
            split_fee_quantity = fee_quantity - tot_fee_quantity if fee_quantity else None

        tot_fee_quantity += split_fee_quantity

        if config.debug:
            sys.stderr.write("%smerge:       split_fee_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_fee_quantity)))

        t.t_record.fee_quantity = split_fee_quantity
        t.t_record.fee_asset = fee_asset
        t.t_record.note = get_note(t_fee.row_dict)

    assert(tot_fee_quantity == fee_quantity)

    # Remove TR for fee now it's been added to each withdrawal
    if t_fee.t_record and t_fee not in t_all:
        if t_fee.t_record.t_type == TransactionOutRecord.TYPE_SPEND:
            t_fee.t_record = None
        else:
            t_fee.t_record.fee_quantity = None
            t_fee.t_record.fee_asset = ''

DataMerge("Etherscan fees & multi-token transactions",
          {TXNS: {'req': DataMerge.MAN, 'obj': etherscan_txns},
           TOKENS: {'req': DataMerge.OPT, 'obj': etherscan_tokens},
           NFTS: {'req': DataMerge.OPT, 'obj': etherscan_nfts},
           INTERNAL_TXNS: {'req': DataMerge.OPT, 'obj': etherscan_int}},
          merge_etherscan)