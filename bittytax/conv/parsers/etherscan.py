# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2019

from decimal import Decimal
import re

from ...config import config
from ..out_record import TransactionOutRecord
from ..dataparser import DataParser
from ..exceptions import DataFilenameError

TOKENS = {
    '0x3b3d4eefdc603b232907a7f3d0ed1eea5c62b5f7': 'UNI-V2-STAKE-WETH-LP',
    '0xa478c2975ab1ea89e8196811f51a7b7ade33eb11': 'UNI-V2-DAI-ETH',
    '0xc5be99a02c6857f9eac67bbce58df5572498f40c': 'UNI-V2-ETH-AMPL',
    '0x6f23d2fedb4ff4f1e9f8c521f66e5f2a1451b6f3': 'UNI-V2-MARK-ETH',
    '0x4d3c5db2c68f6859e0cd05d080979f597dd64bff': 'UNI-V2-MVI-ETH',
    '0x173f8ee61c0fe712cae2a2fc8d5c0ccdda57e6da': 'SNOW-yVault-USD',
    '0x9461173740d27311b176476fa27e94c681b1ea6b': 'SLP-yvBOOST-ETH',
    '0xced67a187b923f0e5ebcc77c7f2f7da20099e378': 'pSLP-yvBOOST-ETH',
    '0x5dbcf33d8c2e976c6b560249878e6f1491bca25c': 'yUSD',
    '0x888888888877a56b4b809bf14bb76d63eb208297': 'OPIUM-NFT',
    '0x35f5a420ef9bcc748329021fbd4ed0986abdf201': 'YEARN-NFT',
    '0xdb25ca703181e7484a155dd612b06f57e12be5f0': 'yvYFI-V2',

    # Avalanche
    '0xa3f1f5076499ec37d5bb095551f85ab5a344bb58': 'JLP-MIM-SDOG',

    # Gnosis
    '0x4505b262dc053998c10685dc5f9098af8ae5c8ad': 'HNY-WXDAI-LP',
    '0x298c7326a6e4a6108b88520f285c7dc89403347d': 'HNY-STAKE-LP',
    '0x9e8e5e4a0900fe4634c02aaf0f130cfb93c53fbc': 'XCOMB-WXDAI-LP',
    '0x50a4867aee9cafd6ddc84de3ce59df027cb29084': 'AGVE-HNY-LP',
    '0x159eb41b54ae70d912f3e426bfdfa19888faa807': 'HNY-COLD-LP',

    # Polygon
    '0xc4e595acdd7d12fec385e5da5d43160e8a0bac0e': 'SLP-WMATIC-WETH',
    '0x019ba0325f1988213d448b3472fa1cf8d07618d7': 'UNI-V2-WMATIC-QUICK',
    '0xcd353f79d9fade311fc3119b841e1f456b54e858': 'SLP-WMATIC-USDC',
    '0xc1dd63ca154837ac4356d888f7c10fbbe442407e': 'APE-LP-WMATIC-YELD',
    # '0x949fdf28f437258e7564a35596b1a99b24f81e4e': 'PLP-WMATIC-WETH',
    # '0xa0273c10b8a4bf0bdc57cb0bc974e3a9d89527b8': 'PLP-WMATIC-WETH',
    '0x6e7a5fafcec6bb1e78bae2a1f0b612012bf14827': 'UNI-V2-WMATIC-USDC',
    '0xadbf1854e5883eb8aa7baf50705338739e558e5b': 'UNI-V2-WMATIC-WETH',

    # Scam tokens
    '0xbc9180be3d8014dd05b53876c487e79d79056022': None,
    '0x5d80a8d8cb80696073e82407968600a37e1dd780': None,
    '0xac4607a2d8a2bfa721955a23d3f290b0e176612e': None,
    '0xfc40ba56a4d5b6c9a69c527bbf4322c4483af3e1': None,
    '0xd03392cf4f60fab2fbea38b7d2b826c70b0208a0': None,
    '0xded4a9caf43dcc92a19b16f617031f07c5f236f9': None,
    '0x4c4f4f4122c3a80d30c1ad6ad2828953015bd52c': None,
    '0xf9d922c055a3f1759299467dafafdf43be844f7a': None,
    '0x8a0b040f27407d7a603bca701b857f8f81a1c7af': None,
    '0x19a935cbaaa4099072479d521962588d7857d717': None,
    '0xdad0d08d5b0544fc853682c6ca07eaab201bd550': None,
    '0x9fd4969573f9dec7882409709c9b35f2dc3074ca': None,
    '0xa9316e1909edf3ed33b0dd1c6631c50b82c6e142': None,
    '0xaaa5b9e6c589642f98a1cda99b9d024b8407285a': None,
    '0x8ae127d224094cb1b27e1b28a472e588cbcc7620': None,
    '0xe4fb1bb8423417a460286b0ed44b64e104c5fae5': None,
    '0x0ef2603cd156e1934e19d0b07cd64f415e1e7940': None,
    '0xe4fb1bb8423417a460286b0ed44b64e104c5fae5': None,
    '0xcbf4ab00b6aa19b4d5d29c7c3508b393a1c01fe3': None,
    '0xe4fb1bb8423417a460286b0ed44b64e104c5fae5': None,
    '0x81067076dcb7d3168ccf7036117b9d72051205e2': None,
    '0xa39b14f57087aa5f16b941e5ec182b84a5432aa7': None,
    '0x81067076dcb7d3168ccf7036117b9d72051205e2': None,
    '0x02677c45fa858b9ffec24fc791bf72cdf4a8a8df': None,
    '0xa85f8a198d59f0fda82333be9aeeb50f24dd59ff': None,
    '0xc68e83a305b0fad69e264a1769a0a070f190d2d6': None,
    '0x22e51bae3f545255e115090202a23c7ede0b00b9': None,
    '0x14f2c84a58e065c846c5fdddade0d3548f97a517': None,
    '0x95e9464b5cc3bf81210259812b51665a437aa11b': None,
    '0x3c0bd2118a5e61c41d2adeebcb8b7567fde1cbaf': None,
    '0xf31cdb090d1d4b86a7af42b62dc5144be8e42906': None,
    '0xdc8fa3fab8421ff44cc6ca7f966673ff6c0b3b58': None,
    '0x0f1f17e4260515d9bfe805cff323374eb771eae6': None,
    '0x5229cadb824fd5117f00e3614c138b62f2bd3156': None,
    '0x8a6b62f5501410d179641e731a8f1cecef1c28ec': None,
    '0x3a8ffb79435c967a565ce9d2134b1254d9c5e1a5': None,
    '0xcf68f02d7dd6a4642ae6a77f6a3676d0cbc834c9': None,
    '0xf9d3d8b25b95bcda979025b74fdfa7ac3f380f9f': None,
    '0xa23cd4da2400b31bcf18e3a8f27578c706dbf902': None,
    '0x6b748903c3d9135acf65f5be488f4be06d557f82': None,
    '0x6142f62e7996faec5c5bb9d10669d60299d69dfe': None,
    '0x8a953cfe442c5e8855cc6c61b1293fa648bae472': None,
    '0x0b91b07beb67333225a5ba0259d55aee10e3a578': None,
    '0xaf6b1a3067bb5245114225556e5b7a52cf002752': None,
    '0x5de9eb5baa578666dfcdc00d3b2ee3f94c4f1c55': None,
    '0x1c0aaf256f581774e21d1fae64244c6676bff04c': None,
    '0x8888888889953bdaa9a7273ba13c80918823ba71': None,
    '0x68c929e7b8fb06c58494a369f6f088fff28f7c77': None,
}

NETWORKS = {
    'ETH': {
        'name': 'Ethereum',
        'explorer': 'Etherscan',
    },
    'AVAX': {
        'name': 'Avalanche',
        'explorer': 'Snowtrace',
    },
    'xDAI': {
        'name': 'Gnosis',
        'explorer': 'Gnosisscan',
    },
    'MATIC': {
        'name': 'Polygon',
        'explorer': 'Polygonscan',
    },
}

def parse_etherscan(data_row, parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    native_asset = kwargs['cryptoasset'] if kwargs['cryptoasset'] else 'ETH'
    if parser.args[0]:
        native_asset = parser.args[0].group(1)

    network = NETWORKS[native_asset]

    value_in = row_dict[f'Value_IN({native_asset})']
    value_out = row_dict[f'Value_OUT({native_asset})']
    fee = row_dict[f'TxnFee({native_asset})']

    if row_dict['Status'] != '' and row_dict['Status'] != 'Error(1)':
        # Failed transaction
        value_in = value_out = 0

    if Decimal(value_in) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=value_in,
                                                 buy_asset=native_asset,
                                                 wallet=get_wallet(row_dict['To'], network['name']),
                                                 note=get_note(row_dict))
    elif Decimal(value_out) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=value_out,
                                                 sell_asset=native_asset,
                                                 fee_quantity=fee,
                                                 fee_asset=native_asset,
                                                 wallet=get_wallet(row_dict['From'], network['name']),
                                                 note=get_note(row_dict))
    else:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_SPEND,
                                                 data_row.timestamp,
                                                 sell_quantity=value_out,
                                                 sell_asset=native_asset,
                                                 fee_quantity=fee,
                                                 fee_asset=native_asset,
                                                 wallet=get_wallet(row_dict['From'], network['name']),
                                                 note=get_note(row_dict))

def get_wallet(address, network):
    if address.lower() in config.ethereum_wallets:
        return config.ethereum_wallets[address.lower()]
    
    return "%s:%s" % (network, address.lower()[0:TransactionOutRecord.WALLET_ADDR_LEN])

def get_note(row_dict):
    if row_dict['Status'] != '':
        if row_dict.get('Method'):
            return "%s(%s)" % ('Failure' if row_dict['Status'] == 'Error(1)' else 'Cancelled', row_dict['Method'])
        return "Failure"

    if row_dict.get('Method'):
        return row_dict['Method']

    return row_dict.get('PrivateNote', '')

def parse_etherscan_internal(data_row, parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    native_asset = kwargs['cryptoasset'] if kwargs['cryptoasset'] else 'ETH'
    if parser.args[0]:
        native_asset = parser.args[0].group(1)

    network = NETWORKS[native_asset]

    value_in = row_dict[f'Value_IN({native_asset})']
    value_out = row_dict[f'Value_OUT({native_asset})']

    if row_dict['Status'] != '0':
        # Failed transaction
        value_in = value_out = 0


    if row_dict['TxTo'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=value_in,
                                                 buy_asset=native_asset,
                                                 wallet=get_wallet(row_dict['TxTo'], network['name']))
    elif row_dict['From'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=value_out,
                                                 sell_asset=native_asset,
                                                 wallet=get_wallet(row_dict['From'], network['name']))

def parse_etherscan_tokens(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    native_asset = kwargs['cryptoasset'] if kwargs['cryptoasset'] else 'ETH'
    network = NETWORKS[native_asset]

    if row_dict['ContractAddress'] in TOKENS:
        asset = TOKENS[row_dict['ContractAddress']] 

        if asset is None:
            # ignore scam tokens
            return
    elif row_dict['TokenSymbol'] in ('UNI-V2', 'SLP', 'pSLP', 'JLP', 'PLP') or row_dict['TokenSymbol'].endswith('-LP'):
        asset = row_dict['TokenSymbol'] + '-' + row_dict['ContractAddress'][0:10]
    else:
        asset = row_dict['TokenSymbol']

    if 'TokenValue' in row_dict:
        row_dict['Value'] = row_dict['TokenValue']

    if row_dict['To'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Value'].replace(',', ''),
                                                 buy_asset=asset,
                                                 wallet=get_wallet(row_dict['To'], network['name']))
    elif row_dict['From'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['Value'].replace(',', ''),
                                                 sell_asset=asset,
                                                 wallet=get_wallet(row_dict['From'], network['name']))
    else:
        raise DataFilenameError(kwargs['filename'], "Ethereum address")


def parse_etherscan_nfts(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    native_asset = kwargs['cryptoasset'] if kwargs['cryptoasset'] else 'ETH'
    network = NETWORKS[native_asset]

    if not 'TokenValue' in row_dict:
        row_dict['TokenValue'] = '1'

    asset = TOKENS.get(row_dict['ContractAddress'], row_dict['TokenSymbol'])
    asset = '{} [{}]'.format(asset, row_dict['TokenId'])


    if row_dict['To'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=int(row_dict['TokenValue']),
                                                 buy_asset=asset,
                                                 wallet=get_wallet(row_dict['To'], network['name']))
    elif row_dict['From'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=int(row_dict['TokenValue']),
                                                 sell_asset=asset,
                                                 wallet=get_wallet(row_dict['From'], network['name']))
    else:
        raise DataFilenameError(kwargs['filename'], "Ethereum address")


etherscan_txns = DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ETH Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
     lambda c: re.match(r"Value_IN\((\w+)\)", c), lambda c: re.match(r"Value_OUT\((\w+)\)", c)
     , None, lambda c: re.match(r"TxnFee\((\w+)\)", c), 'TxnFee(USD)',
     None, 'Status', 'ErrCode'],
    worksheet_name="Txns",
    row_handler=parse_etherscan)

DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ETH Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
     lambda c: re.match(r"Value_IN\((\w+)\)", c), lambda c: re.match(r"Value_OUT\((\w+)\)", c)
     , None, lambda c: re.match(r"TxnFee\((\w+)\)", c), 'TxnFee(USD)',
     None, 'Status', 'ErrCode', 'PrivateNote'],
    worksheet_name="Txns",
    row_handler=parse_etherscan)

DataParser(DataParser.TYPE_EXPLORER,
           "Etherscan (ETH Transactions)",
           ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
            lambda c: re.match(r"Value_IN\((\w+)\)", c), lambda c: re.match(r"Value_OUT\((\w+)\)", c), None,
            lambda c: re.match(r"TxnFee\((\w+)\)", c), 'TxnFee(USD)',
            None, 'Status', 'ErrCode', 'Method'],
           worksheet_name="Txns",
           row_handler=parse_etherscan)

DataParser(DataParser.TYPE_EXPLORER,
           "Etherscan (ETH Transactions)",
           ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
            lambda c: re.match(r"Value_IN\((\w+)\)", c), lambda c: re.match(r"Value_OUT\((\w+)\)", c),
            None, lambda c: re.match(r"TxnFee\((\w+)\)", c), 'TxnFee(USD)',
            None, 'Status', 'ErrCode', 'Method', 'PrivateNote'],
           worksheet_name="Txns",
           row_handler=parse_etherscan)

etherscan_int = DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ETH Internal Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
     None, 'From', 'TxTo', 'ContractAddress', lambda c: re.match(r"Value_IN\((\w+)\)", c),
     lambda c: re.match(r"Value_OUT\((\w+)\)", c), None, None, 'Status', 'ErrCode', 'Type'],
    worksheet_name="Internal Txns",
    row_handler=parse_etherscan_internal)

DataParser(DataParser.TYPE_EXPLORER,
           "Etherscan (ETH Internal Transactions)",
           ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
            None, 'From', 'TxTo', 'ContractAddress', lambda c: re.match(r"Value_IN\((\w+)\)", c),
            lambda c: re.match(r"Value_OUT\((\w+)\)", c), None, None, 'Status', 'ErrCode', 'Type',
            'PrivateNote'],
           worksheet_name="Internal Txns",
           row_handler=parse_etherscan_internal)

etherscan_tokens = DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ERC-20 Tokens)",
    ['Txhash', 'UnixTimestamp', 'DateTime', 'From', 'To', 'Value', 'ContractAddress',
     'TokenName', 'TokenSymbol'],
    worksheet_name="Tokens",
    row_handler=parse_etherscan_tokens)

DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ERC-20 Tokens)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'TokenValue',
     'USDValueDayOfTx', 'ContractAddress', 'TokenName', 'TokenSymbol'],
    worksheet_name="Tokens",
    row_handler=parse_etherscan_tokens)

etherscan_nfts = DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ERC-721/ERC-1155 NFTs)",
    ['Txhash', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress', 'TokenId',
     'TokenName', 'TokenSymbol'],
    worksheet_name="NFTs",
    row_handler=parse_etherscan_nfts)

DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ERC-721/ERC-1155 NFTs)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress', 'TokenId',
     'TokenName', 'TokenSymbol'],
    worksheet_name="NFTs",
    row_handler=parse_etherscan_nfts)

DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ERC-721/ERC-1155 NFTs)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress', 'TokenId',
     'TokenValue', 'TokenName', 'TokenSymbol'],
    worksheet_name="NFTs",
    row_handler=parse_etherscan_nfts)
