# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2019

from decimal import Decimal

from ...config import config
from ..out_record import TransactionOutRecord
from ..dataparser import DataParser
from ..exceptions import DataFilenameError

WALLET = "Ethereum"

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

    # Scam tokens
    '0xbc9180be3d8014dd05b53876c487e79d79056022': None,
    '0x5d80a8d8cb80696073e82407968600a37e1dd780': None,
    '0xac4607a2d8a2bfa721955a23d3f290b0e176612e': None,
    '0xfc40ba56a4d5b6c9a69c527bbf4322c4483af3e1': None,
}

def parse_etherscan(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    if row_dict['Status'] != '' and row_dict['Status'] != 'Error(1)':
        # Failed transaction
        row_dict['Value_IN(ETH)'] = row_dict['Value_OUT(ETH)'] = 0

    if Decimal(row_dict['Value_IN(ETH)']) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Value_IN(ETH)'],
                                                 buy_asset="ETH",
                                                 wallet=get_wallet(row_dict['To']),
                                                 note=get_note(row_dict))
    elif Decimal(row_dict['Value_OUT(ETH)']) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['Value_OUT(ETH)'],
                                                 sell_asset="ETH",
                                                 fee_quantity=row_dict['TxnFee(ETH)'],
                                                 fee_asset="ETH",
                                                 wallet=get_wallet(row_dict['From']),
                                                 note=get_note(row_dict))
    else:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_SPEND,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['Value_OUT(ETH)'],
                                                 sell_asset="ETH",
                                                 fee_quantity=row_dict['TxnFee(ETH)'],
                                                 fee_asset="ETH",
                                                 wallet=get_wallet(row_dict['From']),
                                                 note=get_note(row_dict))

def get_wallet(address):
    if address.lower() in config.ethereum_wallets:
        return config.ethereum_wallets[address.lower()]
    
    return "%s:%s" % (WALLET, address.lower()[0:TransactionOutRecord.WALLET_ADDR_LEN])

def get_note(row_dict):
    if row_dict['Status'] != '':
        if row_dict.get('Method'):
            return "%s(%s)" % ('Failure' if row_dict['Status'] == 'Error(1)' else 'Cancelled', row_dict['Method'])
        return "Failure"

    if row_dict.get('Method'):
        return row_dict['Method']

    return row_dict.get('PrivateNote', '')

def parse_etherscan_internal(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    # failed internal transaction
    if row_dict['Status'] != '0':
        row_dict['Value_IN(ETH)'] = row_dict['Value_OUT(ETH)'] = '0'

    if row_dict['TxTo'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Value_IN(ETH)'],
                                                 buy_asset="ETH",
                                                 wallet=get_wallet(row_dict['TxTo']))
    elif row_dict['From'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['Value_OUT(ETH)'],
                                                 sell_asset="ETH",
                                                 wallet=get_wallet(row_dict['From']))

def parse_etherscan_tokens(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    if row_dict['ContractAddress'] in TOKENS:
        asset = TOKENS[row_dict['ContractAddress']] 

        if asset is None:
            # ignore scam tokens
            return
    elif row_dict['TokenSymbol'] in ('UNI-V2', 'SLP') or row_dict['TokenSymbol'].endswith('-LP'):
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
                                                 wallet=get_wallet(row_dict['To']))
    elif row_dict['From'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['Value'].replace(',', ''),
                                                 sell_asset=asset,
                                                 wallet=get_wallet(row_dict['From']))
    else:
        raise DataFilenameError(kwargs['filename'], "Ethereum address")


def parse_etherscan_nfts(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    if not 'TokenValue' in row_dict:
        row_dict['TokenValue'] = '1'

    asset = TOKENS.get(row_dict['ContractAddress'], row_dict['TokenSymbol'])
    asset = '{} [{}]'.format(asset, row_dict['TokenId'])


    if row_dict['To'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=int(row_dict['TokenValue']),
                                                 buy_asset=asset,
                                                 wallet=get_wallet(row_dict['To']))
    elif row_dict['From'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=int(row_dict['TokenValue']),
                                                 sell_asset=asset,
                                                 wallet=get_wallet(row_dict['From']))
    else:
        raise DataFilenameError(kwargs['filename'], "Ethereum address")

etherscan_txns = DataParser(
        DataParser.TYPE_EXPLORER,
        "Etherscan (ETH Transactions)",
        ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
         'Value_IN(ETH)', 'Value_OUT(ETH)', None, 'TxnFee(ETH)', 'TxnFee(USD)',
         'Historical $Price/Eth', 'Status', 'ErrCode'],
        worksheet_name="Etherscan Txns",
        row_handler=parse_etherscan)

DataParser(
        DataParser.TYPE_EXPLORER,
        "Etherscan (ETH Transactions)",
        ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
         'Value_IN(ETH)', 'Value_OUT(ETH)', None, 'TxnFee(ETH)', 'TxnFee(USD)',
         'Historical $Price/Eth', 'Status', 'ErrCode', 'PrivateNote'],
        worksheet_name="Etherscan Txns",
        row_handler=parse_etherscan)

DataParser(DataParser.TYPE_EXPLORER,
           "Etherscan (ETH Transactions)",
           ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
            'Value_IN(ETH)', 'Value_OUT(ETH)', None, 'TxnFee(ETH)', 'TxnFee(USD)',
            'Historical $Price/Eth', 'Status', 'ErrCode', 'Method'],
           worksheet_name="Etherscan Txns",
           row_handler=parse_etherscan)

DataParser(DataParser.TYPE_EXPLORER,
           "Etherscan (ETH Transactions)",
           ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
            'Value_IN(ETH)', 'Value_OUT(ETH)', None, 'TxnFee(ETH)', 'TxnFee(USD)',
            'Historical $Price/Eth', 'Status', 'ErrCode', 'Method', 'PrivateNote'],
           worksheet_name="Etherscan Txns",
           row_handler=parse_etherscan)

etherscan_int = DataParser(
        DataParser.TYPE_EXPLORER,
        "Etherscan (ETH Internal Transactions)",
        ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
         'ParentTxETH_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(ETH)',
         'Value_OUT(ETH)', None, 'Historical $Price/Eth', 'Status', 'ErrCode', 'Type'],
        worksheet_name="Etherscan Internal Txns",
        row_handler=parse_etherscan_internal)

DataParser(DataParser.TYPE_EXPLORER,
           "Etherscan (ETH Internal Transactions)",
           ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
            'ParentTxETH_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(ETH)',
            'Value_OUT(ETH)', None, 'Historical $Price/Eth', 'Status', 'ErrCode', 'Type',
            'PrivateNote'],
           worksheet_name="Etherscan Internal Txns",
           row_handler=parse_etherscan_internal)

etherscan_tokens = DataParser(
        DataParser.TYPE_EXPLORER,
        "Etherscan (ERC-20 Tokens)",
        ['Txhash', 'UnixTimestamp', 'DateTime', 'From', 'To', 'Value', 'ContractAddress',
         'TokenName', 'TokenSymbol'],
        worksheet_name="Etherscan Tokens",
        row_handler=parse_etherscan_tokens)

DataParser(
        DataParser.TYPE_EXPLORER,
        "Etherscan (ERC-20 Tokens)",
        ['Txhash','Blockno','UnixTimestamp','DateTime','From','To','TokenValue',
         'USDValueDayOfTx','ContractAddress','TokenName','TokenSymbol'],
        worksheet_name="Etherscan Tokens",
        row_handler=parse_etherscan_tokens)

etherscan_nfts = DataParser(
        DataParser.TYPE_EXPLORER,
        "Etherscan (ERC-721/ERC-1155 NFTs)",
        ['Txhash', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress', 'TokenId',
         'TokenName', 'TokenSymbol'],
        worksheet_name="Etherscan NFTs",
        row_handler=parse_etherscan_nfts)

DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ERC-721/ERC-1155 NFTs)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress', 'TokenId',
     'TokenName', 'TokenSymbol'],
    worksheet_name="Etherscan NFTs",
    row_handler=parse_etherscan_nfts)

DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ERC-721/ERC-1155 NFTs)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress', 'TokenId',
     'TokenValue', 'TokenName', 'TokenSymbol'],
    worksheet_name="Etherscan NFTs",
    row_handler=parse_etherscan_nfts)

