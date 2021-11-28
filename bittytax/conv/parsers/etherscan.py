# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2019

from decimal import Decimal

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
    '0x173f8ee61c0fe712cae2a2fc8d5c0ccdda57e6da': 'snowyDAI+yUSDC+yUSDT+yTUSD',
    '0xced67a187b923f0e5ebcc77c7f2f7da20099e378': 'pSLP-yvBOOST-ETH'
}

def parse_etherscan(data_row, _parser, **_kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    if row_dict['Status'] != '':
        # Failed txns should not have a Value_OUT
        row_dict['Value_OUT(ETH)'] = 0

    if Decimal(row_dict['Value_IN(ETH)']) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Value_IN(ETH)'],
                                                 buy_asset="ETH",
                                                 wallet=WALLET,
                                                 note=get_note(row_dict))
    elif Decimal(row_dict['Value_OUT(ETH)']) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['Value_OUT(ETH)'],
                                                 sell_asset="ETH",
                                                 fee_quantity=row_dict['TxnFee(ETH)'],
                                                 fee_asset="ETH",
                                                 wallet=WALLET,
                                                 note=get_note(row_dict))
    else:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_SPEND,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['Value_OUT(ETH)'],
                                                 sell_asset="ETH",
                                                 fee_quantity=row_dict['TxnFee(ETH)'],
                                                 fee_asset="ETH",
                                                 wallet=WALLET,
                                                 note=get_note(row_dict))

def get_note(row_dict):
    if row_dict['Status'] != '':
        if row_dict.get('Method'):
            return "Failure (%s)" % row_dict['Method']
        return "Failure"

    if row_dict.get('Method'):
        return row_dict['Method']

    return row_dict.get('PrivateNote', '')

def parse_etherscan_internal(data_row, _parser, **_kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    if Decimal(row_dict['Value_IN(ETH)']) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Value_IN(ETH)'],
                                                 buy_asset="ETH",
                                                 wallet=WALLET)
    elif Decimal(row_dict['Value_OUT(ETH)']) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['Value_OUT(ETH)'],
                                                 sell_asset="ETH",
                                                 wallet=WALLET)

def parse_etherscan_tokens(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    asset = TOKENS[row_dict['ContractAddress']] if row_dict['ContractAddress'] in TOKENS else row_dict['TokenSymbol']

    if row_dict['To'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Value'].replace(',', ''),
                                                 buy_asset=asset,
                                                 wallet=WALLET)
    elif row_dict['From'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['Value'].replace(',', ''),
                                                 sell_asset=asset,
                                                 wallet=WALLET)
    else:
        raise DataFilenameError(kwargs['filename'], "Ethereum address")

def parse_etherscan_nfts(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['UnixTimestamp']))

    asset = TOKENS[row_dict['ContractAddress']] if row_dict['ContractAddress'] in TOKENS else row_dict['TokenSymbol']

    if row_dict['To'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=1,
                                                 buy_asset=asset,
                                                 wallet=WALLET)
    elif row_dict['From'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=1,
                                                 sell_asset=asset,
                                                 wallet=WALLET)
    else:
        raise DataFilenameError(kwargs['filename'], "Ethereum address")

etherscan_txns = DataParser(
        DataParser.TYPE_EXPLORER,
        "Etherscan (ETH Transactions)",
        ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
         'Value_IN(ETH)', 'Value_OUT(ETH)', None, 'TxnFee(ETH)', 'TxnFee(USD)',
         'Historical $Price/Eth', 'Status', 'ErrCode'],
        worksheet_name="Etherscan",
        row_handler=parse_etherscan)

DataParser(
        DataParser.TYPE_EXPLORER,
        "Etherscan (ETH Transactions)",
        ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
         'Value_IN(ETH)', 'Value_OUT(ETH)', None, 'TxnFee(ETH)', 'TxnFee(USD)',
         'Historical $Price/Eth', 'Status', 'ErrCode', 'PrivateNote'],
        worksheet_name="Etherscan",
        row_handler=parse_etherscan)

DataParser(DataParser.TYPE_EXPLORER,
           "Etherscan (ETH Transactions)",
           ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
            'Value_IN(ETH)', 'Value_OUT(ETH)', None, 'TxnFee(ETH)', 'TxnFee(USD)',
            'Historical $Price/Eth', 'Status', 'ErrCode', 'Method'],
           worksheet_name="Etherscan",
           row_handler=parse_etherscan)

DataParser(DataParser.TYPE_EXPLORER,
           "Etherscan (ETH Transactions)",
           ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
            'Value_IN(ETH)', 'Value_OUT(ETH)', None, 'TxnFee(ETH)', 'TxnFee(USD)',
            'Historical $Price/Eth', 'Status', 'ErrCode', 'Method', 'PrivateNote'],
           worksheet_name="Etherscan",
           row_handler=parse_etherscan)

DataParser(DataParser.TYPE_EXPLORER,
           "Etherscan (ETH Internal Transactions)",
           ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
            'ParentTxETH_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(ETH)',
            'Value_OUT(ETH)', None, 'Historical $Price/Eth', 'Status', 'ErrCode', 'Type'],
           worksheet_name="Etherscan",
           row_handler=parse_etherscan_internal)

DataParser(DataParser.TYPE_EXPLORER,
           "Etherscan (ETH Internal Transactions)",
           ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
            'ParentTxETH_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(ETH)',
            'Value_OUT(ETH)', None, 'Historical $Price/Eth', 'Status', 'ErrCode', 'Type',
            'PrivateNote'],
           worksheet_name="Etherscan",
           row_handler=parse_etherscan_internal)

etherscan_tokens = DataParser(
        DataParser.TYPE_EXPLORER,
        "Etherscan (ERC-20 Tokens)",
        ['Txhash', 'UnixTimestamp', 'DateTime', 'From', 'To', 'Value', 'ContractAddress',
         'TokenName', 'TokenSymbol'],
        worksheet_name="Etherscan",
        row_handler=parse_etherscan_tokens)

DataParser(DataParser.TYPE_EXPLORER,
           "Etherscan (ERC-721 NFTs)",
           ['Txhash', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress', 'TokenId',
            'TokenName', 'TokenSymbol'],
           worksheet_name="Etherscan",
           row_handler=parse_etherscan_nfts)
