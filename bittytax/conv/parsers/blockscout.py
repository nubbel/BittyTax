# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2019

from decimal import Decimal

from bittytax.conv.parsers.etherscan import TOKENS

from ...config import config
from ..out_record import TransactionOutRecord
from ..dataparser import DataParser
from ..exceptions import DataFilenameError

WALLET = 'xDAI'

XDAI_FAUCET = '0x97aae423c9a1bc9cf2d81f9f1299b117a7b01136'
TOKENS = {}

def parse_blockscout(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['UnixTimestamp'])

    value = Decimal(row_dict['Value(xDAI)']) / 10**18

    if row_dict['Type'] == 'IN':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=value,
                                                 buy_asset="xDAI",
                                                 wallet=get_wallet(row_dict['ToAddress']),
                                                 note=get_note(row_dict))
    elif row_dict['Type'] == 'OUT':
        fee = Decimal(row_dict['Fee(xDAI)']) / 10**18

        if value > 0:
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                    data_row.timestamp,
                                                    sell_quantity=value,
                                                    sell_asset="xDAI",
                                                    fee_quantity=fee,
                                                    fee_asset="xDAI",
                                                    wallet=get_wallet(row_dict['FromAddress']),
                                                    note=get_note(row_dict))
        else:
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_SPEND,
                                                    data_row.timestamp,
                                                    sell_quantity=value,
                                                    sell_asset="xDAI",
                                                    fee_quantity=fee,
                                                    fee_asset="xDAI",
                                                    wallet=get_wallet(row_dict['FromAddress']),
                                                    note=get_note(row_dict))

def parse_blockscout_internal(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['UnixTimestamp'])

    # failed internal transaction
    if row_dict['ErrCode'] != '':
        row_dict['Value(xDAI)'] = '0'

    value = Decimal(row_dict['Value(xDAI)']) / 10**18

    if row_dict['ToAddress'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=value,
                                                 buy_asset="xDAI",
                                                 wallet=get_wallet(row_dict['ToAddress']))
    elif row_dict['FromAddress'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=value,
                                                 sell_asset="xDAI",
                                                 wallet=get_wallet(row_dict['FromAddress']))

def parse_blockscout_tokens(data_row, _parser, **_kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['UnixTimestamp'])

    if row_dict['TokensTransferred'] in ['', '1']:
        # ERC-1155 token (NFT or HoneyFarm Deposit)
        return

    asset = TOKENS[row_dict['TokenContractAddress']] if row_dict['TokenContractAddress'] in TOKENS else row_dict['TokenSymbol']
    value = Decimal(row_dict['TokensTransferred']) / 10**18

    if row_dict['Type'] == 'IN':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=value,
                                                 buy_asset=asset,
                                                 wallet=get_wallet(row_dict['ToAddress']))
    elif row_dict['Type'] == 'OUT':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=value,
                                                 sell_asset=asset,
                                                 wallet=get_wallet(row_dict['FromAddress']))

def get_note(row_dict):
    if row_dict['Status'] == 'error':
        return 'Cancelled'

    return ''

def get_wallet(address):
    if address.lower() in config.ethereum_wallets:
        return config.ethereum_wallets[address.lower()].replace('Ethereum', 'xDAI')
    
    return WALLET


blockscout_txns = DataParser(
        DataParser.TYPE_EXPLORER,
        "Blockscout (xDAI Transactions)",
        ['TxHash','BlockNumber','UnixTimestamp','FromAddress','ToAddress',
         'ContractAddress','Type','Value(xDAI)','Fee(xDAI)',
         'Status','ErrCode',
         'CurrentPrice','TxDateOpeningPrice','TxDateClosingPrice'],
        worksheet_name="Blockscout(xDAI Transactions)",
        row_handler=parse_blockscout)

blockscout_internal_txns = DataParser(
        DataParser.TYPE_EXPLORER,
        "Blockscout (xDAI Internal Transactions)",
        ['TxHash','Index','BlockNumber','BlockHash','TxIndex','BlockIndex',
        'UnixTimestamp','FromAddress','ToAddress','ContractAddress','Type',
        'CallType','Gas','GasUsed','Value(xDAI)','Input','Output','ErrCode'],
        worksheet_name="Blockscout(xDAI Internal Transactions)",
        row_handler=parse_blockscout_internal)

blockscout_token = DataParser(
        DataParser.TYPE_EXPLORER,
        "Blockscout (xDAI Token Transactions)",
        ['TxHash','BlockNumber','UnixTimestamp','FromAddress','ToAddress',
        'TokenContractAddress','Type','TokenSymbol','TokensTransferred',
        'TransactionFee(xDAI)','Status','ErrCode'],
        worksheet_name="Blockscout(xDAI Token Transactions)",
        row_handler=parse_blockscout_tokens)



