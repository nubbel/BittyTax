# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2021

import re
from decimal import Decimal

from ..out_record import TransactionOutRecord
from ..dataparser import DataParser
from ..exceptions import UnexpectedTradingPairError, UnexpectedTypeError
from ..output_csv import OutputBase

WALLET = "dYdX"

def parse_transfers(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['createdAt'])

    if row_dict['status'] != 'CONFIRMED':
        return

    if row_dict['type'] == 'DEPOSIT':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['creditAmount'],
                                                 buy_asset=row_dict['creditAsset'],
                                                 wallet=get_wallet(kwargs['filename']))

    elif row_dict['type'] == 'WITHDRAWAL':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['debitAmount'],
                                                 sell_asset=row_dict['debitAsset'],
                                                 wallet=get_wallet(kwargs['filename']))

def parse_trades(data_row, parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['createdAt'])

    asset, usd = row_dict['market'].split('-')

    if usd != 'USD':
        raise UnexpectedTradingPairError(parser.in_header.index('market'), 'market',
                                         row_dict['market'])

    quantity = Decimal(row_dict['price']) * Decimal(row_dict['size'])
    fee = Decimal(row_dict['fee'])

    if row_dict['side'] == 'BUY':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['size'],
                                                 buy_asset=asset,
                                                 sell_quantity=quantity - fee,
                                                 sell_asset='USDC',
                                                 fee_quantity=fee,
                                                 fee_asset='USDC',
                                                 wallet=get_wallet(kwargs['filename']))

    elif row_dict['side'] == 'SELL':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                 data_row.timestamp,
                                                 buy_quantity=quantity,
                                                 buy_asset='USDC',
                                                 sell_quantity=row_dict['size'],
                                                 sell_asset=asset,
                                                 fee_quantity=fee,
                                                 fee_asset='USDC',
                                                 wallet=get_wallet(kwargs['filename']))

def parse_funding(data_row, parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['effectiveAt'])

    _, usd = row_dict['market'].split('-')

    if usd != 'USD':
        raise UnexpectedTradingPairError(parser.in_header.index('market'), 'market',
                                         row_dict['market'])

    payment = Decimal(row_dict['payment'])

    if payment > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_INCOME,
                                                 data_row.timestamp,
                                                 buy_quantity=payment,
                                                 buy_asset='USDC',
                                                 wallet=get_wallet(kwargs['filename']),
                                                 note='Funding received')

    elif payment < 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_SPEND,
                                                 data_row.timestamp,
                                                 sell_quantity=abs(payment),
                                                 sell_asset='USDC',
                                                 wallet=get_wallet(kwargs['filename']),
                                                 note='Funding paid')

def get_wallet(filename):
    match = re.search(r"0x[a-f0-9]{40}", filename.lower())
    
    if not match:
        return WALLET

    return "%s:%s" % (WALLET, match.group()[0:TransactionOutRecord.WALLET_ADDR_LEN])

DataParser(DataParser.TYPE_EXCHANGE,
           "dYdX Transfers",
           ['createdAt','type','debitAsset','creditAsset','debitAmount','creditAmount',
            'transactionHash','status','confirmedAt','fromAddress','toAddress'],
           row_handler=parse_transfers)

DataParser(DataParser.TYPE_EXCHANGE,
           "dYdX Trades",
           ['createdAt','side','liquidity','type','market','price','size','fee'],
           row_handler=parse_trades)

DataParser(DataParser.TYPE_EXCHANGE,
           "dYdX Funding",
           ['effectiveAt','market','payment','rate','positionSize','price'],
           row_handler=parse_funding)
