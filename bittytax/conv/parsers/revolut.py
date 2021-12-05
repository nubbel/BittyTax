# -*- coding: utf-8 -*-

from decimal import Decimal

from ...config import config
from ..out_record import TransactionOutRecord
from ..dataparser import DataParser

WALLET = 'Revolut'

def parse_revolut(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['Completed Date'])

    if row_dict['State'] != 'COMPLETED':
        return

    wallet = "%s:%s" % (WALLET, row_dict['Product'])
    note = row_dict['Description']

    if row_dict['Type'] != 'EXCHANGE' and row_dict['Currency'] in config.fiat_list:
        # ignore fiat transactions that are not trades
        return

    if Decimal(row_dict['Amount']) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Amount'],
                                                 buy_asset=row_dict['Currency'],
                                                 fee_quantity=row_dict['Fee'],
                                                 fee_asset=row_dict['Currency'],
                                                 wallet=wallet,
                                                 note=note)
    elif Decimal(row_dict['Amount']) < 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                data_row.timestamp,
                                                sell_quantity=abs(Decimal(row_dict['Amount'])),
                                                sell_asset=row_dict['Currency'],
                                                fee_quantity=row_dict['Fee'],
                                                fee_asset=row_dict['Currency'],
                                                wallet=wallet,
                                                note=note)

revolut_txns = DataParser(DataParser.TYPE_EXCHANGE,
                          "revolut",
                          ['Type','Product','Started Date','Completed Date',
                           'Description','Amount','Fee','Currency','State','Balance'],
                           worksheet_name="revolut",
                           row_handler=parse_revolut)
