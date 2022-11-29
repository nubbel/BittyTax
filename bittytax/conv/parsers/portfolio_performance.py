# -*- coding: utf-8 -*-

from decimal import Decimal
import os

from ...config import config
from ..out_record import TransactionOutRecord
from ..dataparser import DataParser

def parse_portfolio_performance_deposit(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['Date'])

    wallet = get_wallet(kwargs['filename'])
    note = row_dict['Note']

    if row_dict['Type'] in ['Transfer (Inbound)', 'Deposit']:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Value'].replace(',', ''),
                                                 buy_asset=row_dict['Transaction Currency'],
                                                 wallet=wallet,
                                                 note=note)
    elif row_dict['Type'] == 'Transfer (Outbound)':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                data_row.timestamp,
                                                sell_quantity=abs(Decimal(row_dict['Value'].replace(',', ''))),
                                                sell_asset=row_dict['Transaction Currency'],
                                                wallet=wallet,
                                                note=note)
    elif row_dict['Type'] == 'Dividend':
        shares = Decimal(row_dict['Shares'])
        note = "%s share(s) of %s" % ('{:0,.2f}'.format(shares), row_dict['Security Name'])

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DIVIDEND,
                                                data_row.timestamp,
                                                buy_quantity=row_dict['Value'].replace(',', ''),
                                                buy_asset=row_dict['Transaction Currency'],
                                                wallet=wallet,
                                                note=note)
    elif row_dict['Type'] == 'Interest':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_INTEREST,
                                                data_row.timestamp,
                                                buy_quantity=row_dict['Value'].replace(',', ''),
                                                buy_asset=row_dict['Transaction Currency'],
                                                wallet=wallet,
                                                note=note)
    elif row_dict['Type'] == 'Fees':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_SPEND,
                                                data_row.timestamp,
                                                sell_quantity=abs(Decimal(row_dict['Value'].replace(',', ''))),
                                                sell_asset=row_dict['Transaction Currency'],
                                                wallet=wallet,
                                                note=note)

def parse_portfolio_performance_security(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['Date'])

    wallet = get_wallet(kwargs['filename'])
    note = row_dict['Note']

    if row_dict['Type'] == 'Buy':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Shares'],
                                                 buy_asset=get_asset(row_dict),
                                                 sell_quantity=row_dict['Value'].replace(',', ''),
                                                 sell_asset=row_dict['Transaction Currency'],
                                                 fee_quantity=row_dict['Fees'].replace(',', ''),
                                                 fee_asset=row_dict['Transaction Currency'],
                                                 wallet=wallet,
                                                 note=note)
    elif row_dict['Type'] == 'Sell':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                 data_row.timestamp,
                                                 buy_quantity=abs(Decimal(row_dict['Value'].replace(',', ''))) + Decimal(row_dict['Fees'].replace(',', '')),
                                                 buy_asset=row_dict['Transaction Currency'],
                                                 sell_quantity=row_dict['Shares'].replace(',', ''),
                                                 sell_asset=get_asset(row_dict),
                                                 fee_quantity=row_dict['Fees'].replace(',', ''),
                                                 fee_asset=row_dict['Transaction Currency'],
                                                 wallet=wallet,
                                                 note=note)
    elif row_dict['Type'] == 'Delivery (Inbound)':
        shares = Decimal(row_dict['Shares'])
        note = "%s share(s) of %s" % ('{:0,.2f}'.format(shares), row_dict['Security Name'])

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_GIFT_RECEIVED,
                                                data_row.timestamp,
                                                buy_quantity=row_dict['Shares'].replace(',', ''),
                                                buy_asset=get_asset(row_dict),
                                                buy_value=DataParser.convert_currency(
                                                    row_dict['Value'].replace(',', ''),
                                                    from_currency=row_dict['Transaction Currency'],
                                                    timestamp=data_row.timestamp
                                                ),
                                                wallet=wallet,
                                                note=note)

def get_wallet(filename):
    return os.path.splitext(os.path.basename(filename))[0].replace('_', ':').removesuffix(':Cash')

def get_asset(row_dict):
    if row_dict['Ticker Symbol']:
        return "%s (%s)" % (row_dict['Security Name'], row_dict['Ticker Symbol'])

    if row_dict['ISIN']:
        return "%s (%s)" % (row_dict['Security Name'], row_dict['ISIN'])

    return row_dict['Security Name']


portfolio_performance_deposit = DataParser(DataParser.TYPE_SHARES,
                          "PortfolioPerformance Deposit",
                          ['Date','Type','Value','Transaction Currency','Taxes','Shares',
                          'ISIN','WKN','Ticker Symbol','Security Name','Note'],
                           worksheet_name="PortfolioPerformance D",
                           row_handler=parse_portfolio_performance_deposit)

portfolio_performance_security = DataParser(DataParser.TYPE_SHARES,
                          "PortfolioPerformance Security",
                          ['Date','Type','Value','Transaction Currency','Gross Amount',
                          'Currency Gross Amount','Exchange Rate','Fees','Taxes','Shares',
                          'ISIN','WKN','Ticker Symbol','Security Name','Note',],
                           worksheet_name="PortfolioPerformance S",
                           row_handler=parse_portfolio_performance_security)
