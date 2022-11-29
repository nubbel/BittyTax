# -*- coding: utf-8 -*-

from decimal import Decimal

from ...config import config
from ..out_record import TransactionOutRecord
from ..dataparser import DataParser

WALLET = 'Yoroi'

def parse_yoroi(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['Date'])

    wallet = get_wallet(kwargs['filename'])
    note = row_dict['Comment (optional)']

    if row_dict['Type (Trade, IN or OUT)'] == 'Deposit':
        if 'Staking Reward' in note:
            t_type = TransactionOutRecord.TYPE_STAKING
        else:
            t_type = TransactionOutRecord.TYPE_DEPOSIT

        data_row.t_record = TransactionOutRecord(t_type,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Buy Amount'],
                                                 buy_asset=row_dict['Buy Cur.'],
                                                 wallet=wallet,
                                                 note=note)
    elif row_dict['Type (Trade, IN or OUT)'] == 'Withdrawal':
        if Decimal(row_dict['Sell Amount']) > 0:
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                    data_row.timestamp,
                                                    sell_quantity=row_dict['Sell Amount'],
                                                    sell_asset=row_dict['Sell Cur.'],
                                                    fee_quantity=row_dict['Fee Amount (optional)'],
                                                    fee_asset=row_dict['Fee Cur. (optional)'],
                                                    wallet=wallet,
                                                    note=note)
        else:
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_SPEND,
                                                    data_row.timestamp,
                                                    sell_quantity=row_dict['Sell Amount'],
                                                    sell_asset=row_dict['Sell Cur.'],
                                                    fee_quantity=row_dict['Fee Amount (optional)'],
                                                    fee_asset=row_dict['Fee Cur. (optional)'],
                                                    wallet=wallet,
                                                    note=note)

def get_wallet(filename):
    for plate in config.cardano_wallets:
        if plate in filename:
            return config.cardano_wallets[plate]

    return WALLET

DataParser(
        DataParser.TYPE_EXPLORER,
        "Yoroi",
        ["Type (Trade, IN or OUT)","Buy Amount","Buy Cur.",
         "Sell Amount","Sell Cur.","Fee Amount (optional)","Fee Cur. (optional)",
         "Exchange (optional)","Trade Group (optional)","Comment (optional)","Date","ID"],
        worksheet_name="Yoroi",
        row_handler=parse_yoroi)
