# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2019

from decimal import Decimal

from bittytax.conv.parsers.etherscan import TOKENS
from bittytax.price.datasource import Blockscout

from ...config import config
from ..out_record import TransactionOutRecord
from ..dataparser import DataParser

WALLET = 'xDAI'

TOKENS = {
    '0x4505b262dc053998c10685dc5f9098af8ae5c8ad': 'HNY-WXDAI-LP',
    '0x298c7326a6e4a6108b88520f285c7dc89403347d': 'HNY-STAKE-LP',
    '0x9e8e5e4a0900fe4634c02aaf0f130cfb93c53fbc': 'XCOMB-WXDAI-LP',
    '0x50a4867aee9cafd6ddc84de3ce59df027cb29084': 'AGVE-HNY-LP',
    '0x159eb41b54ae70d912f3e426bfdfa19888faa807': 'HNY-COLD-LP',
}

XDAI_FAUCET = '0x97AAE423C9A1Bc9cf2D81f9f1299b117A7b01136'

TOKEN_AIRDROPS = {
    '0x967ebb4343c442d19a47b9196d121bd600600911': {
        'desc': 'Honey Faucet'
    },
    '0xdd36008685108afafc11f88bbc66c39a851df843': {
        'desc': 'xCOMB Airdrop'
    },
}

blockscout = Blockscout()

def parse_blockscout(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['UnixTimestamp'])

    quantity = Decimal(row_dict['Value(xDAI)']) / 10**18

    usd_price = Decimal(row_dict['TxDateOpeningPrice'])
    unit_price = DataParser.convert_currency(usd_price, 'USD', data_row.timestamp)

    blockscout.update_prices("XDAI/USD", {
                        data_row.timestamp.strftime('%Y-%m-%d'): {
                            'price': usd_price,
                            'url': "https://blockscout.com/xdai/mainnet/tx/%s" % row_dict['TxHash'],
                        }},
                        data_row.timestamp)

    blockscout.update_prices("XDAI/%s" % config.ccy, {
                        data_row.timestamp.strftime('%Y-%m-%d'): {
                            'price': unit_price,
                            'url': "https://blockscout.com/xdai/mainnet/tx/%s" % row_dict['TxHash'],
                        }},
                        data_row.timestamp)

    if row_dict['Type'] == 'IN':
        if row_dict['FromAddress'] == XDAI_FAUCET:
            t_type = TransactionOutRecord.TYPE_AIRDROP 
            note = 'xDAI Faucet'
        else:
             t_type = TransactionOutRecord.TYPE_DEPOSIT
             note = get_note(row_dict)

        data_row.t_record = TransactionOutRecord(t_type,
                                                 data_row.timestamp,
                                                 buy_quantity=quantity,
                                                 buy_asset="xDAI",
                                                 wallet=get_wallet(row_dict['ToAddress']),
                                                 note=note)
    elif row_dict['Type'] == 'OUT':
        fee = Decimal(row_dict['Fee(xDAI)']) / 10**18

        if quantity > 0:
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                    data_row.timestamp,
                                                    sell_quantity=quantity,
                                                    sell_asset="xDAI",
                                                    fee_quantity=fee,
                                                    fee_asset="xDAI",
                                                    wallet=get_wallet(row_dict['FromAddress']),
                                                    note=get_note(row_dict))
        else:
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_SPEND,
                                                    data_row.timestamp,
                                                    sell_quantity=quantity,
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

    quantity = Decimal(row_dict['Value(xDAI)']) / 10**18

    if row_dict['ToAddress'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=quantity,
                                                 buy_asset="xDAI",
                                                 wallet=get_wallet(row_dict['ToAddress']))
    elif row_dict['FromAddress'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=quantity,
                                                 sell_asset="xDAI",
                                                 wallet=get_wallet(row_dict['FromAddress']))

def parse_blockscout_tokens(data_row, _parser, **_kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['UnixTimestamp'])

    if row_dict['TokensTransferred'] in ['', '1']:
        # ERC-1155 token (NFT or HoneyFarm Deposit)
        return

    asset = TOKENS[row_dict['TokenContractAddress']] if row_dict['TokenContractAddress'] in TOKENS else row_dict['TokenSymbol']
    quantity = Decimal(row_dict['TokensTransferred']) / 10**18

    if row_dict['Type'] == 'IN':
        if row_dict['FromAddress'] in TOKEN_AIRDROPS:
            t_type = TransactionOutRecord.TYPE_AIRDROP
            note = TOKEN_AIRDROPS[row_dict['FromAddress']]['desc']
        else:
            t_type = TransactionOutRecord.TYPE_DEPOSIT
            note = get_note(row_dict)

        data_row.t_record = TransactionOutRecord(t_type,
                                                 data_row.timestamp,
                                                 buy_quantity=quantity,
                                                 buy_asset=asset,
                                                 wallet=get_wallet(row_dict['ToAddress']),
                                                 note=note)
    elif row_dict['Type'] == 'OUT':
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=quantity,
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

blockscout_tokens = DataParser(
        DataParser.TYPE_EXPLORER,
        "Blockscout (xDAI Token Transactions)",
        ['TxHash','BlockNumber','UnixTimestamp','FromAddress','ToAddress',
        'TokenContractAddress','Type','TokenSymbol','TokensTransferred',
        'TransactionFee(xDAI)','Status','ErrCode'],
        worksheet_name="Blockscout(xDAI Token Transactions)",
        row_handler=parse_blockscout_tokens)



