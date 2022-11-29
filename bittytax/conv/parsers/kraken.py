# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2020

from decimal import Decimal

from ..out_record import TransactionOutRecord
from ..dataparser import DataParser
from ..exceptions import UnexpectedTypeError, UnexpectedTradingPairError

SPOT_WALLET = "Kraken:Spot"
STAKING_WALLET = "Kraken:Staking"

QUOTE_ASSETS = ['AUD', 'CAD', 'CHF', 'DAI', 'DOT', 'ETH', 'EUR', 'GBP', 'JPY', 'USD',
                'USDC', 'USDT', 'XBT', 'XETH', 'XXBT', 'ZAUD', 'ZCAD', 'ZEUR', 'ZGBP', 'ZJPY',
                'ZUSD']

ALT_ASSETS = {'KFEE': 'FEE', 'XETC': 'ETC', 'XETH': 'ETH', 'XLTC': 'LTC', 'XMLN': 'MLN',
              'XREP': 'REP', 'XXBT': 'XBT', 'XXDG': 'XDG', 'XXLM': 'XLM', 'XXMR': 'XMR',
              'XXRP': 'XRP', 'XZEC': 'ZEC', 'ZAUD': 'AUD', 'ZCAD': 'CAD', 'ZEUR': 'EUR',
              'ZGBP': 'GBP', 'ZJPY': 'JPY', 'ZUSD': 'USD'}

STAKING_ASSETS = {'XTZ.S': 'XTZ', 'DOT.S': 'DOT', 'ATOM.S': 'ATOM', 'ETH2.S': 'ETH2', 'SOL.S': 'SOL'}

ASSETS_2CHARS = ['SC']

def parse_kraken_ledgers(data_row, parser, **_kwargs):
    # https://support.kraken.com/hc/en-us/articles/360001169383-How-to-interpret-Ledger-history-fields

    row_dict = data_row.row_dict
    if row_dict['txid'] == "":
        # Check for txid to filter failed transactions
        return

    data_row.timestamp = DataParser.parse_timestamp(row_dict['time'])

    if row_dict['type'] == "deposit":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['amount'],
                                                 buy_asset=normalise_asset(row_dict['asset']),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(row_dict['asset']),
                                                 wallet=SPOT_WALLET)
    elif row_dict['type'] == "withdrawal":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=abs(Decimal(row_dict['amount'])),
                                                 sell_asset=normalise_asset(row_dict['asset']),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(row_dict['asset']),
                                                 wallet=SPOT_WALLET)

    elif row_dict['type'] == "staking":
        wallet = STAKING_WALLET if row_dict['asset'] in STAKING_ASSETS else SPOT_WALLET

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_STAKING,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['amount'],
                                                 buy_asset=normalise_asset(row_dict['asset']),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(row_dict['asset']),
                                                 wallet=wallet)

    elif row_dict['type'] == "transfer":
        if row_dict['subtype'] == "spottostaking":
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=abs(Decimal(row_dict['amount'])),
                                                 sell_asset=normalise_asset(row_dict['asset']),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(row_dict['asset']),
                                                 wallet=SPOT_WALLET,
                                                 note="stake")
        elif row_dict['subtype'] == "stakingtospot":
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=abs(Decimal(row_dict['amount'])),
                                                 sell_asset=normalise_asset(row_dict['asset']),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(row_dict['asset']),
                                                 wallet=STAKING_WALLET,
                                                 note="unstake")
        elif row_dict['subtype'] == "spotfromstaking":
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['amount'],
                                                 buy_asset=normalise_asset(row_dict['asset']),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(row_dict['asset']),
                                                 wallet=SPOT_WALLET,
                                                 note="unstake")
        elif row_dict['subtype'] == "stakingfromspot":
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['amount'],
                                                 buy_asset=normalise_asset(row_dict['asset']),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(row_dict['asset']),
                                                 wallet=STAKING_WALLET,
                                                 note="stake")
        else:
            raise UnexpectedTypeError(parser.in_header.index('subtype'), 'subtype', row_dict['subtype'])
        
    elif row_dict['type'] in ["trade", "spend", "receive"]:
        asset = normalise_asset(row_dict['asset'])
        quantity = Decimal(row_dict['amount'])

        buy_asset = buy_quantity = None
        sell_asset = sell_quantity = None

        if quantity > 0:
            buy_asset = asset
            buy_quantity = quantity
        else:
            sell_asset = asset
            sell_quantity = abs(quantity)

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                 data_row.timestamp,
                                                 buy_quantity=buy_quantity,
                                                 buy_asset=buy_asset,
                                                 sell_quantity=sell_quantity,
                                                 sell_asset=sell_asset,
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(row_dict['asset']),
                                                 wallet=SPOT_WALLET)

    else:
        raise UnexpectedTypeError(parser.in_header.index('type'), 'type', row_dict['type'])

def parse_kraken_trades(data_row, parser, **_kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['time'])

    base_asset, quote_asset = split_trading_pair(row_dict['pair'])
    if base_asset is None or quote_asset is None:
        raise UnexpectedTradingPairError(parser.in_header.index('pair'), 'pair', row_dict['pair'])

    if row_dict['type'] == "buy":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['vol'],
                                                 buy_asset=normalise_asset(base_asset),
                                                 sell_quantity=row_dict['cost'],
                                                 sell_asset=normalise_asset(quote_asset),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(quote_asset),
                                                 wallet=SPOT_WALLET)
    elif row_dict['type'] == "sell":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['cost'],
                                                 buy_asset=normalise_asset(quote_asset),
                                                 sell_quantity=row_dict['vol'],
                                                 sell_asset=normalise_asset(base_asset),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(quote_asset),
                                                 wallet=SPOT_WALLET)
    else:
        raise UnexpectedTypeError(parser.in_header.index('type'), 'type', row_dict['type'])

def split_trading_pair(trading_pair):
    for quote_asset in sorted(QUOTE_ASSETS, reverse=True):
        if trading_pair.endswith(quote_asset) and (len(trading_pair)-len(quote_asset) >= 3 \
                or trading_pair[:2] in ASSETS_2CHARS):
            return trading_pair[:-len(quote_asset)], quote_asset

    return None, None

def normalise_asset(asset):
    if asset in ALT_ASSETS:
        asset = ALT_ASSETS.get(asset)

    if asset in STAKING_ASSETS:
        asset = STAKING_ASSETS.get(asset)

    if asset == "XBT":
        return "BTC"
    return asset

kraken_ledgers = DataParser(DataParser.TYPE_EXCHANGE,
           "Kraken Ledgers",
           ['txid', 'refid', 'time', 'type', 'subtype', 'aclass', 'asset', 'amount', 'fee',
            'balance'],
           worksheet_name="Kraken L",
           row_handler=parse_kraken_ledgers)

kraken_trades = DataParser(DataParser.TYPE_EXCHANGE,
           "Kraken Trades",
           ['txid', 'ordertxid', 'pair', 'time', 'type', 'ordertype', 'price', 'cost', 'fee', 'vol',
            'margin', 'misc', 'ledgers'],
           worksheet_name="Kraken T",
           row_handler=parse_kraken_trades)
