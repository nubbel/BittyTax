# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2019

import os
import atexit
import json
import platform
import urllib

from decimal import Decimal
from datetime import datetime, timedelta
from pathlib import Path

from colorama import Fore, Back
import dateutil.parser
import requests
from web3 import Web3

from ..version import __version__
from ..config import config
from .exceptions import UnexpectedDataSourceAssetIdError

CRYPTOCOMPARE_MAX_DAYS = 2000
COINPAPRIKA_MAX_DAYS = 5000

class DataSourceBase(object):
    USER_AGENT = 'BittyTax/%s Python/%s %s/%s' % (__version__,
                                                  platform.python_version(),
                                                  platform.system(), platform.release())
    TIME_OUT = 30

    def __init__(self, no_persist=False):
        self.assets = {}
        self.ids = {}
        self.prices = self.load_prices()

        for pair in sorted(self.prices):
            if config.debug:
                print("%sprice: %s (%s) data cache loaded" % (Fore.YELLOW, self.name(), pair))

        if not no_persist:
            atexit.register(self.dump_prices)

    def name(self):
        return self.__class__.__name__

    def get_json(self, url):
        if config.debug:
            print("%sprice: GET %s" % (Fore.YELLOW, url))

        response = requests.get(url, headers={'User-Agent': self.USER_AGENT}, timeout=self.TIME_OUT)

        if response.status_code in [429, 502, 503, 504]:
            response.raise_for_status()

        if response:
            return response.json()
        return {}

    def get_graphql(self, url, query):
        if config.debug:
            print("%sprice: QUERY %s `%s`" % (Fore.YELLOW, url, query))

        response = requests.post(
            url,
            headers={
                'User-Agent': self.USER_AGENT,
                "Content-Type": "application/json; charset=utf-8",
            },
            timeout=self.TIME_OUT,
            json={'query': query}
        )

        if response.status_code in [429, 502, 503, 504]:
            response.raise_for_status()

        if response:
            return response.json()['data']
        return {}

    def update_prices(self, pair, prices, timestamp):
        if pair not in self.prices:
            self.prices[pair] = {}

        # We are not interested in today's latest price, only the days closing price, also need to
        #  filter any erroneous future dates returned
        prices = {k: v
                  for k, v in prices.items()
                  if dateutil.parser.parse(k).date() < datetime.now().date()}

        # We might not receive data for the date requested, if so set to None to prevent repeat
        #  lookups, assuming date is in the past
        date = timestamp.strftime('%Y-%m-%d')
        if date not in prices and timestamp.date() < datetime.now().date():
            prices[date] = {'price': None,
                            'url': None}

        self.prices[pair].update(prices)

    def load_prices(self):
        filename = os.path.join(config.CACHE_DIR, self.name() + '.json')
        if not os.path.exists(filename):
            return {}

        try:
            with open(filename, 'r') as price_cache:
                json_prices = json.load(price_cache)
                return {pair: {date: {'price': self.str_to_decimal(price['price']),
                                      'url': price['url']}
                               for date, price in json_prices[pair].items()}
                        for pair in json_prices}
        except:
            print("%sWARNING%s Data cached for %s could not be loaded" % (
                Back.YELLOW+Fore.BLACK, Back.RESET+Fore.YELLOW, self.name()))
            return {}

    def dump_prices(self):
        with open(os.path.join(config.CACHE_DIR, self.name() + '.json'), 'w') as price_cache:
            json_prices = {pair: {date: {'price': self.decimal_to_str(price['price']),
                                         'url': price['url']}
                                  for date, price in self.prices[pair].items()}
                           for pair in self.prices}
            json.dump(json_prices, price_cache, indent=4, sort_keys=True)

    def get_config_assets(self):
        for symbol in config.data_source_select:
            for ds in config.data_source_select[symbol]:
                if ds.upper().startswith(self.name().upper() + ':'):
                    if symbol.upper() in self.assets:
                        self._update_asset(symbol, ds)
                    else:
                        self._add_asset(symbol, ds)

    def _update_asset(self, symbol, data_source):
        asset_id = data_source.split(':')[1]
        # Update an existing symbol, validate id belongs to that symbol
        if asset_id in self.ids and self.ids[asset_id]['symbol'].casefold() == symbol.casefold():
            self.assets[symbol.upper()] = {'id': asset_id, 'name': self.ids[asset_id]['name']}

            if config.debug:
                print("%sprice: %s updated as %s [ID:%s] (%s)" % (
                    Fore.YELLOW,
                    symbol,
                    self.name(),
                    asset_id,
                    self.ids[asset_id]['name']))
        else:
            raise UnexpectedDataSourceAssetIdError(data_source, symbol)

    def _add_asset(self, symbol, data_source):
        asset_id = data_source.split(':')[1]
        if asset_id in self.ids:
            self.assets[symbol.upper()] = {'id': asset_id, 'name': self.ids[asset_id]['name']}
            self.ids[asset_id] = {'symbol': symbol, 'name': self.ids[asset_id]['name']}

            if config.debug:
                print("%sprice: %s added as %s [ID:%s] (%s)" % (
                    Fore.YELLOW,
                    symbol,
                    self.name(),
                    asset_id,
                    self.ids[asset_id]['name']))
        else:
            raise UnexpectedDataSourceAssetIdError(data_source, symbol)

    def get_list(self):
        if self.ids:
            asset_list = {}
            for c in self.ids:
                symbol = self.ids[c]['symbol']
                if symbol not in asset_list:
                    asset_list[symbol] = []

                asset_list[symbol].append({'id': c, 'name': self.ids[c]['name']})

            # Include any custom symbols as well
            for symbol in asset_list:
                if self.assets[symbol] not in asset_list[symbol]:
                    asset_list[symbol].append(self.assets[symbol])

            return asset_list
        return {k: [{'id':None, 'name': v['name']}] for k, v in self.assets.items()}

    @staticmethod
    def pair(asset, quote):
        return asset.upper() + '/' + quote.upper()

    @staticmethod
    def str_to_decimal(price):
        if price:
            return Decimal(price)

        return None

    @staticmethod
    def decimal_to_str(price):
        if price:
            return '{0:f}'.format(price)

        return None

    @staticmethod
    def epoch_time(timestamp):
        epoch = (timestamp.replace(tzinfo=config.TZ_UTC) - datetime(1970, 1, 1, tzinfo=config.TZ_UTC)).total_seconds()
        return int(epoch)

class BittyTaxAPI(DataSourceBase):
    def __init__(self, no_persist=False):
        super(BittyTaxAPI, self).__init__(no_persist=no_persist)
        json_resp = self.get_json("https://api.bitty.tax/v1/symbols")
        self.assets = {k: {'name': v}
                       for k, v in json_resp['symbols'].items()}

    def get_latest(self, asset, quote, _asset_id=None):
        json_resp = self.get_json(
            "https://api.bitty.tax/v1/latest?base=%s&symbols=%s" % (asset, quote)
        )
        return Decimal(repr(json_resp['rates'][quote])) \
                if 'rates' in json_resp and quote in json_resp['rates'] else None, quote

    def get_historical(self, asset, quote, timestamp, _asset_id=None):
        url = "https://api.bitty.tax/v1/%s?base=%s&symbols=%s" % (
            timestamp.strftime('%Y-%m-%d'), asset, quote)
        json_resp = self.get_json(url)
        pair = self.pair(asset, quote)
        # Date returned in response might not be date requested due to weekends/holidays
        self.update_prices(pair,
                           {timestamp.strftime('%Y-%m-%d'): {
                               'price': Decimal(repr(json_resp['rates'][quote])) \
                                        if 'rates' in json_resp and quote \
                                        in json_resp['rates'] else None,
                               'url': url}},
                           timestamp)

        return quote

class Frankfurter(DataSourceBase):
    def __init__(self, no_persist=False):
        super(Frankfurter, self).__init__(no_persist=no_persist)
        currencies = ['EUR', 'USD', 'JPY', 'BGN', 'CYP', 'CZK', 'DKK', 'EEK', 'GBP', 'HUF',
                      'LTL', 'LVL', 'MTL', 'PLN', 'ROL', 'RON', 'SEK', 'SIT', 'SKK', 'CHF',
                      'ISK', 'NOK', 'HRK', 'RUB', 'TRL', 'TRY', 'AUD', 'BRL', 'CAD', 'CNY',
                      'HKD', 'IDR', 'ILS', 'INR', 'KRW', 'MXN', 'MYR', 'NZD', 'PHP', 'SGD',
                      'THB', 'ZAR']
        self.assets = {c: {'name': 'Fiat ' + c} for c in currencies}

    def get_latest(self, asset, quote, _asset_id=None):
        json_resp = self.get_json(
            "https://api.frankfurter.app/latest?from=%s&to=%s" % (asset, quote)
        )
        return Decimal(repr(json_resp['rates'][quote])) \
                if 'rates' in json_resp and quote in json_resp['rates'] else None, quote

    def get_historical(self, asset, quote, timestamp, _asset_id=None):
        url = "https://api.frankfurter.app/%s?from=%s&to=%s" % (
            timestamp.strftime('%Y-%m-%d'), asset, quote)
        json_resp = self.get_json(url)
        pair = self.pair(asset, quote)
        # Date returned in response might not be date requested due to weekends/holidays
        self.update_prices(pair,
                           {timestamp.strftime('%Y-%m-%d'): {
                               'price': Decimal(repr(json_resp['rates'][quote])) \
                                        if 'rates' in json_resp and quote \
                                        in json_resp['rates'] else None,
                               'url': url}},
                           timestamp)

        return quote

class CoinDesk(DataSourceBase):
    def __init__(self, no_persist=False):
        super(CoinDesk, self).__init__(no_persist=no_persist)
        self.assets = {'BTC': {'name': 'Bitcoin'}}

    def get_latest(self, _asset, quote, _asset_id=None):
        json_resp = self.get_json("https://api.coindesk.com/v1/bpi/currentprice.json")
        return Decimal(repr(json_resp['bpi'][quote]['rate_float'])) \
                if 'bpi' in json_resp and quote in json_resp['bpi'] else None, quote

    def get_historical(self, asset, quote, timestamp, _asset_id=None):
        url = "https://api.coindesk.com/v1/bpi/historical/close.json" \
              "?start=%s&end=%s&currency=%s" % (
                  timestamp.strftime('%Y-%m-%d'), datetime.now().strftime('%Y-%m-%d'), quote)
        json_resp = self.get_json(url)
        pair = self.pair(asset, quote)
        if 'bpi' in json_resp:
            self.update_prices(pair,
                               {k: {
                                   'price': Decimal(repr(v)) if v else None,
                                   'url': url} for k, v in json_resp['bpi'].items()},
                               timestamp)

        return quote

class CryptoCompare(DataSourceBase):
    def __init__(self, no_persist=False):
        super(CryptoCompare, self).__init__(no_persist=no_persist)
        json_resp = self.get_json("https://min-api.cryptocompare.com/data/all/coinlist")
        self.assets = {c[1]['Symbol'].strip().upper(): {'name': c[1]['CoinName'].strip()}
                       for c in json_resp['Data'].items()}
        # CryptoCompare symbols are unique, so no ID required

    def get_latest(self, asset, quote, _asset_id=None):
        json_resp = self.get_json("https://min-api.cryptocompare.com/data/price" \
            "?extraParams=%s&fsym=%s&tsyms=%s" % (self.USER_AGENT, asset, quote))
        return Decimal(repr(json_resp[quote])) if quote in json_resp else None, quote

    def get_historical(self, asset, quote, timestamp, _asset_id=None):
        url = "https://min-api.cryptocompare.com/data/histoday?aggregate=1&extraParams=%s" \
              "&fsym=%s&tsym=%s&limit=%s&toTs=%d" % (
                  self.USER_AGENT, asset, quote, CRYPTOCOMPARE_MAX_DAYS,
                  self.epoch_time(timestamp + timedelta(days=CRYPTOCOMPARE_MAX_DAYS)))

        json_resp = self.get_json(url)
        pair = self.pair(asset, quote)
        # Warning - CryptoCompare returns 0 as data for missing dates, convert these to None.
        if 'Data' in json_resp:
            self.update_prices(pair,
                               {datetime.utcfromtimestamp(d['time']).strftime('%Y-%m-%d'): {
                                   'price': Decimal(repr(d['close'])) if 'close' in d and \
                                           d['close'] else None,
                                   'url': url} for d in json_resp['Data']},
                               timestamp)

        return quote

class CoinGecko(DataSourceBase):
    def __init__(self, no_persist=False):
        super(CoinGecko, self).__init__(no_persist=no_persist)
        json_resp = self.get_json("https://api.coingecko.com/api/v3/coins/list")
        self.ids = {c['id']: {'symbol': c['symbol'].strip().upper(), 'name': c['name'].strip()}
                    for c in json_resp}
        self.assets = {c['symbol'].strip().upper(): {'id': c['id'], 'name': c['name'].strip()}
                       for c in json_resp}
        self.get_config_assets()

    def get_latest(self, asset, quote, asset_id=None):
        if asset_id is None:
            asset_id = self.assets[asset]['id']

        json_resp = self.get_json("https://api.coingecko.com/api/v3/coins/%s?localization=false" \
            "&community_data=false&developer_data=false" % asset_id)
        return Decimal(repr(json_resp['market_data']['current_price'][quote.lower()])) \
                if 'market_data' in json_resp and 'current_price' in json_resp['market_data'] and \
                quote.lower() in json_resp['market_data']['current_price'] else None, quote

    def get_historical(self, asset, quote, timestamp, asset_id=None):
        if asset_id is None:
            asset_id = self.assets[asset]['id']

        url = "https://api.coingecko.com/api/v3/coins/%s/market_chart?vs_currency=%s&days=max" % (
            asset_id, quote)
        json_resp = self.get_json(url)
        pair = self.pair(asset, quote)
        if 'prices' in json_resp:
            self.update_prices(pair,
                               {datetime.utcfromtimestamp(p[0]/1000).strftime('%Y-%m-%d'): {
                                   'price': Decimal(repr(p[1])) if p[1] else None,
                                   'url': url} for p in json_resp['prices']},
                               timestamp)

        return quote

class CoinPaprika(DataSourceBase):
    def __init__(self, no_persist=False):
        super(CoinPaprika, self).__init__(no_persist=no_persist)
        json_resp = self.get_json("https://api.coinpaprika.com/v1/coins")
        self.ids = {c['id']: {'symbol': c['symbol'].strip().upper(), 'name': c['name'].strip()}
                    for c in json_resp}
        self.assets = {c['symbol'].strip().upper(): {'id': c['id'], 'name': c['name'].strip()}
                       for c in json_resp}
        self.get_config_assets()

    def get_latest(self, asset, quote, asset_id=None):
        if asset_id is None:
            asset_id = self.assets[asset]['id']

        json_resp = self.get_json("https://api.coinpaprika.com/v1/tickers/%s?quotes=%s" % (
            (asset_id, quote)))
        return Decimal(repr(json_resp['quotes'][quote]['price'])) \
                if 'quotes' in json_resp and quote in json_resp['quotes'] else None, quote

    def get_historical(self, asset, quote, timestamp, asset_id=None):
        # Historic prices only available in USD or BTC
        if quote not in ('USD', 'BTC'):
            quote = 'USD'

        if asset_id is None:
            asset_id = self.assets[asset]['id']

        url = "https://api.coinpaprika.com/v1/tickers/%s/historical" \
              "?start=%s&limit=%s&quote=%s&interval=1d" % (
                  asset_id, timestamp.strftime('%Y-%m-%d'), COINPAPRIKA_MAX_DAYS, quote)

        json_resp = self.get_json(url)
        pair = self.pair(asset, quote)
        self.update_prices(pair,
                           {dateutil.parser.parse(p['timestamp']).strftime('%Y-%m-%d'): {
                               'price': Decimal(repr(p['price'])) if p['price'] else None,
                               'url': url} for p in json_resp},
                           timestamp)

        return quote

class Kraken(DataSourceBase):
    def __init__(self, no_persist=False):
        super(Kraken, self).__init__(no_persist=no_persist)

        self.ids = {
            'ETH2.SETH': {
                'symbol': 'ETH2',
                'name': 'Ethereum 2.0'
            }
        }

        self.assets = {
            self.ids[asset_id]['symbol']: {
                'id': asset_id,
                'name': self.ids[asset_id]['name']
            }
            for asset_id in self.ids
        }

        self.get_config_assets()

    def get_latest(self, asset, quote, asset_id=None):
        if asset_id is None:
            asset_id = self.assets[asset]['id']

        url = "https://api.kraken.com/0/public/Ticker?pair=ETH2.SETH"

        json_resp = self.get_json(url)

        return Decimal(json_resp['result']['ETH2.SETH']['c'][0]) if 'result' in json_resp else None, 'ETH'

    def get_historical(self, asset, quote, timestamp, asset_id=None):
        if asset_id is None:
            asset_id = self.assets[asset]['id']

        url = "https://api.kraken.com/0/public/OHLC?pair=ETH2.SETH&interval=1440&since=%d" % (
            self.epoch_time(timestamp) - 86400 # make sure requested timestamp is included
        )
        json_resp = self.get_json(url)

        if 'result' in json_resp:
            pair = self.pair(asset, 'ETH')
            self.update_prices(pair,
                               {datetime.utcfromtimestamp(ohlc[0]).strftime('%Y-%m-%d'): {
                                   'price': Decimal(ohlc[1]),
                                   'url': url,
                                } for ohlc in json_resp['result']['ETH2.SETH']},
                               timestamp)

        return 'ETH'

class Blockscout(DataSourceBase):
    def __init__(self, no_persist=False):
        super(Blockscout, self).__init__(no_persist=no_persist)

        self.ids = {
            'xdai': {
                'symbol': 'XDAI',
                'name': 'xDAI'
            },
        }
        self.assets = {
            self.ids[asset_id]['symbol']: {
                'id': asset_id,
                'name': self.ids[asset_id]['name']
            }
            for asset_id in self.ids
        }

    def get_latest(self, asset, quote, asset_id=None):
        if asset.lower() != 'xdai':
            None

        url = "https://blockscout.com/xdai/mainnet/api?module=stats&action=coinprice"

        json_resp = self.get_json(url)

        return Decimal(json_resp['result']['coin_usd']) if 'result' in json_resp else None, 'USD'

    def get_historical(self, asset, quote, timestamp, asset_id=None):
        # only serve from cache
        return 'USD'

class yVault(DataSourceBase):
    def __init__(self, no_persist=False):
        super(yVault, self).__init__(no_persist=no_persist)

        self.yvault_abi = self.get_abi('yVault')
        self.token_abi = self.get_abi('ERC20')

        self.w3 = Web3(Web3.HTTPProvider(config.web3_endpoint))
        self.block_number = self.w3.eth.get_block_number()

        self.load_vaults_info()
        self.get_config_assets()

    def get_latest(self, asset, quote, asset_id=None):
        if asset_id is None:
            asset_id = self.assets[asset]['id']

        price_per_share, token_symbol, _ = self.get_at_block(asset_id, self.block_number)        

        return price_per_share, token_symbol


    def get_historical(self, asset, quote, timestamp, asset_id=None):
        print('DEBUG[yVault.get_historical]', asset, quote, timestamp, asset_id, self.assets[asset]['id'])
        if not asset_id:
            asset_id = self.assets[asset]['id']

        block = self.find_block(timestamp)
        print('DEBUG[yVault.get_historical]   block', block.number, datetime.utcfromtimestamp(block.timestamp))

        price_per_share, token_symbol, _ = self.get_at_block(asset_id, block.number)   
        print('DEBUG[yVault.get_historical]  price', price_per_share)
        print('DEBUG[yVault.get_historical]  token_symbol', token_symbol)

        pair = self.pair(asset, token_symbol)
        data = {
            'platform': 'ethereum',
            'block': {
                'number': block.number,
                'timestamp': datetime.utcfromtimestamp(block.timestamp),
            },
            'address': asset_id,
            'calculation': "%s = %.18g * %s" % (asset, price_per_share, token_symbol)
        }

        self.update_prices(pair, {
            datetime.utcfromtimestamp(block.timestamp).strftime('%Y-%m-%d'): {
                'price': price_per_share,
                # TODO: IPFS
                'url': "data:application/json,%s" % urllib.parse.quote(json.dumps(data, indent=2, default=str)),
                'data': data,
            }
        }, timestamp)

        return token_symbol

    def get_at_block(self, address, block_number):
        yvault_contract = self.w3.eth.contract(address, abi=self.yvault_abi)

        price_per_share = yvault_contract.functions.getPricePerFullShare().call(block_identifier=block_number)

        return Web3.fromWei(price_per_share, 'ether'), self.ids[address]['token_symbol'], self.ids[address]['token_address']


    def get_abi(self, contract_name):
        path = Path(__file__).parent.joinpath('abi/%s.json' % (contract_name)).absolute()
        file = open(path)
        abi = json.load(file)
        file.close()

        return abi

    # find first block of the given date
    def find_block(self, timestamp):
        json_resp = self.get_json(
            'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp=%s&closest=after&apikey=%s' % (
                self.epoch_time(timestamp), config.etherscan_api_key
            )
        )

        if json_resp['status'] == '1':
            block = self.w3.eth.get_block(int(json_resp['result']))

            return block

    def load_vaults_info(self, load_all=False):
        # this works, but is very slow :(
        if load_all:
            # YRegistry: https://etherscan.io/address/0x3eE41C098f9666ed2eA246f4D2558010e59d63A0
            yregistry = self.w3.eth.contract('0x3eE41C098f9666ed2eA246f4D2558010e59d63A0', abi=self.get_abi('YRegistry'))

            yvaults = yregistry.functions.getVaults().call()
            for vault_address in yvaults:
                _, token_address, _, is_wrapped, is_delegated  = yregistry.functions.getVaultInfo(vault_address).call()

                if is_wrapped or is_delegated:
                    # skip these, because I don't know what wrapped/delegated means :)
                    continue

                vault_contract = self.w3.eth.contract(vault_address, abi=self.yvault_abi)
            
                symbol = vault_contract.functions.symbol().call()
                name = vault_contract.functions.name().call()

                token_contract = self.w3.eth.contract(token_address, abi=self.token_abi)
                token_symbol = token_contract.functions.symbol().call()

                self.ids[vault_address] = {
                    'symbol': symbol,
                    'name': name,
                    'token_address': token_address,
                    'token_symbol': token_symbol,
                }
        else:
            self.ids = {
                '0x2f08119C6f07c006695E079AAFc638b8789FAf18': {
                    'symbol': 'yUSDT',
                    'name': 'yearn Tether USD',
                    'token_address': '0xdAC17F958D2ee523a2206206994597C13D831ec7',
                    'token_symbol': 'USDT',
                },
                '0x597aD1e0c13Bfe8025993D9e79C69E1c0233522e': {
                    'symbol': 'yUSDC',
                    'name': 'yearn USD/C',
                    'token_address': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
                    'token_symbol': 'USDC',
                },
                '0xACd43E627e64355f1861cEC6d3a6688B31a6F952': {
                    'symbol': 'yDAI',
                    'name': 'yearn DAI',
                    'token_address': '0x6B175474E89094C44Da98b954EedeAC495271d0F',
                    'token_symbol': 'DAI',
                },
                '0x37d19d1c4E1fa9DC47bD1eA12f742a0887eDa74a': {
                    'symbol': 'yTUSD',
                    'name': 'yearn TrueUSD',
                    'token_address': '0x0000000000085d4780B73119b644AE5ecd22b376',
                    'token_symbol': 'TUSD',
                },
                '0x9cA85572E6A3EbF24dEDd195623F188735A5179f': {
                    'symbol': 'y3Crv',
                    'name': 'yearn Curve.fi DAI/USDC/USDT',
                    'token_address': '0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490',
                    'token_symbol': '3Crv',
                },
                '0x5dbcF33D8c2E976c6b560249878e6F1491Bca25c': {
                    'symbol': 'yUSD',
                    'name': 'yearn Curve.fi yDAI/yUSDC/yUSDT/yTUSD',
                    'token_address': '0xdF5e0e81Dff6FAF3A7e52BA697820c5e32D806A8',
                    'token_symbol': 'yCurve',
                },
            }

        self.assets = {
            self.ids[asset_id]['symbol'].upper(): {
                'id': asset_id,
                'name': self.ids[asset_id]['name']
            }
            for asset_id in self.ids
        }

class yVaultV2(DataSourceBase):
    def __init__(self, no_persist=False):
        super(yVaultV2, self).__init__(no_persist=no_persist)

        self.yvault_abi = self.get_abi('yVaultV2')
        self.token_abi = self.get_abi('ERC20')

        self.w3 = Web3(Web3.HTTPProvider(config.web3_endpoint))
        self.block_number = self.w3.eth.get_block_number()

        self.load_vaults_info()
        self.get_config_assets()

    def get_latest(self, asset, quote, asset_id=None):
        if asset_id is None:
            asset_id = self.assets[asset]['id']

        price_per_share, token_symbol, _ = self.get_at_block(asset_id, self.block_number)        

        return price_per_share, token_symbol


    def get_historical(self, asset, quote, timestamp, asset_id=None):
        print('DEBUG[yVault.get_historical]', asset, quote, timestamp, asset_id, self.assets[asset]['id'])
        if not asset_id:
            asset_id = self.assets[asset]['id']

        block = self.find_block(timestamp)
        print('DEBUG[yVault.get_historical]   block', block.number, datetime.utcfromtimestamp(block.timestamp))

        price_per_share, token_symbol, _ = self.get_at_block(asset_id, block.number)   
        print('DEBUG[yVault.get_historical]  price', price_per_share)
        print('DEBUG[yVault.get_historical]  token_symbol', token_symbol)

        pair = self.pair(asset, token_symbol)
        data = {
            'platform': 'ethereum',
            'block': {
                'number': block.number,
                'timestamp': datetime.utcfromtimestamp(block.timestamp),
            },
            'address': asset_id,
            'calculation': "%s = %.18g * %s" % (asset, price_per_share, token_symbol)
        }

        self.update_prices(pair, {
            datetime.utcfromtimestamp(block.timestamp).strftime('%Y-%m-%d'): {
                'price': price_per_share,
                # TODO: IPFS
                'url': "data:application/json,%s" % urllib.parse.quote(json.dumps(data, indent=2, default=str)),
                'data': data,
            }
        }, timestamp)

        return token_symbol

    def get_at_block(self, address, block_number):
        yvault_contract = self.w3.eth.contract(address, abi=self.yvault_abi)

        price_per_share = yvault_contract.functions.pricePerShare().call(block_identifier=block_number)

        return Web3.fromWei(price_per_share, 'ether'), self.ids[address]['token_symbol'], self.ids[address]['token_address']


    def get_abi(self, contract_name):
        path = Path(__file__).parent.joinpath('abi/%s.json' % (contract_name)).absolute()
        file = open(path)
        abi = json.load(file)
        file.close()

        return abi

    # find first block of the given date
    def find_block(self, timestamp):
        json_resp = self.get_json(
            'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp=%s&closest=after&apikey=%s' % (
                self.epoch_time(timestamp), config.etherscan_api_key
            )
        )

        if json_resp['status'] == '1':
            block = self.w3.eth.get_block(int(json_resp['result']))

            return block

    def load_vaults_info(self, load_all=False):
        self.ids = {
            '0x84E13785B5a27879921D6F685f041421C7F482dA': {
                'symbol': 'yvCurve-3pool',
                'name': 'Curve 3pool yVault',
                'token_address': '0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490',
                'token_symbol': '3Crv',
            },
            '0xE14d13d8B3b85aF791b2AADD661cDBd5E6097Db1': {
                'symbol': 'yvYFI',
                'name': 'YFI yVault',
                'token_address': '0x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e',
                'token_symbol': 'YFI',
            },
            '0xB8C3B7A2A618C552C23B1E4701109a9E756Bab67': {
                'symbol': 'yv1INCH',
                'name': '1INCH yVault',
                'token_address': '0x111111111117dc0aa78b770fa6a738034120c302',
                'token_symbol': '1INCH',
            },
        }

        self.assets = {
            self.ids[asset_id]['symbol'].upper(): {
                'id': asset_id,
                'name': self.ids[asset_id]['name']
            }
            for asset_id in self.ids
        }

class HoneyswapSubgraph(DataSourceBase):
    def __init__(self, no_persist=False):
        super(HoneyswapSubgraph, self).__init__(no_persist=no_persist)

        json_resp = self.get_graphql(
            "https://api.thegraph.com/subgraphs/name/1hive/honeyswap-xdai",
            "{ tokens(where: { symbol_in: %s }) { id symbol name } }" % (
                json.dumps([symbol for symbol in config.data_source_select
                                   for ds in config.data_source_select[symbol]
                                   if ds.upper().startswith(self.name().upper())])
            ),
        )

        self.ids = {
            asset['id']: {
                'symbol': asset['symbol'],
                'name': asset['name'],
            }
            for asset in json_resp['tokens']
        }

        self.assets = {
            self.ids[asset_id]['symbol'].upper(): {
                'id': asset_id,
                'name': self.ids[asset_id]['name']
            }
            for asset_id in self.ids
        }

        self.get_config_assets()

    def get_latest(self, asset, quote, asset_id=None):
        if asset_id is None:
            asset_id = self.assets[asset]['id']

        json_resp = self.get_graphql(
            "https://api.thegraph.com/subgraphs/name/1hive/honeyswap-xdai",
            "{ token(id: \"%s\") { priceUSD: derivedNativeCurrency } }" % asset_id,
        )

        return Decimal(json_resp['token']['priceUSD']) if 'token' in json_resp else None, 'USD'

    def get_historical(self, asset, quote, timestamp, asset_id=None):
        if not asset_id:
            asset_id = self.assets[asset]['id']

        url = "https://api.thegraph.com/subgraphs/name/1hive/honeyswap-xdai"
        query = "{ token(id: \"%s\") { tokenDayData(where:{date_gte:%d}) { date priceUSD } } }" % (
            asset_id, 
            self.epoch_time(timestamp) - 86400 # make sure requested timestamp is included
        )

        json_resp = self.get_graphql(url, query)

        pair = self.pair(asset, 'USD')
        self.update_prices(pair, {
            datetime.utcfromtimestamp(data['date']).strftime('%Y-%m-%d'): {
                'price': Decimal(data['priceUSD']),
                'url': "%s?query=%s" % (url, query),
            }
            for data in json_resp['token']['tokenDayData']

        }, timestamp)

        return 'USD'
