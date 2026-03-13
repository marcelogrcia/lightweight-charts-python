import unittest
import pandas as pd
from util import BARS, Tester
from lightweight_charts import Chart


class TestChart(Tester):
    def test_data_is_renamed(self):
        uppercase_df = pd.DataFrame(BARS.copy()).rename({'date': 'Date', 'open': 'OPEN', 'high': 'HIgh', 'low': 'Low', 'close': 'close', 'volUME': 'volume'})
        result = self.chart._df_datetime_format(uppercase_df)
        self.assertEqual(list(result.columns), list(BARS.rename(columns={'date': 'time'}).columns))

    def test_line_in_list(self):
        result0 = self.chart.create_line()
        result1 = self.chart.create_line()
        self.assertEqual(result0, self.chart.lines()[0])
        self.assertEqual(result1, self.chart.lines()[1])

    def test_indicator_spec_normalization(self):
        normalized = self.chart._normalize_indicators_spec({
            'rsi': ['subplot', 'histogram'],
            'atr': {'pane': 'osc', 'type': 'line'},
            'sma': ['main', 'line'],
        })
        self.assertEqual(normalized['rsi']['pane_key'], 'subplot:rsi')
        self.assertEqual(normalized['rsi']['type'], 'histogram')
        self.assertEqual(normalized['atr']['pane_key'], 'pane:osc')
        self.assertEqual(normalized['atr']['type'], 'line')
        self.assertEqual(normalized['sma']['pane_key'], 'main')

    def test_indicator_spec_invalid_type(self):
        with self.assertRaises(ValueError):
            self.chart._normalize_indicators_spec({'rsi': ['main', 'area']})

    def test_set_invalid_engine_raises(self):
        with self.assertRaises(ValueError):
            self.chart.set(BARS.copy(), engine='sqlite')

    def test_set_unknown_engine_option_raises(self):
        with self.assertRaises(ValueError):
            self.chart.set(BARS.copy(), engine='duckdb', engine_options={'window': 1000})

    def test_df_datetime_format_infers_millisecond_epoch(self):
        df = BARS.rename(columns={'date': 'time'}).copy()
        df['time'] = pd.to_datetime(df['time']).astype('int64') // 10 ** 6
        result = self.chart._df_datetime_format(df)
        self.assertGreater(int(result['time'].iloc[0]), 1_000_000_000)

    def test_df_datetime_format_infers_millisecond_epoch_from_string(self):
        df = BARS.rename(columns={'date': 'time'}).copy()
        df['time'] = (pd.to_datetime(df['time']).astype('int64') // 10 ** 6).astype(str)
        result = self.chart._df_datetime_format(df)
        self.assertGreater(int(result['time'].iloc[0]), 1_000_000_000)

    def test_df_datetime_format_sorts_descending_input(self):
        df = BARS.rename(columns={'date': 'time'}).copy().iloc[::-1].reset_index(drop=True)
        result = self.chart._df_datetime_format(df)
        self.assertTrue(result['time'].is_monotonic_increasing)


if __name__ == '__main__':
    unittest.main()
