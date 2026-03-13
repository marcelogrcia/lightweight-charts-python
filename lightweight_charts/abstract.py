import asyncio
import json
import os
import shutil
import tempfile
import warnings
from base64 import b64decode
from datetime import datetime
from pathlib import Path
from typing import Callable, Union, Literal, List, Optional, Dict, Any
import pandas as pd

from .table import Table
from .toolbox import ToolBox
from .drawings import Box, HorizontalLine, RayLine, TrendLine, TwoPointDrawing, VerticalLine, VerticalSpan
from .topbar import TopBar
from .util import (
    BulkRunScript, Pane, Events, IDGen, as_enum, jbool, js_json, TIME, NUM, FLOAT,
    LINE_STYLE, MARKER_POSITION, MARKER_SHAPE, CROSSHAIR_MODE,
    PRICE_SCALE_MODE, marker_position, marker_shape, js_data,
)
from .streaming import StreamingSource, PandasSource, DuckDBSource, StreamConfig

current_dir = os.path.dirname(os.path.abspath(__file__))
INDEX = os.path.join(current_dir, 'js', 'index.html')


class Window:
    _id_gen = IDGen()
    handlers = {}

    def __init__(
        self,
        script_func: Optional[Callable] = None,
        js_api_code: Optional[str] = None,
        run_script: Optional[Callable] = None
    ):
        self.loaded = False
        self.script_func = script_func
        self.scripts = []
        self.final_scripts = []
        self.bulk_run = BulkRunScript(script_func)

        if run_script:
            self.run_script = run_script

        if js_api_code:
            self.run_script(f'window.callbackFunction = {js_api_code}')

    def on_js_load(self):
        if self.loaded:
            return
        self.loaded = True

        if hasattr(self, '_return_q'):
            while not self.run_script_and_get('document.readyState == "complete"'):
                continue    # scary, but works

        initial_script = ''
        self.scripts.extend(self.final_scripts)
        for script in self.scripts:
            initial_script += f'\n{script}'
        self.script_func(initial_script)

    def run_script(self, script: str, run_last: bool = False):
        """
        For advanced users; evaluates JavaScript within the Webview.
        """
        if self.script_func is None:
            raise AttributeError("script_func has not been set")
        if self.loaded:
            if self.bulk_run.enabled:
                self.bulk_run.add_script(script)
            else:
                self.script_func(script)
        elif run_last:
            self.final_scripts.append(script)
        else:
            self.scripts.append(script)

    def run_script_and_get(self, script: str):
        self.run_script(f'_~_~RETURN~_~_{script}')
        return self._return_q.get()

    def create_table(
        self,
        width: NUM,
        height: NUM,
        headings: tuple,
        widths: Optional[tuple] = None,
        alignments: Optional[tuple] = None,
        position: FLOAT = 'left',
        draggable: bool = False,
        background_color: str = '#121417',
        border_color: str = 'rgb(70, 70, 70)',
        border_width: int = 1,
        heading_text_colors: Optional[tuple] = None,
        heading_background_colors: Optional[tuple] = None,
        return_clicked_cells: bool = False,
        func: Optional[Callable] = None
    ) -> 'Table':
        return Table(*locals().values())

    def create_subchart(
        self,
        position: FLOAT = 'left',
        width: float = 0.5,
        height: float = 0.5,
        sync_id: Optional[str] = None,
        scale_candles_only: bool = False,
        sync_crosshairs_only: bool = False,
        sync_mode: Literal['main', 'active'] = 'main',
        toolbox: bool = False
    ) -> 'AbstractChart':
        subchart = AbstractChart(
            self,
            width,
            height,
            scale_candles_only,
            toolbox,
            position=position
        )
        if not sync_id:
            return subchart
        self.run_script(f'''
            Lib.Handler.syncCharts(
                {subchart.id},
                {sync_id},
                {jbool(sync_crosshairs_only)},
                "{sync_mode}"
            )
        ''', run_last=True)
        return subchart

    def style(
        self,
        background_color: str = '#0c0d0f',
        hover_background_color: str = '#3c434c',
        click_background_color: str = '#50565E',
        active_background_color: str = 'rgba(0, 122, 255, 0.7)',
        muted_background_color: str = 'rgba(0, 122, 255, 0.3)',
        border_color: str = '#3C434C',
        color: str = '#d8d9db',
        active_color: str = '#ececed'
    ):
        self.run_script(f'Lib.Handler.setRootStyles({js_json(locals())});')


class SeriesCommon(Pane):
    def __init__(self, chart: 'AbstractChart', name: str = ''):
        super().__init__(chart.win)
        self._chart = chart
        if hasattr(chart, '_interval'):
            self._interval = chart._interval
        else:
            self._interval = 1
        self._last_bar = None
        self.name = name
        self.num_decimals = 2
        self.offset = 0
        self.data = pd.DataFrame()
        self.markers = {}

    @staticmethod
    def _infer_epoch_unit(max_abs_value: float) -> str:
        if max_abs_value >= 1e17:
            return 'ns'
        if max_abs_value >= 1e14:
            return 'us'
        if max_abs_value >= 1e11:
            return 'ms'
        return 's'

    @classmethod
    def _numeric_to_datetime(cls, values: pd.Series) -> pd.Series:
        values = pd.to_numeric(values, errors='coerce')
        non_null = values.dropna()
        if non_null.empty:
            return pd.to_datetime(values, errors='coerce')
        unit = cls._infer_epoch_unit(float(non_null.abs().max()))
        return pd.to_datetime(values, unit=unit, errors='coerce')

    @classmethod
    def _to_datetime_series(cls, values: pd.Series) -> pd.Series:
        if pd.api.types.is_datetime64_any_dtype(values):
            return pd.to_datetime(values, errors='coerce')
        if pd.api.types.is_numeric_dtype(values):
            return cls._numeric_to_datetime(values)

        numeric_values = pd.to_numeric(values, errors='coerce')
        non_null_count = int(values.notna().sum())
        numeric_ratio = (float(numeric_values.notna().sum()) / non_null_count) if non_null_count else 0.0
        if numeric_ratio >= 0.9 and numeric_values.notna().any():
            return cls._numeric_to_datetime(numeric_values)
        return pd.to_datetime(values, errors='coerce')

    @classmethod
    def _to_datetime_scalar(cls, value):
        if isinstance(value, pd.Timestamp):
            return value
        if isinstance(value, (int, float)) and not pd.isna(value):
            unit = cls._infer_epoch_unit(abs(float(value)))
            return pd.to_datetime(value, unit=unit, errors='coerce')
        if isinstance(value, str):
            numeric_value = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
            if not pd.isna(numeric_value):
                unit = cls._infer_epoch_unit(abs(float(numeric_value)))
                return pd.to_datetime(float(numeric_value), unit=unit, errors='coerce')
        return pd.to_datetime(value, errors='coerce')

    def _set_interval(self, df: pd.DataFrame):
        if not pd.api.types.is_datetime64_any_dtype(df['time']):
            df['time'] = self._to_datetime_series(df['time'])
        common_interval = df['time'].diff().value_counts()
        if common_interval.empty:
            return
        self._interval = common_interval.index[0].total_seconds()

        units = [
            pd.Timedelta(microseconds=df['time'].dt.microsecond.value_counts().index[0]),
            pd.Timedelta(seconds=df['time'].dt.second.value_counts().index[0]),
            pd.Timedelta(minutes=df['time'].dt.minute.value_counts().index[0]),
            pd.Timedelta(hours=df['time'].dt.hour.value_counts().index[0]),
            pd.Timedelta(days=df['time'].dt.day.value_counts().index[0]),
        ]
        self.offset = 0
        for value in units:
            value = value.total_seconds()
            if value == 0:
                continue
            elif value >= self._interval:
                break
            self.offset = value
            break

    @staticmethod
    def _format_labels(data, labels, index, exclude_lowercase):
        def rename(la, mapper):
            return [mapper[key] if key in mapper else key for key in la]
        if 'date' not in labels and 'time' not in labels:
            labels = labels.str.lower()
            if exclude_lowercase:
                labels = rename(labels, {exclude_lowercase.lower(): exclude_lowercase})
        if 'date' in labels:
            labels = rename(labels, {'date': 'time'})
        elif 'time' not in labels:
            data['time'] = index
            labels = [*labels, 'time']
        return labels

    def _df_datetime_format(self, df: pd.DataFrame, exclude_lowercase=None):
        df = df.copy()
        df.columns = self._format_labels(df, df.columns, df.index, exclude_lowercase)
        if not pd.api.types.is_datetime64_any_dtype(df['time']):
            df['time'] = self._to_datetime_series(df['time'])
        df = df.dropna(subset=['time'])
        if df.empty:
            return df
        df = (
            df
            .sort_values('time')
            .drop_duplicates(subset=['time'], keep='last')
            .reset_index(drop=True)
        )
        self._set_interval(df)
        df['time'] = df['time'].astype('int64') // 10 ** 9
        return df

    def _series_datetime_format(self, series: pd.Series, exclude_lowercase=None):
        series = series.copy()
        series.index = self._format_labels(series, series.index, series.name, exclude_lowercase)
        series['time'] = self._single_datetime_format(series['time'])
        return series

    def _single_datetime_format(self, arg) -> float:
        if isinstance(arg, (str, int, float)) or not pd.api.types.is_datetime64_any_dtype(arg):
            arg = self._to_datetime_scalar(arg)
        if pd.isna(arg):
            raise ValueError('Could not parse time value.')
        arg = self._interval * (arg.timestamp() // self._interval)+self.offset
        return arg

    def set(self, df: Optional[pd.DataFrame] = None, format_cols: bool = True):
        if df is None or df.empty:
            self.run_script(f'{self.id}.series.setData([])')
            self.data = pd.DataFrame()
            return
        if format_cols:
            df = self._df_datetime_format(df, exclude_lowercase=self.name)
            if df.empty:
                self.run_script(f'{self.id}.series.setData([])')
                self.data = pd.DataFrame()
                return
        if self.name:
            if self.name not in df:
                raise NameError(f'No column named "{self.name}".')
            df = df.rename(columns={self.name: 'value'})
        self.data = df.copy()
        self._last_bar = df.iloc[-1]
        self.run_script(f'{self.id}.series.setData({js_data(df)}); ')

    def update(self, series: pd.Series):
        series = self._series_datetime_format(series, exclude_lowercase=self.name)
        if self.name in series.index:
            series.rename({self.name: 'value'}, inplace=True)
        if self._last_bar is not None and series['time'] != self._last_bar['time']:
            self.data.loc[self.data.index[-1]] = self._last_bar
            self.data = pd.concat([self.data, series.to_frame().T], ignore_index=True)
        self._last_bar = series
        self.run_script(f'{self.id}.series.update({js_data(series)})')

    def _update_markers(self):
        self.run_script(f'{self.id}.series.setMarkers({json.dumps(list(self.markers.values()))})')

    def marker_list(self, markers: list):
        """
        Creates multiple markers.\n
        :param markers: The list of markers to set. These should be in the format:\n
        [
            {"time": "2021-01-21", "position": "below", "shape": "circle", "color": "#2196F3", "text": ""},
            {"time": "2021-01-22", "position": "below", "shape": "circle", "color": "#2196F3", "text": ""},
            ...
        ]
        :return: a list of marker ids.
        """
        markers = markers.copy()
        marker_ids = []
        for marker in markers:
            marker_id = self.win._id_gen.generate()
            self.markers[marker_id] = {
                "time": self._single_datetime_format(marker['time']),
                "position": marker_position(marker['position']),
                "color": marker['color'],
                "shape": marker_shape(marker['shape']),
                "text": marker['text'],
            }
            marker_ids.append(marker_id)
        self._update_markers()
        return marker_ids

    def marker(self, time: Optional[datetime] = None, position: MARKER_POSITION = 'below',
               shape: MARKER_SHAPE = 'arrow_up', color: str = '#2196F3', text: str = ''
               ) -> str:
        """
        Creates a new marker.\n
        :param time: Time location of the marker. If no time is given, it will be placed at the last bar.
        :param position: The position of the marker.
        :param color: The color of the marker (rgb, rgba or hex).
        :param shape: The shape of the marker.
        :param text: The text to be placed with the marker.
        :return: The id of the marker placed.
        """
        try:
            formatted_time = self._last_bar['time'] if not time else self._single_datetime_format(time)
        except TypeError:
            raise TypeError('Chart marker created before data was set.')
        marker_id = self.win._id_gen.generate()

        self.markers[marker_id] = {
            "time": formatted_time,
            "position": marker_position(position),
            "color": color,
            "shape": marker_shape(shape),
            "text": text,
        }
        self._update_markers()
        return marker_id

    def remove_marker(self, marker_id: str):
        """
        Removes the marker with the given id.\n
        """
        self.markers.pop(marker_id)
        self._update_markers()

    def horizontal_line(self, price: NUM, color: str = 'rgb(122, 146, 202)', width: int = 2,
                        style: LINE_STYLE = 'solid', text: str = '', axis_label_visible: bool = True,
                        func: Optional[Callable] = None
                        ) -> 'HorizontalLine':
        """
        Creates a horizontal line at the given price.
        """
        return HorizontalLine(self, price, color, width, style, text, axis_label_visible, func)

    def trend_line(
        self,
        start_time: TIME,
        start_value: NUM,
        end_time: TIME,
        end_value: NUM,
        round: bool = False,
        line_color: str = '#1E80F0',
        width: int = 2,
        style: LINE_STYLE = 'solid',
    ) -> TwoPointDrawing:
        return TrendLine(*locals().values())

    def box(
        self,
        start_time: TIME,
        start_value: NUM,
        end_time: TIME,
        end_value: NUM,
        round: bool = False,
        color: str = '#1E80F0',
        fill_color: str = 'rgba(255, 255, 255, 0.2)',
        width: int = 2,
        style: LINE_STYLE = 'solid',
    ) -> TwoPointDrawing:
        return Box(*locals().values())

    def ray_line(
        self,
        start_time: TIME,
        value: NUM,
        round: bool = False,
        color: str = '#1E80F0',
        width: int = 2,
        style: LINE_STYLE = 'solid',
        text: str = ''
    ) -> RayLine:
    # TODO
        return RayLine(*locals().values())

    def vertical_line(
        self,
        time: TIME,
        color: str = '#1E80F0',
        width: int = 2,
        style: LINE_STYLE ='solid',
        text: str = ''
    ) -> VerticalLine:
        return VerticalLine(*locals().values())

    def clear_markers(self):
        """
        Clears the markers displayed on the data.\n
        """
        self.markers.clear()
        self._update_markers()

    def price_line(self, label_visible: bool = True, line_visible: bool = True, title: str = ''):
        self.run_script(f'''
        {self.id}.series.applyOptions({{
            lastValueVisible: {jbool(label_visible)},
            priceLineVisible: {jbool(line_visible)},
            title: '{title}',
        }})''')

    def precision(self, precision: int):
        """
        Sets the precision and minMove.\n
        :param precision: The number of decimal places.
        """
        min_move = 1 / (10**precision)
        self.run_script(f'''
        {self.id}.series.applyOptions({{
            priceFormat: {{precision: {precision}, minMove: {min_move}}}
        }})''')
        self.num_decimals = precision

    def hide_data(self):
        self._toggle_data(False)

    def show_data(self):
        self._toggle_data(True)

    def _toggle_data(self, arg):
        self.run_script(f'''
        {self.id}.series.applyOptions({{visible: {jbool(arg)}}})
        if ('volumeSeries' in {self.id}) {self.id}.volumeSeries.applyOptions({{visible: {jbool(arg)}}})
        ''')

    def vertical_span(
        self,
        start_time: Union[TIME, tuple, list],
        end_time: Optional[TIME] = None,
        color: str = 'rgba(252, 219, 3, 0.2)',
        round: bool = False
    ):
        """
        Creates a vertical line or span across the chart.\n
        Start time and end time can be used together, or end_time can be
        omitted and a single time or a list of times can be passed to start_time.
        """
        if round:
            start_time = self._single_datetime_format(start_time)
            end_time = self._single_datetime_format(end_time) if end_time else None
        return VerticalSpan(self, start_time, end_time, color)


class Line(SeriesCommon):
    def __init__(self, chart, name, color, style, width, price_line, price_label, price_scale_id=None, crosshair_marker=True):

        super().__init__(chart, name)
        self.color = color

        self.run_script(f'''
            {self.id} = {self._chart.id}.createLineSeries(
                "{name}",
                {{
                    color: '{color}',
                    lineStyle: {as_enum(style, LINE_STYLE)},
                    lineWidth: {width},
                    lastValueVisible: {jbool(price_label)},
                    priceLineVisible: {jbool(price_line)},
                    crosshairMarkerVisible: {jbool(crosshair_marker)},
                    priceScaleId: {f'"{price_scale_id}"' if price_scale_id else 'undefined'}
                    {"""autoscaleInfoProvider: () => ({
                            priceRange: {
                                minValue: 1_000_000_000,
                                maxValue: 0,
                            },
                        }),
                    """ if chart._scale_candles_only else ''}
                }}
            )
        null''')

    # def _set_trend(self, start_time, start_value, end_time, end_value, ray=False, round=False):
    #     if round:
    #         start_time = self._single_datetime_format(start_time)
    #         end_time = self._single_datetime_format(end_time)
    #     else:
    #         start_time, end_time = pd.to_datetime((start_time, end_time)).astype('int64') // 10 ** 9

    #     self.run_script(f'''
    #     {self._chart.id}.chart.timeScale().applyOptions({{shiftVisibleRangeOnNewBar: false}})
    #     {self.id}.series.setData(
    #         calculateTrendLine({start_time}, {start_value}, {end_time}, {end_value},
    #                             {self._chart.id}, {jbool(ray)}))
    #     {self._chart.id}.chart.timeScale().applyOptions({{shiftVisibleRangeOnNewBar: true}})
    #     ''')

    def delete(self):
        """
        Irreversibly deletes the line, as well as the object that contains the line.
        """
        self._chart._lines.remove(self) if self in self._chart._lines else None
        self.run_script(f'''
            {self.id}legendItem = {self._chart.id}.legend._lines.find((line) => line.series == {self.id}.series)
            {self._chart.id}.legend._lines = {self._chart.id}.legend._lines.filter((item) => item != {self.id}legendItem)

            if ({self.id}legendItem) {{
                {self._chart.id}.legend.div.removeChild({self.id}legendItem.row)
            }}

            {self._chart.id}.chart.removeSeries({self.id}.series)
            delete {self.id}legendItem
            delete {self.id}
        ''')


class Histogram(SeriesCommon):
    def __init__(self, chart, name, color, price_line, price_label, scale_margin_top, scale_margin_bottom):
        super().__init__(chart, name)
        self.color = color
        self.run_script(f'''
        {self.id} = {chart.id}.createHistogramSeries(
            "{name}",
            {{
                color: '{color}',
                lastValueVisible: {jbool(price_label)},
                priceLineVisible: {jbool(price_line)},
                priceScaleId: '{self.id}',
                priceFormat: {{type: "volume"}},
            }},
            // precision: 2,
        )
        {self.id}.series.priceScale().applyOptions({{
            scaleMargins: {{top:{scale_margin_top}, bottom: {scale_margin_bottom}}}
        }})''')

    def delete(self):
        """
        Irreversibly deletes the histogram.
        """
        self.run_script(f'''
            {self.id}legendItem = {self._chart.id}.legend._lines.find((line) => line.series == {self.id}.series)
            {self._chart.id}.legend._lines = {self._chart.id}.legend._lines.filter((item) => item != {self.id}legendItem)

            if ({self.id}legendItem) {{
                {self._chart.id}.legend.div.removeChild({self.id}legendItem.row)
            }}

            {self._chart.id}.chart.removeSeries({self.id}.series)
            delete {self.id}legendItem
            delete {self.id}
        ''')

    def scale(self, scale_margin_top: float = 0.0, scale_margin_bottom: float = 0.0):
        self.run_script(f'''
        {self.id}.series.priceScale().applyOptions({{
            scaleMargins: {{top: {scale_margin_top}, bottom: {scale_margin_bottom}}}
        }})''')


class Candlestick(SeriesCommon):
    def __init__(self, chart: 'AbstractChart'):
        super().__init__(chart)
        self._volume_up_color = 'rgba(83,141,131,0.8)'
        self._volume_down_color = 'rgba(200,127,130,0.8)'

        self.candle_data = pd.DataFrame()

        # self.run_script(f'{self.id}.makeCandlestickSeries()')

    @staticmethod
    def _normalize_indicator_type(series_type: str) -> str:
        normalized = series_type.strip().lower()
        if normalized == 'hist':
            normalized = 'histogram'
        if normalized not in ('line', 'histogram'):
            raise ValueError(f'Invalid indicator type "{series_type}". Use "line" or "histogram".')
        return normalized

    @staticmethod
    def _normalize_indicators_spec(indicators: Optional[dict]) -> Dict[str, Dict[str, str]]:
        if indicators is None:
            return {}
        if not isinstance(indicators, dict):
            raise TypeError('indicators must be a dict.')

        normalized: Dict[str, Dict[str, str]] = {}
        for column, spec in indicators.items():
            if not isinstance(column, str):
                raise TypeError('Indicator names must be strings.')
            if isinstance(spec, (list, tuple)):
                if len(spec) != 2:
                    raise ValueError(f'Indicator "{column}" list format must be [pane, type].')
                pane, series_type = spec
            elif isinstance(spec, dict):
                pane = spec.get('pane', 'main')
                series_type = spec.get('type', 'line')
            else:
                raise TypeError(
                    f'Indicator "{column}" spec must be [pane, type] or dict with pane/type keys.'
                )

            if not isinstance(pane, str):
                raise TypeError(f'Indicator "{column}" pane must be a string.')
            if not isinstance(series_type, str):
                raise TypeError(f'Indicator "{column}" type must be a string.')

            pane_value = pane.strip()
            if not pane_value:
                raise ValueError(f'Indicator "{column}" pane cannot be empty.')

            pane_lower = pane_value.lower()
            if pane_lower == 'main':
                pane_key = 'main'
            elif pane_lower == 'subplot':
                pane_key = f'subplot:{column}'
            else:
                pane_key = f'pane:{pane_lower}'

            normalized[column] = {
                'pane': pane_value,
                'pane_key': pane_key,
                'type': Candlestick._normalize_indicator_type(series_type),
            }
        return normalized

    def _get_or_create_indicator_chart(self, pane_key: str) -> 'AbstractChart':
        if pane_key == 'main':
            return self
        pane_chart = self._indicator_panes.get(pane_key)
        if pane_chart is None:
            indicator_chart = self.create_subchart(
                width=1.0,
                height=0.2,
                sync=True,
                sync_mode='main'
            )
            indicator_chart.legend(visible=True, ohlc=False, percent=False, lines=True)
            indicator_chart.hide_data()
            indicator_chart.crosshair(horz_visible=False)
            indicator_chart.run_script(f'{indicator_chart.id}.chart.applyOptions({{handleScroll: false, handleScale: false}})')
            self._indicator_panes[pane_key] = indicator_chart
            pane_chart = indicator_chart
        if pane_key not in self._auto_indicator_pane_keys:
            self._auto_indicator_pane_keys.append(pane_key)
            self._rebalance_auto_indicator_panes()
        return pane_chart

    def _rebalance_auto_indicator_panes(self):
        pane_count = len(self._auto_indicator_pane_keys)
        if pane_count == 0:
            return

        preferred_pane_height = 0.2
        preferred_main_height = 1 - (pane_count * preferred_pane_height)
        main_height = max(0.2, preferred_main_height)
        pane_height = (1 - main_height) / pane_count

        self.resize(height=main_height)
        for pane_key in self._auto_indicator_pane_keys:
            pane_chart = self._indicator_panes.get(pane_key)
            if pane_chart is None:
                continue
            pane_chart.resize(height=pane_height)

    def _create_indicator_series(
        self,
        pane_chart: 'AbstractChart',
        column: str,
        series_type: str
    ) -> Union['Line', 'Histogram']:
        if series_type == 'line':
            return pane_chart.create_line(name=column, price_line=False, price_label=False)
        return pane_chart.create_histogram(name=column, price_line=False, price_label=False)

    def _set_indicator_pane_timeline(self, pane_chart: 'AbstractChart', df: pd.DataFrame):
        timeline = df[['time']].copy()
        timeline['open'] = 0.0
        timeline['high'] = 0.0
        timeline['low'] = 0.0
        timeline['close'] = 0.0
        pane_chart.run_script(f'{pane_chart.id}.series.setData({js_data(timeline)})')
        pane_chart.run_script(f'{pane_chart.id}.volumeSeries.setData([])')

    @staticmethod
    def _sanitize_ohlc_rows(df: pd.DataFrame) -> Optional[pd.Series]:
        ohlc_cols = ['open', 'high', 'low', 'close']
        if any(col not in df.columns for col in ohlc_cols):
            return None
        valid_mask = df[ohlc_cols].notna().all(axis=1)
        if not bool(valid_mask.all()):
            df.loc[~valid_mask, ohlc_cols] = pd.NA
        return valid_mask

    def _apply_indicators(
        self,
        df: pd.DataFrame,
        indicators: Optional[dict],
        valid_ohlc_mask: Optional[pd.Series] = None
    ):
        if indicators is None:
            return
        normalized = self._normalize_indicators_spec(indicators)
        resolved: Dict[str, Dict[str, str]] = {}
        for indicator, spec in normalized.items():
            resolved_name = (
                indicator
                if indicator in df.columns
                else indicator.lower() if indicator.lower() in df.columns else None
            )
            if resolved_name is None:
                raise NameError(f'No column named "{indicator}" for indicators.')
            resolved[resolved_name] = spec

        active_indicators = set(resolved)
        for indicator in list(self._indicator_series):
            if indicator in active_indicators:
                continue
            self._indicator_series[indicator]['series'].delete()
            del self._indicator_series[indicator]

        timeline_synced_panes = set()

        for indicator, spec in resolved.items():
            existing = self._indicator_series.get(indicator)
            needs_recreate = (
                existing is None
                or existing['pane_key'] != spec['pane_key']
                or existing['type'] != spec['type']
            )
            if needs_recreate:
                if existing is not None:
                    existing['series'].delete()
                pane_chart = self._get_or_create_indicator_chart(spec['pane_key'])
                indicator_series = self._create_indicator_series(pane_chart, indicator, spec['type'])
                self._indicator_series[indicator] = {
                    'series': indicator_series,
                    'pane_key': spec['pane_key'],
                    'type': spec['type'],
                }
            pane_key = self._indicator_series[indicator]['pane_key']
            if pane_key != 'main' and pane_key not in timeline_synced_panes:
                pane_chart = self._indicator_series[indicator]['series']._chart
                self._set_indicator_pane_timeline(pane_chart, df)
                timeline_synced_panes.add(pane_key)
            indicator_df = df[['time', indicator]].copy()
            if valid_ohlc_mask is not None:
                indicator_df.loc[~valid_ohlc_mask, indicator] = pd.NA
            self._indicator_series[indicator]['series'].set(
                indicator_df,
                format_cols=False
            )

        active_pane_keys = {
            meta['pane_key']
            for meta in self._indicator_series.values()
            if meta['pane_key'] != 'main'
        }
        removed_panes = [pane_key for pane_key in self._auto_indicator_pane_keys if pane_key not in active_pane_keys]
        for pane_key in removed_panes:
            pane_chart = self._indicator_panes.get(pane_key)
            if pane_chart is not None:
                pane_chart.run_script(f'{pane_chart.id}.series.setData([])')
                pane_chart.run_script(f'{pane_chart.id}.volumeSeries.setData([])')
                pane_chart.resize(height=0)
            while pane_key in self._auto_indicator_pane_keys:
                self._auto_indicator_pane_keys.remove(pane_key)
        if removed_panes:
            if self._auto_indicator_pane_keys:
                self._rebalance_auto_indicator_panes()
            else:
                self.resize(height=1.0)

    def _normalize_df_for_engine(self, df: pd.DataFrame) -> pd.DataFrame:
        normalized = df.copy()
        normalized.columns = self._format_labels(normalized, normalized.columns, normalized.index, None)
        if 'time' not in normalized.columns:
            raise NameError('No "time" or "date" column found for engine-backed set().')
        if not pd.api.types.is_datetime64_any_dtype(normalized['time']):
            normalized['time'] = self._to_datetime_series(normalized['time'])
        return (
            normalized
            .sort_values('time')
            .drop_duplicates(subset=['time'], keep='last')
            .reset_index(drop=True)
        )

    def _cleanup_engine_artifacts(self):
        for artifact_dir in self._engine_artifact_dirs:
            shutil.rmtree(artifact_dir, ignore_errors=True)
        self._engine_artifact_dirs.clear()

    def _create_internal_duckdb_source(self, df: pd.DataFrame) -> DuckDBSource:
        try:
            import duckdb  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                'duckdb is required for engine="duckdb". Install with: pip install duckdb'
            ) from exc

        self._cleanup_engine_artifacts()
        normalized = self._normalize_df_for_engine(df)
        artifact_dir = Path(tempfile.mkdtemp(prefix='lwc_duckdb_'))
        parquet_path = artifact_dir / 'dataset.parquet'
        db_path = artifact_dir / 'dataset.duckdb'

        con = duckdb.connect(database=str(db_path))
        try:
            con.register('src_df', normalized)
            con.execute('CREATE TABLE candles AS SELECT * FROM src_df')
            safe_path = str(parquet_path).replace("'", "''")
            con.execute(f"COPY candles TO '{safe_path}' (FORMAT PARQUET)")
        finally:
            con.close()

        self._engine_artifact_dirs.append(str(artifact_dir))
        return DuckDBSource(
            database=str(db_path),
            table='candles',
            time_col='time',
            time_unit='timestamp',
        )

    def reset(self, keep_drawings: bool = False):
        self.stop_stream()
        self._cleanup_engine_artifacts()

        self.candle_data = pd.DataFrame()
        self.data = pd.DataFrame()
        self._last_bar = None

        self.run_script(f'{self.id}.series.setData([])')
        self.run_script(f'{self.id}.volumeSeries.setData([])')

        for line in self._lines:
            line.set(pd.DataFrame())

        for indicator in list(self._indicator_series):
            self._indicator_series[indicator]['series'].delete()
            del self._indicator_series[indicator]

        for pane_chart in self._indicator_panes.values():
            pane_chart.run_script(f'{pane_chart.id}.series.setData([])')
            pane_chart.run_script(f'{pane_chart.id}.volumeSeries.setData([])')
            pane_chart.resize(height=0)
        self._auto_indicator_pane_keys.clear()
        self.resize(height=1.0)

        if keep_drawings:
            self.run_script(f'{self._chart.id}.toolBox?._drawingTool.repositionOnTime()')
        else:
            self.run_script(f"{self._chart.id}.toolBox?.clearDrawings()")

    def set(
        self,
        df: Optional[pd.DataFrame] = None,
        keep_drawings: bool = False,
        indicators: Optional[dict] = None,
        engine: Optional[Literal['pandas', 'duckdb']] = None,
        engine_options: Optional[dict] = None,
        _from_stream: bool = False,
        render_drawings: Optional[bool] = None,
    ):
        """
        Sets the initial data for the chart.\n
        :param df: columns: date/time, open, high, low, close, volume (if volume enabled).
        :param keep_drawings: keeps any drawings made through the toolbox. Otherwise, they will be deleted.
        :param indicators: optional indicator spec mapping indicator column to pane/type.
        :param engine: optional backend for large datasets (`duckdb` enables internal streaming).
        :param engine_options: optional streaming options for engine mode.
        """
        if render_drawings is not None:
            keep_drawings = bool(render_drawings)

        selected_engine = (engine or 'pandas').strip().lower()
        if selected_engine not in ('pandas', 'duckdb'):
            raise ValueError('engine must be "pandas" or "duckdb".')

        if not _from_stream and selected_engine != 'duckdb':
            self.stop_stream()
            self._cleanup_engine_artifacts()

        if selected_engine == 'duckdb' and not _from_stream:
            if df is None or df.empty:
                self.reset(keep_drawings=keep_drawings)
                return
            options = engine_options.copy() if engine_options else {}
            unknown_option_keys = sorted(
                set(options.keys()) - {'initial_bars', 'chunk_bars', 'prefetch_bars', 'max_bars', 'debounce_ms', 'keep_drawings'}
            )
            if unknown_option_keys:
                unknown_options = ', '.join(unknown_option_keys)
                raise ValueError(f'Unknown engine_options for engine="duckdb": {unknown_options}')
            self.stop_stream()
            source = self._create_internal_duckdb_source(df)
            initial_bars = int(options.pop('initial_bars', 2000))
            chunk_bars = int(options.pop('chunk_bars', 1200))
            prefetch_bars = int(options.pop('prefetch_bars', 300))
            max_bars = int(options.pop('max_bars', 20000))
            debounce_ms = int(options.pop('debounce_ms', 80))
            stream_keep_drawings = bool(options.pop('keep_drawings', keep_drawings))

            # Static backends (e.g., Jupyter iframe) have no JS->Python callback bridge.
            if self.win.script_func is None:
                initial_df = source.get_latest(initial_bars)
                source.close()
                self.set(
                    initial_df,
                    keep_drawings=stream_keep_drawings,
                    indicators=indicators,
                    _from_stream=True,
                )
                warnings.warn(
                    'engine="duckdb" loaded only the latest window because this chart backend '
                    'does not support JS callbacks for dynamic streaming.',
                    RuntimeWarning,
                )
                return

            self.set_stream(
                source=source,
                indicators=indicators,
                initial_bars=initial_bars,
                chunk_bars=chunk_bars,
                prefetch_bars=prefetch_bars,
                max_bars=max_bars,
                debounce_ms=debounce_ms,
                keep_drawings=stream_keep_drawings,
            )
            return

        if df is None or df.empty:
            self.run_script(f'{self.id}.series.setData([])')
            self.run_script(f'{self.id}.volumeSeries.setData([])')
            self.candle_data = pd.DataFrame()
            return
        df = self._df_datetime_format(df)
        if df.empty:
            self.run_script(f'{self.id}.series.setData([])')
            self.run_script(f'{self.id}.volumeSeries.setData([])')
            self.candle_data = pd.DataFrame()
            return
        valid_ohlc_mask = self._sanitize_ohlc_rows(df)
        self.candle_data = df.copy()
        self._last_bar = df.iloc[-1]
        self.run_script(f'{self.id}.series.setData({js_data(df)})')

        if 'volume' in df:
            volume = df.drop(columns=['open', 'high', 'low', 'close']).rename(columns={'volume': 'value'})
            if valid_ohlc_mask is not None:
                volume.loc[~valid_ohlc_mask, 'value'] = pd.NA
            volume['color'] = self._volume_down_color
            volume.loc[df['close'] > df['open'], 'color'] = self._volume_up_color
            volume.loc[volume['value'].isna(), 'color'] = None
            self.run_script(f'{self.id}.volumeSeries.setData({js_data(volume)})')
        else:
            self.run_script(f'{self.id}.volumeSeries.setData([])')

        for line in self._lines:
            if line.name not in df.columns:
                continue
            line.set(df[['time', line.name]], format_cols=False)
        self._apply_indicators(df, indicators, valid_ohlc_mask=valid_ohlc_mask)
        # set autoScale to true in case the user has dragged the price scale
        self.run_script(f'''
            if (!{self.id}.chart.priceScale("right").options.autoScale)
                {self.id}.chart.priceScale("right").applyOptions({{autoScale: true}})
        ''')
        # TODO keep drawings doesn't work consistenly w
        if keep_drawings:
            self.run_script(f'{self._chart.id}.toolBox?._drawingTool.repositionOnTime()')
        else:
            self.run_script(f"{self._chart.id}.toolBox?.clearDrawings()")

    def _install_stream_listener(self, handler_name: str, debounce_ms: int):
        chart_salt = self.id[self.id.index('.')+1:]
        self.run_script(f'''
            if ({self.id}._streamRangeHandler{chart_salt}) {{
                {self.id}.chart.timeScale().unsubscribeVisibleTimeRangeChange({self.id}._streamRangeHandler{chart_salt})
            }}
            if ({self.id}._streamRangeTimer{chart_salt}) clearTimeout({self.id}._streamRangeTimer{chart_salt})

            {self.id}._streamRangeHandler{chart_salt} = (range) => {{
                if (!range) return
                if (typeof window.callbackFunction !== "function") return
                if ({self.id}._streamRangeTimer{chart_salt}) clearTimeout({self.id}._streamRangeTimer{chart_salt})
                {self.id}._streamRangeTimer{chart_salt} = setTimeout(() => {{
                    window.callbackFunction("{handler_name}_~_" + range.from + ";;;" + range.to)
                }}, {debounce_ms})
            }}
            {self.id}.chart.timeScale().subscribeVisibleTimeRangeChange({self.id}._streamRangeHandler{chart_salt})
        ''')

    def stop_stream(self):
        controller = getattr(self, '_stream_controller', None)
        if controller:
            controller['enabled'] = False
            handler_name = controller.get('handler_name')
            source = controller.get('source')
            if source is not None and hasattr(source, 'close'):
                try:
                    source.close()
                except Exception:
                    pass
            if handler_name in self.win.handlers:
                del self.win.handlers[handler_name]
            chart_salt = self.id[self.id.index('.')+1:]
            self.run_script(f'''
                if ({self.id}._streamRangeHandler{chart_salt}) {{
                    {self.id}.chart.timeScale().unsubscribeVisibleTimeRangeChange({self.id}._streamRangeHandler{chart_salt})
                    {self.id}._streamRangeHandler{chart_salt} = null
                }}
                if ({self.id}._streamRangeTimer{chart_salt}) {{
                    clearTimeout({self.id}._streamRangeTimer{chart_salt})
                    {self.id}._streamRangeTimer{chart_salt} = null
                }}
            ''')
            self._stream_controller = None

    def set_stream(
        self,
        source: Union[pd.DataFrame, StreamingSource],
        indicators: Optional[dict] = None,
        initial_bars: int = 2000,
        chunk_bars: int = 1200,
        prefetch_bars: int = 300,
        max_bars: int = 20000,
        debounce_ms: int = 80,
        keep_drawings: bool = True,
    ):
        if isinstance(source, pd.DataFrame):
            source = PandasSource(source)
        if not isinstance(source, StreamingSource):
            raise TypeError('source must be a pandas.DataFrame or StreamingSource implementation.')
        if initial_bars <= 0 or chunk_bars <= 0 or prefetch_bars < 0 or max_bars <= 0:
            raise ValueError('initial_bars/chunk_bars/max_bars must be > 0 and prefetch_bars must be >= 0.')

        self.stop_stream()

        config = StreamConfig(
            initial_bars=initial_bars,
            chunk_bars=chunk_bars,
            prefetch_bars=prefetch_bars,
            max_bars=max_bars,
            debounce_ms=debounce_ms,
            keep_drawings=keep_drawings,
        )

        initial_df = source.get_latest(config.initial_bars)
        self.set(initial_df, keep_drawings=config.keep_drawings, indicators=indicators, _from_stream=True)

        chart_salt = self.id[self.id.index('.')+1:]
        handler_name = f'stream_range{chart_salt}'
        state = {
            'enabled': True,
            'updating': False,
            'source': source,
            'indicators': indicators,
            'config': config,
            'handler_name': handler_name,
            'window_df': initial_df.copy(),
        }
        self._stream_controller = state

        def series_to_seconds(series: pd.Series):
            if pd.api.types.is_datetime64_any_dtype(series):
                return (series.astype('int64') // 10 ** 9).astype(float)
            if pd.api.types.is_numeric_dtype(series):
                return series.astype(float)
            return pd.to_datetime(series).astype('int64') // 10 ** 9

        def on_stream_range(from_time, to_time):
            if not state['enabled'] or state['updating']:
                return
            try:
                from_ts = float(from_time)
                to_ts = float(to_time)
            except (TypeError, ValueError):
                return

            loaded = state['window_df']
            if loaded.empty or 'time' not in loaded:
                return

            loaded_seconds = series_to_seconds(loaded['time'])
            loaded_min = float(loaded_seconds.iloc[0])
            loaded_max = float(loaded_seconds.iloc[-1])
            threshold = max(self._interval, self._interval * config.prefetch_bars)

            fetch_left = (from_ts - loaded_min) <= threshold
            fetch_right = (loaded_max - to_ts) <= threshold
            if not fetch_left and not fetch_right:
                return

            frames = []
            if fetch_left:
                left_df = source.get_before(loaded_min, config.chunk_bars)
                if not left_df.empty:
                    frames.append(left_df)

            frames.append(loaded)

            if fetch_right:
                right_df = source.get_after(loaded_max, config.chunk_bars)
                if not right_df.empty:
                    frames.append(right_df)

            if len(frames) == 1:
                return

            merged = pd.concat(frames, ignore_index=True)
            merged = merged.sort_values('time').drop_duplicates(subset=['time'], keep='last').reset_index(drop=True)
            if len(merged) > config.max_bars:
                if fetch_left and not fetch_right:
                    merged = merged.iloc[-config.max_bars:].reset_index(drop=True)
                elif fetch_right and not fetch_left:
                    merged = merged.iloc[:config.max_bars].reset_index(drop=True)
                else:
                    center = (from_ts + to_ts) / 2
                    merged_seconds = series_to_seconds(merged['time'])
                    center_pos = merged_seconds.sub(center).abs().idxmin()
                    start = max(0, int(center_pos) - (config.max_bars // 2))
                    end = min(len(merged), start + config.max_bars)
                    start = max(0, end - config.max_bars)
                    merged = merged.iloc[start:end].reset_index(drop=True)

            state['updating'] = True
            try:
                state['window_df'] = merged.copy()
                self.set(merged, keep_drawings=True, indicators=state['indicators'], _from_stream=True)
            finally:
                state['updating'] = False

        self.win.handlers[handler_name] = on_stream_range
        self._install_stream_listener(handler_name, config.debounce_ms)
        return self

    def update(self, series: pd.Series, _from_tick=False):
        """
        Updates the data from a bar;
        if series['time'] is the same time as the last bar, the last bar will be overwritten.\n
        :param series: labels: date/time, open, high, low, close, volume (if using volume).
        """
        series = self._series_datetime_format(series) if not _from_tick else series
        if series['time'] != self._last_bar['time']:
            self.candle_data.loc[self.candle_data.index[-1]] = self._last_bar
            self.candle_data = pd.concat([self.candle_data, series.to_frame().T], ignore_index=True)
            self._chart.events.new_bar._emit(self)

        self._last_bar = series
        self.run_script(f'{self.id}.series.update({js_data(series)})')
        if 'volume' not in series:
            return
        volume = series.drop(['open', 'high', 'low', 'close']).rename({'volume': 'value'})
        volume['color'] = self._volume_up_color if series['close'] > series['open'] else self._volume_down_color
        self.run_script(f'{self.id}.volumeSeries.update({js_data(volume)})')

    def update_from_tick(self, series: pd.Series, cumulative_volume: bool = False):
        """
        Updates the data from a tick.\n
        :param series: labels: date/time, price, volume (if using volume).
        :param cumulative_volume: Adds the given volume onto the latest bar.
        """
        series = self._series_datetime_format(series)
        if series['time'] < self._last_bar['time']:
            raise ValueError(f'Trying to update tick of time "{pd.to_datetime(series["time"])}", which occurs before the last bar time of "{pd.to_datetime(self._last_bar["time"])}".')
        bar = pd.Series(dtype='float64')
        if series['time'] == self._last_bar['time']:
            bar = self._last_bar
            bar['high'] = max(self._last_bar['high'], series['price'])
            bar['low'] = min(self._last_bar['low'], series['price'])
            bar['close'] = series['price']
            if 'volume' in series:
                if cumulative_volume:
                    bar['volume'] += series['volume']
                else:
                    bar['volume'] = series['volume']
        else:
            for key in ('open', 'high', 'low', 'close'):
                bar[key] = series['price']
            bar['time'] = series['time']
            if 'volume' in series:
                bar['volume'] = series['volume']
        self.update(bar, _from_tick=True)

    def price_scale(
        self,
        auto_scale: bool = True,
        mode: PRICE_SCALE_MODE = 'normal',
        invert_scale: bool = False,
        align_labels: bool = True,
        scale_margin_top: float = 0.2,
        scale_margin_bottom: float = 0.2,
        border_visible: bool = False,
        border_color: Optional[str] = None,
        text_color: Optional[str] = None,
        entire_text_only: bool = False,
        visible: bool = True,
        ticks_visible: bool = False,
        minimum_width: int = 0
    ):
        self.run_script(f'''
            {self.id}.series.priceScale().applyOptions({{
                autoScale: {jbool(auto_scale)},
                mode: {as_enum(mode, PRICE_SCALE_MODE)},
                invertScale: {jbool(invert_scale)},
                alignLabels: {jbool(align_labels)},
                scaleMargins: {{top: {scale_margin_top}, bottom: {scale_margin_bottom}}},
                borderVisible: {jbool(border_visible)},
                {f'borderColor: "{border_color}",' if border_color else ''}
                {f'textColor: "{text_color}",' if text_color else ''}
                entireTextOnly: {jbool(entire_text_only)},
                visible: {jbool(visible)},
                ticksVisible: {jbool(ticks_visible)},
                minimumWidth: {minimum_width}
            }})''')

    def candle_style(
            self, up_color: str = 'rgba(39, 157, 130, 100)', down_color: str = 'rgba(200, 97, 100, 100)',
            wick_visible: bool = True, border_visible: bool = True, border_up_color: str = '',
            border_down_color: str = '', wick_up_color: str = '', wick_down_color: str = ''):
        """
        Candle styling for each of its parts.\n
        If only `up_color` and `down_color` are passed, they will color all parts of the candle.
        """
        border_up_color = border_up_color if border_up_color else up_color
        border_down_color = border_down_color if border_down_color else down_color
        wick_up_color = wick_up_color if wick_up_color else up_color
        wick_down_color = wick_down_color if wick_down_color else down_color
        self.run_script(f"{self.id}.series.applyOptions({js_json(locals())})")

    def volume_config(self, scale_margin_top: float = 0.8, scale_margin_bottom: float = 0.0,
                      up_color='rgba(83,141,131,0.8)', down_color='rgba(200,127,130,0.8)'):
        """
        Configure volume settings.\n
        Numbers for scaling must be greater than 0 and less than 1.\n
        Volume colors must be applied prior to setting/updating the bars.\n
        """
        self._volume_up_color = up_color if up_color else self._volume_up_color
        self._volume_down_color = down_color if down_color else self._volume_down_color
        self.run_script(f'''
        {self.id}.volumeSeries.priceScale().applyOptions({{
            scaleMargins: {{
            top: {scale_margin_top},
            bottom: {scale_margin_bottom},
            }}
        }})''')


class AbstractChart(Candlestick, Pane):
    def __init__(self, window: Window, width: float = 1.0, height: float = 1.0,
                 scale_candles_only: bool = False, toolbox: bool = False,
                 autosize: bool = True, position: FLOAT = 'left'):
        Pane.__init__(self, window)

        self._lines = []
        self._indicator_series: Dict[str, Dict[str, Any]] = {}
        self._indicator_panes: Dict[str, 'AbstractChart'] = {}
        self._auto_indicator_pane_keys: List[str] = []
        self._stream_controller: Optional[dict] = None
        self._engine_artifact_dirs: List[str] = []
        self._scale_candles_only = scale_candles_only
        self._width = width
        self._height = height
        self.events: Events = Events(self)

        from lightweight_charts.polygon import PolygonAPI
        self.polygon: PolygonAPI = PolygonAPI(self)

        self.run_script(
            f'{self.id} = new Lib.Handler("{self.id}", {width}, {height}, "{position}", {jbool(autosize)})')

        Candlestick.__init__(self, self)

        self.topbar: TopBar = TopBar(self)
        if toolbox:
            self.toolbox: ToolBox = ToolBox(self)

    def fit(self):
        """
        Fits the maximum amount of the chart data within the viewport.
        """
        self.run_script(f'{self.id}.chart.timeScale().fitContent()')

    def create_line(
            self, name: str = '', color: str = 'rgba(214, 237, 255, 0.6)',
            style: LINE_STYLE = 'solid', width: int = 2,
            price_line: bool = True, price_label: bool = True, price_scale_id: Optional[str] = None
    ) -> Line:
        """
        Creates and returns a Line object.
        """
        self._lines.append(Line(self, name, color, style, width, price_line, price_label, price_scale_id))
        return self._lines[-1]

    def create_histogram(
            self, name: str = '', color: str = 'rgba(214, 237, 255, 0.6)',
            price_line: bool = True, price_label: bool = True,
            scale_margin_top: float = 0.0, scale_margin_bottom: float = 0.0
    ) -> Histogram:
        """
        Creates and returns a Histogram object.
        """
        return Histogram(
            self, name, color, price_line, price_label,
            scale_margin_top, scale_margin_bottom)

    def lines(self) -> List[Line]:
        """
        Returns all lines for the chart.
        """
        return self._lines.copy()

    def set_visible_range(self, start_time: TIME, end_time: TIME):
        self.run_script(f'''
        {self.id}.chart.timeScale().setVisibleRange({{
            from: {pd.to_datetime(start_time).timestamp()},
            to: {pd.to_datetime(end_time).timestamp()}
        }})
        ''')

    def resize(self, width: Optional[float] = None, height: Optional[float] = None):
        """
        Resizes the chart within the window.
        Dimensions should be given as a float between 0 and 1.
        """
        self._width = width if width is not None else self._width
        self._height = height if height is not None else self._height
        self.run_script(f'''
        {self.id}.scale.width = {self._width}
        {self.id}.scale.height = {self._height}
        {self.id}.reSize()
        ''')

    def time_scale(self, right_offset: int = 0, min_bar_spacing: float = 0.5,
                   visible: bool = True, time_visible: bool = True, seconds_visible: bool = False,
                   border_visible: bool = True, border_color: Optional[str] = None):
        """
        Options for the timescale of the chart.
        """
        self.run_script(f'''{self.id}.chart.applyOptions({{timeScale: {js_json(locals())}}})''')

    def layout(self, background_color: str = '#000000', text_color: Optional[str] = None,
               font_size: Optional[int] = None, font_family: Optional[str] = None):
        """
        Global layout options for the chart.
        """
        self.run_script(f"""
            document.getElementById('container').style.backgroundColor = '{background_color}'
            {self.id}.chart.applyOptions({{
            layout: {{
                background: {{color: "{background_color}"}},
                {f'textColor: "{text_color}",' if text_color else ''}
                {f'fontSize: {font_size},' if font_size else ''}
                {f'fontFamily: "{font_family}",' if font_family else ''}
            }}}})""")

    def grid(self, vert_enabled: bool = True, horz_enabled: bool = True,
             color: str = 'rgba(29, 30, 38, 5)', style: LINE_STYLE = 'solid'):
        """
        Grid styling for the chart.
        """
        self.run_script(f"""
           {self.id}.chart.applyOptions({{
           grid: {{
               vertLines: {{
                   visible: {jbool(vert_enabled)},
                   color: "{color}",
                   style: {as_enum(style, LINE_STYLE)},
               }},
               horzLines: {{
                   visible: {jbool(horz_enabled)},
                   color: "{color}",
                   style: {as_enum(style, LINE_STYLE)},
               }},
           }}
           }})""")

    def crosshair(
        self,
        mode: CROSSHAIR_MODE = 'normal',
        vert_visible: bool = True,
        vert_width: int = 1,
        vert_color: Optional[str] = None,
        vert_style: LINE_STYLE = 'large_dashed',
        vert_label_background_color: str = 'rgb(46, 46, 46)',
        horz_visible: bool = True,
        horz_width: int = 1,
        horz_color: Optional[str] = None,
        horz_style: LINE_STYLE = 'large_dashed',
        horz_label_background_color: str = 'rgb(55, 55, 55)'
    ):
        """
        Crosshair formatting for its vertical and horizontal axes.
        """
        self.run_script(f'''
        {self.id}.chart.applyOptions({{
            crosshair: {{
                mode: {as_enum(mode, CROSSHAIR_MODE)},
                vertLine: {{
                    visible: {jbool(vert_visible)},
                    width: {vert_width},
                    {f'color: "{vert_color}",' if vert_color else ''}
                    style: {as_enum(vert_style, LINE_STYLE)},
                    labelBackgroundColor: "{vert_label_background_color}"
                }},
                horzLine: {{
                    visible: {jbool(horz_visible)},
                    width: {horz_width},
                    {f'color: "{horz_color}",' if horz_color else ''}
                    style: {as_enum(horz_style, LINE_STYLE)},
                    labelBackgroundColor: "{horz_label_background_color}"
                }}
            }}
        }})''')

    def watermark(self, text: str, font_size: int = 44, color: str = 'rgba(180, 180, 200, 0.5)'):
        """
        Adds a watermark to the chart.
        """
        self.run_script(f'''
          {self.id}.chart.applyOptions({{
              watermark: {{
                  visible: true,
                  horzAlign: 'center',
                  vertAlign: 'center',
                  ...{js_json(locals())}
              }}
          }})''')

    def legend(self, visible: bool = False, ohlc: bool = True, percent: bool = True, lines: bool = True,
               color: str = 'rgb(191, 195, 203)', font_size: int = 11, font_family: str = 'Monaco',
               text: str = '', color_based_on_candle: bool = False):
        """
        Configures the legend of the chart.
        """
        l_id = f'{self.id}.legend'
        if not visible:
            self.run_script(f'''
            {l_id}.div.style.display = "none"
            {l_id}.ohlcEnabled = false
            {l_id}.percentEnabled = false
            {l_id}.linesEnabled = false
            ''')
            return
        self.run_script(f'''
        {l_id}.div.style.display = 'flex'
        {l_id}.ohlcEnabled = {jbool(ohlc)}
        {l_id}.percentEnabled = {jbool(percent)}
        {l_id}.linesEnabled = {jbool(lines)}
        {l_id}.colorBasedOnCandle = {jbool(color_based_on_candle)}
        {l_id}.div.style.color = '{color}'
        {l_id}.color = '{color}'
        {l_id}.div.style.fontSize = '{font_size}px'
        {l_id}.div.style.fontFamily = '{font_family}'
        {l_id}.text.innerText = '{text}'
        ''')

    def spinner(self, visible):
        self.run_script(f"{self.id}.spinner.style.display = '{'block' if visible else 'none'}'")

    def hotkey(self, modifier_key: Literal['ctrl', 'alt', 'shift', 'meta', None],
               keys: Union[str, tuple, int], func: Callable):
        if not isinstance(keys, tuple):
            keys = (keys,)
        for key in keys:
            key = str(key)
            if key.isalnum() and len(key) == 1:
                key_code = f'Digit{key}' if key.isdigit() else f'Key{key.upper()}'
                key_condition = f'event.code === "{key_code}"'
            else:
                key_condition = f'event.key === "{key}"'
            if modifier_key is not None:
                key_condition += f'&& event.{modifier_key}Key'

            self.run_script(f'''
                    {self.id}.commandFunctions.unshift((event) => {{
                        if ({key_condition}) {{
                            event.preventDefault()
                            window.callbackFunction(`{modifier_key, keys}_~_{key}`)
                            return true
                        }}
                        else return false
                    }})''')
        self.win.handlers[f'{modifier_key, keys}'] = func

    def create_table(
        self,
        width: NUM,
        height: NUM,
        headings: tuple,
        widths: Optional[tuple] = None,
        alignments: Optional[tuple] = None,
        position: FLOAT = 'left',
        draggable: bool = False,
        background_color: str = '#121417',
        border_color: str = 'rgb(70, 70, 70)',
        border_width: int = 1,
        heading_text_colors: Optional[tuple] = None,
        heading_background_colors: Optional[tuple] = None,
        return_clicked_cells: bool = False,
        func: Optional[Callable] = None
    ) -> Table:
        args = locals()
        del args['self']
        return self.win.create_table(*args.values())

    def screenshot(self) -> bytes:
        """
        Takes a screenshot. This method can only be used after the chart window is visible.
        :return: a bytes object containing a screenshot of the chart.
        """
        serial_data = self.win.run_script_and_get(f'{self.id}.chart.takeScreenshot().toDataURL()')
        return b64decode(serial_data.split(',')[1])

    def create_subchart(self, position: FLOAT = 'left', width: float = 0.5, height: float = 0.5,
                        sync: Optional[Union[str, bool]] = None, scale_candles_only: bool = False,
                        sync_crosshairs_only: bool = False,
                        sync_mode: Literal['main', 'active'] = 'main',
                        toolbox: bool = False) -> 'AbstractChart':
        if sync is True:
            sync = self.id
        args = locals()
        del args['self']
        return self.win.create_subchart(*args.values())
