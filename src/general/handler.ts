import {
    ColorType,
    CrosshairMode,
    DeepPartial,
    HistogramStyleOptions,
    IChartApi,
    ISeriesApi,
    LineStyleOptions,
    MouseEventHandler,
    MouseEventParams,
    SeriesOptionsCommon,
    SeriesType,
    Time,
    createChart
} from "lightweight-charts";

import { GlobalParams, globalParamInit } from "./global-params";
import { Legend } from "./legend";
import { ToolBox } from "./toolbox";
import { TopBar } from "./topbar";


export interface Scale{
    width: number,
    height: number,
}


globalParamInit();
declare const window: GlobalParams;

export class Handler {
    public id: string;
    public commandFunctions: Function[] = [];

    public wrapper: HTMLDivElement;
    public div: HTMLDivElement;

    public chart: IChartApi;
    public scale: Scale;
    public precision: number = 2;

    public series: ISeriesApi<SeriesType>;
    public volumeSeries: ISeriesApi<SeriesType>;

    public legend: Legend;
    private _topBar: TopBar | undefined;
    public toolBox: ToolBox | undefined;
    public spinner: HTMLDivElement | undefined;

    public _seriesList: ISeriesApi<SeriesType>[] = [];

    // TODO find a better solution rather than the 'position' parameter
    constructor(
        chartId: string,
        innerWidth: number,
        innerHeight: number,
        position: string,
        autoSize: boolean
    ) {
        this.reSize = this.reSize.bind(this)

        this.id = chartId
        this.scale = {
            width: innerWidth,
            height: innerHeight,
        }

        this.wrapper = document.createElement('div')
        this.wrapper.classList.add("handler");
        this.wrapper.style.float = position

        this.div = document.createElement('div')
        this.div.style.position = 'relative'

        this.wrapper.appendChild(this.div);
        window.containerDiv.append(this.wrapper)
        
        this.chart = this._createChart();
        this.series = this.createCandlestickSeries();
        this.volumeSeries = this.createVolumeSeries();

        this.legend = new Legend(this)
        
        document.addEventListener('keydown', (event) => {
            for (let i = 0; i < this.commandFunctions.length; i++) {
                if (this.commandFunctions[i](event)) break
            }
        })
        window.handlerInFocus = this.id;
        this.wrapper.addEventListener('mouseover', () => window.handlerInFocus = this.id)

        this.reSize()
        if (!autoSize) return
        window.addEventListener('resize', () => this.reSize())
    }


    reSize() {
        let topBarOffset = this.scale.height !== 0 ? this._topBar?._div.offsetHeight || 0 : 0
        this.chart.resize(window.innerWidth * this.scale.width, (window.innerHeight * this.scale.height) - topBarOffset)
        this.wrapper.style.width = `${100 * this.scale.width}%`
        this.wrapper.style.height = `${100 * this.scale.height}%`
        
        // TODO definitely a better way to do this
        if (this.scale.height === 0 || this.scale.width === 0) {
            // if (this.legend.div.style.display == 'flex') this.legend.div.style.display = 'none'
            if (this.toolBox) {
                this.toolBox.div.style.display = 'none'
            }
        }
        else {
            // this.legend.div.style.display = 'flex'
            if (this.toolBox) {
                this.toolBox.div.style.display = 'flex'
            }
        }
    }

    private _createChart() {
        return createChart(this.div, {
            width: window.innerWidth * this.scale.width,
            height: window.innerHeight * this.scale.height,
            layout:{
                textColor: window.pane.color,
                background: {
                    color: '#000000',
                    type: ColorType.Solid,
                },
                fontSize: 12
            },
            rightPriceScale: {
                scaleMargins: {top: 0.3, bottom: 0.25},
            },
            timeScale: {timeVisible: true, secondsVisible: false},
            crosshair: {
                mode: CrosshairMode.Normal,
                vertLine: {
                    labelBackgroundColor: 'rgb(46, 46, 46)'
                },
                horzLine: {
                    labelBackgroundColor: 'rgb(55, 55, 55)'
                }
            },
            grid: {
                vertLines: {color: 'rgba(29, 30, 38, 5)'},
                horzLines: {color: 'rgba(29, 30, 58, 5)'},
            },
            handleScroll: {vertTouchDrag: true},
        })
    }

    createCandlestickSeries() {
        const up = 'rgba(39, 157, 130, 100)'
        const down = 'rgba(200, 97, 100, 100)'
        const candleSeries = this.chart.addCandlestickSeries({
            upColor: up, borderUpColor: up, wickUpColor: up,
            downColor: down, borderDownColor: down, wickDownColor: down
        });
        candleSeries.priceScale().applyOptions({
            scaleMargins: {top: 0.2, bottom: 0.2},
        });
        return candleSeries;
    }

    createVolumeSeries() {
        const volumeSeries = this.chart.addHistogramSeries({
            color: '#26a69a',
            priceFormat: {type: 'volume'},
            priceScaleId: 'volume_scale',
        })
        volumeSeries.priceScale().applyOptions({
            scaleMargins: {top: 0.8, bottom: 0},
        });
        return volumeSeries;
    }

    createLineSeries(name: string, options: DeepPartial<LineStyleOptions & SeriesOptionsCommon>) {
        const line = this.chart.addLineSeries({...options});
        this._seriesList.push(line);
        this.legend.makeSeriesRow(name, line)
        return {
            name: name,
            series: line,
        }
    }

    createHistogramSeries(name: string, options: DeepPartial<HistogramStyleOptions & SeriesOptionsCommon>) {
        const line = this.chart.addHistogramSeries({...options});
        this._seriesList.push(line);
        this.legend.makeSeriesRow(name, line)
        return {
            name: name,
            series: line,
        }
    }

    createToolBox() {
        this.toolBox = new ToolBox(this.id, this.chart, this.series, this.commandFunctions);
        this.div.appendChild(this.toolBox.div);
    }

    createTopBar() {
        this._topBar = new TopBar(this);
        this.wrapper.prepend(this._topBar._div)
        return this._topBar;
    }

    toJSON() {
        // Exclude the chart attribute from serialization
        const {chart, ...serialized} = this;
        return serialized;
    }

    public static syncCharts(
        childChart: Handler,
        parentChart: Handler,
        crosshairOnly = false,
        syncMode: 'main' | 'active' = 'main'
    ) {
        const mode = syncMode === 'active' ? 'active' : 'main'
        const childTimeScale = childChart.chart.timeScale();
        const parentTimeScale = parentChart.chart.timeScale();

        const syncRegistry: Map<string, () => void> =
            (window as any).__syncRegistry || ((window as any).__syncRegistry = new Map<string, () => void>());
        const syncKey = `${parentChart.id}::${childChart.id}`;
        const previousDispose = syncRegistry.get(syncKey);
        if (previousDispose) {
            previousDispose();
            syncRegistry.delete(syncKey);
        }

        let isApplyingRange = false;
        const disposers: Array<() => void> = [];
        let resizeTimer: ReturnType<typeof setTimeout> | null = null;
        let syncWatchdog: ReturnType<typeof setInterval> | null = null;

        function hasPrice(point: any) {
            if (!point) return false;
            const price = point.value ?? point.close;
            if (price === undefined || price === null) return false;
            return Number.isFinite(Number(price));
        }

        function getLogicalIndex(chart: Handler, time: Time): number | null {
            const coordinate = chart.chart.timeScale().timeToCoordinate(time);
            if (coordinate === null) return null;
            const logical = chart.chart.timeScale().coordinateToLogical(coordinate);
            if (logical === null) return null;
            return logical.valueOf();
        }

        function getPointForTime(
            chart: Handler,
            time: Time
        ): { series: ISeriesApi<SeriesType>, point: any } | null {
            const index = getLogicalIndex(chart, time);
            if (index === null) return null;

            const mainPoint = chart.series.dataByIndex(index);
            const mainVisible = (chart.series.options() as any).visible !== false;
            if (mainVisible && hasPrice(mainPoint)) {
                return { series: chart.series, point: mainPoint };
            }

            const series = [...chart._seriesList, chart.volumeSeries];
            for (const candidate of series) {
                const point = candidate.dataByIndex(index);
                if (hasPrice(point)) {
                    return { series: candidate, point };
                }
            }

            if (hasPrice(mainPoint)) {
                return { series: chart.series, point: mainPoint };
            }
            return null;
        }

        function getPointFromParam(chart: Handler, param: MouseEventParams) {
            if (!param.time) return null;
            const mainVisible = (chart.series.options() as any).visible !== false;
            const candidates = [
                ...(mainVisible ? [param.seriesData.get(chart.series)] : []),
                ...chart._seriesList.map(series => param.seriesData.get(series)),
                ...(!mainVisible ? [param.seriesData.get(chart.series)] : []),
                param.seriesData.get(chart.volumeSeries),
                ...Array.from(param.seriesData.values())
            ];
            for (const point of candidates) {
                if (hasPrice(point)) {
                    return {
                        ...point,
                        time: point.time ?? param.time,
                    };
                }
            }
            return null;
        }

        function crosshairHandler(chart: Handler, point: any) {
            if (!point || !point.time) {
                chart.chart.clearCrosshairPosition()
                return
            }
            const pointAtTime = getPointForTime(chart, point.time);
            if (!pointAtTime) {
                chart.chart.clearCrosshairPosition()
                return
            }
            const series = pointAtTime.series;
            const pricePoint = pointAtTime.point;
            const price = pricePoint.value ?? pricePoint.close;
            const time = pricePoint.time ?? point.time;
            if (price === undefined || price === null || !Number.isFinite(Number(price))) {
                chart.chart.clearCrosshairPosition()
                return
            }
            try {
                chart.chart.setCrosshairPosition(Number(price), time, series);
                chart.legend.legendHandler({ ...point, ...pricePoint, time }, true)
            } catch (error) {
                chart.chart.clearCrosshairPosition()
            }
        }

        const syncChildToParentRange = () => {
            if (isApplyingRange) return;
            isApplyingRange = true;
            const logicalRange = parentTimeScale.getVisibleLogicalRange();
            if (logicalRange) {
                childTimeScale.setVisibleLogicalRange(logicalRange);
            } else {
                const timeRange = parentTimeScale.getVisibleRange();
                if (timeRange) childTimeScale.setVisibleRange(timeRange);
            }
            setTimeout(() => { isApplyingRange = false }, 0)
        }
        const setParentRange = (timeRange: { from: Time, to: Time } | null) => {
            if (!timeRange || isApplyingRange) return;
            isApplyingRange = true;
            parentTimeScale.setVisibleRange(timeRange);
            setTimeout(() => { isApplyingRange = false }, 0)
        }

        const setParentCrosshair = (param: MouseEventParams) => {
            crosshairHandler(parentChart, getPointFromParam(childChart, param))
        }
        const setChildCrosshair = (param: MouseEventParams) => {
            crosshairHandler(childChart, getPointFromParam(parentChart, param))
        }

        let activeCrosshairSource: 'parent' | 'child' = 'parent';
        const setActiveCrosshairSource = (source: 'parent' | 'child') => {
            parentChart.chart.unsubscribeCrosshairMove(setChildCrosshair)
            childChart.chart.unsubscribeCrosshairMove(setParentCrosshair)
            if (source === 'parent') {
                parentChart.chart.subscribeCrosshairMove(setChildCrosshair)
            } else {
                childChart.chart.subscribeCrosshairMove(setParentCrosshair)
            }
            activeCrosshairSource = source
        }
        setActiveCrosshairSource('parent')
        disposers.push(() => {
            parentChart.chart.unsubscribeCrosshairMove(setChildCrosshair)
            childChart.chart.unsubscribeCrosshairMove(setParentCrosshair)
        })

        syncChildToParentRange()

        let setActiveRangeSource: ((source: 'parent' | 'child') => void) | null = null;
        if (!crosshairOnly && mode === 'active') {
            let activeRangeSource: 'parent' | 'child' = 'parent';
            setActiveRangeSource = (source: 'parent' | 'child') => {
                parentTimeScale.unsubscribeVisibleTimeRangeChange(syncChildToParentRange)
                childTimeScale.unsubscribeVisibleTimeRangeChange(setParentRange)
                if (source === 'parent') {
                    parentTimeScale.subscribeVisibleTimeRangeChange(syncChildToParentRange)
                } else {
                    childTimeScale.subscribeVisibleTimeRangeChange(setParentRange)
                }
                activeRangeSource = source
            }
            setActiveRangeSource('parent')
            disposers.push(() => {
                parentTimeScale.unsubscribeVisibleTimeRangeChange(syncChildToParentRange)
                childTimeScale.unsubscribeVisibleTimeRangeChange(setParentRange)
            })
        }

        if (mode === 'active') {
            const onParentMouseOver = () => {
                if (activeCrosshairSource !== 'parent') setActiveCrosshairSource('parent')
                if (setActiveRangeSource) setActiveRangeSource('parent')
            }
            const onChildMouseOver = () => {
                if (activeCrosshairSource !== 'child') setActiveCrosshairSource('child')
                if (setActiveRangeSource) setActiveRangeSource('child')
            }
            parentChart.wrapper.addEventListener('mouseover', onParentMouseOver)
            childChart.wrapper.addEventListener('mouseover', onChildMouseOver)
            disposers.push(() => parentChart.wrapper.removeEventListener('mouseover', onParentMouseOver))
            disposers.push(() => childChart.wrapper.removeEventListener('mouseover', onChildMouseOver))
        }

        if (crosshairOnly) {
            const dispose = () => {
                for (const teardown of disposers) teardown()
                if (resizeTimer) clearTimeout(resizeTimer)
                syncRegistry.delete(syncKey)
            }
            syncRegistry.set(syncKey, dispose)
            return;
        }

        if (mode === 'active') {
            const dispose = () => {
                for (const teardown of disposers) teardown()
                if (resizeTimer) clearTimeout(resizeTimer)
                syncRegistry.delete(syncKey)
            }
            syncRegistry.set(syncKey, dispose)
            return;
        }

        const onParentRangeChange = () => syncChildToParentRange()
        const onChildRangeChange = () => syncChildToParentRange()

        parentTimeScale.subscribeVisibleLogicalRangeChange(onParentRangeChange)
        parentTimeScale.subscribeVisibleTimeRangeChange(onParentRangeChange)
        childTimeScale.subscribeVisibleLogicalRangeChange(onChildRangeChange)
        childTimeScale.subscribeVisibleTimeRangeChange(onChildRangeChange)
        disposers.push(() => {
            parentTimeScale.unsubscribeVisibleLogicalRangeChange(onParentRangeChange)
            parentTimeScale.unsubscribeVisibleTimeRangeChange(onParentRangeChange)
            childTimeScale.unsubscribeVisibleLogicalRangeChange(onChildRangeChange)
            childTimeScale.unsubscribeVisibleTimeRangeChange(onChildRangeChange)
        })

        const onResize = () => {
            if (resizeTimer) clearTimeout(resizeTimer)
            resizeTimer = setTimeout(() => {
                syncChildToParentRange()
                setTimeout(() => syncChildToParentRange(), 0)
                setTimeout(() => syncChildToParentRange(), 16)
            }, 0)
        }
        window.addEventListener('resize', onResize)
        disposers.push(() => {
            window.removeEventListener('resize', onResize)
            if (resizeTimer) clearTimeout(resizeTimer)
        })

        const rangesOutOfSync = () => {
            const parentLogical = parentTimeScale.getVisibleLogicalRange()
            const childLogical = childTimeScale.getVisibleLogicalRange()
            if (parentLogical && childLogical) {
                const eps = 1e-6
                return (
                    Math.abs(parentLogical.from - childLogical.from) > eps
                    || Math.abs(parentLogical.to - childLogical.to) > eps
                )
            }

            const parentRange = parentTimeScale.getVisibleRange()
            const childRange = childTimeScale.getVisibleRange()
            if (!parentRange || !childRange) return false
            return JSON.stringify(parentRange) !== JSON.stringify(childRange)
        }

        syncWatchdog = setInterval(() => {
            if (isApplyingRange) return
            if (rangesOutOfSync()) syncChildToParentRange()
        }, 120)
        disposers.push(() => {
            if (syncWatchdog) clearInterval(syncWatchdog)
            syncWatchdog = null
        })

        const dispose = () => {
            for (const teardown of disposers) teardown()
            if (resizeTimer) clearTimeout(resizeTimer)
            syncRegistry.delete(syncKey)
        }
        syncRegistry.set(syncKey, dispose)
    }

    public static makeSearchBox(chart: Handler) {
        const searchWindow = document.createElement('div')
        searchWindow.classList.add('searchbox');
        searchWindow.style.display = 'none';

        const magnifyingGlass = document.createElement('div');
        magnifyingGlass.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="24px" height="24px" viewBox="0 0 24 24" version="1.1"><path style="fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;stroke:lightgray;stroke-opacity:1;stroke-miterlimit:4;" d="M 15 15 L 21 21 M 10 17 C 6.132812 17 3 13.867188 3 10 C 3 6.132812 6.132812 3 10 3 C 13.867188 3 17 6.132812 17 10 C 17 13.867188 13.867188 17 10 17 Z M 10 17 "/></svg>`

        const sBox = document.createElement('input');
        sBox.type = 'text';

        searchWindow.appendChild(magnifyingGlass)
        searchWindow.appendChild(sBox)
        chart.div.appendChild(searchWindow);

        chart.commandFunctions.push((event: KeyboardEvent) => {
            if (window.handlerInFocus !== chart.id || window.textBoxFocused) return false
            if (searchWindow.style.display === 'none') {
                if (/^[a-zA-Z0-9]$/.test(event.key)) {
                    searchWindow.style.display = 'flex';
                    sBox.focus();
                    return true
                }
                else return false
            }
            else if (event.key === 'Enter' || event.key === 'Escape') {
                if (event.key === 'Enter') window.callbackFunction(`search${chart.id}_~_${sBox.value}`)
                searchWindow.style.display = 'none'
                sBox.value = ''
                return true
            }
            else return false
        })
        sBox.addEventListener('input', () => sBox.value = sBox.value.toUpperCase())
        return {
            window: searchWindow,
            box: sBox,
        }
    }

    public static makeSpinner(chart: Handler) {
        chart.spinner = document.createElement('div');
        chart.spinner.classList.add('spinner');
        chart.wrapper.appendChild(chart.spinner)

        // TODO below can be css (animate)
        let rotation = 0;
        const speed = 10;
        function animateSpinner() {
            if (!chart.spinner) return;
            rotation += speed
            chart.spinner.style.transform = `translate(-50%, -50%) rotate(${rotation}deg)`
            requestAnimationFrame(animateSpinner)
        }
        animateSpinner();
    }

    private static readonly _styleMap = {
        '--bg-color': 'backgroundColor',
        '--hover-bg-color': 'hoverBackgroundColor',
        '--click-bg-color': 'clickBackgroundColor',
        '--active-bg-color': 'activeBackgroundColor',
        '--muted-bg-color': 'mutedBackgroundColor',
        '--border-color': 'borderColor',
        '--color': 'color',
        '--active-color': 'activeColor',
    }
    public static setRootStyles(styles: any) {
        const rootStyle = document.documentElement.style;
        for (const [property, valueKey] of Object.entries(this._styleMap)) {
            rootStyle.setProperty(property, styles[valueKey]);
        }
    }
}
