module Alpha

open Core
open FSharp.Data
open System.Globalization
open System
open Deedle

type OHLCV =
    { O: float
      H: float
      L: float
      C: float
      V: int }

type TimeSeriesData = DateTime * OHLCV
type MetaData = DateTime * string

type Functions =
    | TIME_SERIES_INTRADAY
    | TIME_SERIES_DAILY
    | TIME_SERIES_WEEKLY
    | TIME_SERIES_MONTHLY

type InputCase =
    { FTName: string
      Symbol: string
      Interval: string
      ApiKey: string }

let json2TimeSeries (dt, jvq): TimeSeriesData =
    match jvq with
    | JsonValue.Record [| _, sO; _, sH; _, sL; _, sC; _, sV |] ->
        (DateTime.Parse(dt),
         { O = sO.AsFloat()
           H = sH.AsFloat()
           L = sL.AsFloat()
           C = sC.AsFloat()
           V = sV.AsInteger() })
    | _ -> failwith " Unrecognized JSON format"

let tupleValue (timeSeriesData: TimeSeriesData) =
    let dt = fst (timeSeriesData)
    let feed = snd (timeSeriesData)

    let dbRow =
        { feedsdatetime = (dt.ToString "yyyy/MM/dd hh:mm:ss")
          opens = feed.O
          highs = feed.H
          lows = feed.L
          closes = feed.C
          volume = feed.V
          functionsid = 1 }

    createRecord<FeedRecord> "feeds" dbRow
    |> Async.RunSynchronously

let extract jv =
    match jv with
    | JsonValue.Record [| _metaTitle, JsonValue.Record [| _; _; _, serverTime; _; _ |]; _seriesTitle, series |]
    | JsonValue.Record [| _metaTitle, JsonValue.Record [| _; _; _, serverTime; _; _; _ |]; _seriesTitle, series |]
    | JsonValue.Record [| _metaTitle, JsonValue.Record [| _; _; _, serverTime; _; _; _; _ |]; _seriesTitle, series |]
    | JsonValue.Record [| _metaTitle, JsonValue.Record [| _; _; _; _, serverTime; _ |]; _seriesTitle, series |] 
        when fst (DateTime.TryParse(serverTime.AsString())) ->
        serverTime.AsDateTime(),
        match series with
        | JsonValue.Record quotes -> quotes
        | _ -> failwith " Unrecognized JSON format"
    | _ -> failwith " Unrecognized JSON format"

let urlFormat _functionTypeName _symbol _interval _apiKey =
    String.Format
        ("https://www.alphavantage.co/query?function={0}&symbol={1}&interval={2}&apikey={3}",
         _functionTypeName,
         _symbol,
         _interval,
         _apiKey)
