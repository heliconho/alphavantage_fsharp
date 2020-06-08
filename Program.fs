// Learn more about F# at http://fsharp.org

open System
open Core
open Alpha
open FSharp.Data
open System.Linq
open System.Globalization


let _apiKey = @"AVE4SB8VMPZI3BZO"

let newCase : InputCase = {
    FTName = Functions.TIME_SERIES_INTRADAY.ToString();
    Symbol = "MSFT";
    Interval = "5min";
    ApiKey = _apiKey;
    }

let requestUrl = urlFormat newCase.FTName newCase.Symbol newCase.Interval newCase.ApiKey

let exec json2TimeSeries  = async {
    let url = requestUrl.ToString()
    let! jv = JsonValue.AsyncLoad(url) 
    let dt, series = extract jv
    return series |> Seq.map json2TimeSeries
}

[<EntryPoint>]
let main argv =
    let historyFeeds = loadRecords<FeedRecord> "feeds" 
                            |> Async.RunSynchronously
                            |> Seq.toList

    let lastestFeeds = exec json2TimeSeries 
                    |> Async.RunSynchronously 
                    |> Seq.sortBy(fun (x,_) -> x)
                    |> Seq.map( fun (x,y) -> {feedsdatetime = x.ToString(); opens = y.O; highs = y.H; lows = y.L; closes = y.C; volume = y.V; functionsid = 1})
                    |> Seq.toList

    let result =    //Check if feed record exist. if exist-> removeDups, else insert to db
                    if historyFeeds.Count() > 0 
                    then 
                        mergeList lastestFeeds historyFeeds
                        |> List.iter(fun x -> createRecord<FeedRecord> "feeds" x |> Async.RunSynchronously) 
                    else 
                        lastestFeeds
                        |> List.iter(fun x -> createRecord<FeedRecord> "feeds" x |> Async.RunSynchronously) 
               
    0 // return an integer exit code
