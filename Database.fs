module Core

open Npgsql
open Dapper
open Npgsql.FSharp
open System
open System.Collections.Generic

type FunctionType = { Id: int; FunctionType: string }
type FeedRecord =
    { feedsdatetime: string
      opens: float
      highs: float
      lows: float
      closes: float
      volume: int
      functionsid: int }

let connectionString =
    Sql.host "localhost"
    |> Sql.port 5432
    |> Sql.username "postgres"
    |> Sql.password "Pass@word1"
    |> Sql.database "AlphaVantage"
    |> Sql.sslMode SslMode.Disable
    |> Sql.config "Pooling=true" // optional Config for connection string
    |> Sql.formatConnectionString


type QuerySingleRecord<'a> = string -> IReadOnlyDictionary<string, obj> -> Async<'a option>
type QueryManyRecords<'a> = string ->  Async<'a IEnumerable>

type CreateRecord<'a> = string -> 'a -> Async<unit>
type LoadRecord<'a> = string -> string -> obj -> Async<'a option>
type LoadRecords<'a> = string ->  Async<'a IEnumerable>
type UpdateRecord<'a> = string -> string -> 'a -> Async<unit>
type DeleteRecord = string -> string -> obj -> Async<unit>
// a list of properties of type 'a
let propertyNames<'a> =
    typedefof<'a>.GetProperties()
    |> Array.map (fun p -> p.Name)
    |> Array.sort

// creates a dictionary of (property name, property value)-pairs that can be passed to Dapper
let getRecordParams a =
    a.GetType().GetProperties()
    |> Array.map (fun p -> p.Name, p.GetValue(a))
    |> dict

let querySingleRecord<'a> : QuerySingleRecord<'a> =
    fun command param ->
        async {
            use conn = new NpgsqlConnection(connectionString)

            let! result =
                conn.QuerySingleOrDefaultAsync<'a>(command, param)
                |> Async.AwaitTask
            if isNull (box result) then return None else return Some result
        }

let queryManyRecords<'a> : QueryManyRecords<'a> =
    fun command ->
        async {
            use conn = new NpgsqlConnection(connectionString)

            return! conn.QueryAsync<'a>(command)
                    |> Async.AwaitTask
        }

// convert a string of pascal case into snake case
let pascalToSnake pascalCaseName =
    pascalCaseName
    // any upper case character gets replaced by an underscore plus the lower case character
    |> Seq.mapi (fun i c ->
        if Char.IsUpper(c)
        then if i > 0 then [ '_'; Char.ToLower(c) ] else [ Char.ToLower(c) ]
        else [ c ])
    |> Seq.concat
    |> Seq.toArray
    |> String


// read property names and convert them into snake case
let columnNames<'a> =
    propertyNames<'a>
    |> Seq.map pascalToSnake
    |> Seq.sort
    |> Seq.toArray

// SQL command builders
let getInsertCommand<'a> tableName =
    let columns = String.Join(", ", columnNames<'a>)

    let placeholders =
        String.Join(", ", propertyNames<'a> |> Array.map (fun c -> "@" + c))

    sprintf "INSERT INTO %s (%s) VALUES (%s)" tableName columns placeholders

let getSelectCommand tableName keyPropertyName =
    let keyColumnName = pascalToSnake keyPropertyName
    sprintf "SELECT * FROM %s WHERE %s = @%s" tableName keyColumnName keyPropertyName

let getAllCommand tableName = 
    sprintf "SELECT * FROM %s" tableName 

let getUpdateCommand<'a> tableName keyPropertyName =
    let assignments =
        (columnNames<'a>, propertyNames<'a>)
        ||> Array.map2 (fun c p -> sprintf "%s = @%s" c p)

    let assignmentsString = String.Join(", ", assignments)
    let keyColumnName = pascalToSnake keyPropertyName
    sprintf "UPDATE %s SET %s WHERE %s = @%s" tableName assignmentsString keyColumnName keyPropertyName

let getDeleteCommand tableName keyPropertyName =
    let keyColumnName = pascalToSnake keyPropertyName
    sprintf "DELETE FROM %s WHERE %s = @%s" tableName keyColumnName keyPropertyName

// actual persistence functions
let createRecord<'a> : CreateRecord<'a> =
    fun tableName a ->
        async {
            use conn = new NpgsqlConnection(connectionString)
            let command = getInsertCommand<'a> tableName
            let paras = getRecordParams a
            let! count = conn.ExecuteAsync(command, paras)
                        |> Async.AwaitTask
            if count = 0 then failwith "error creating record"
        }

let loadRecord<'a> : LoadRecord<'a> =
    fun tableName keyPropertyName id ->
        async {
            use conn = new NpgsqlConnection(connectionString)

            let command =
                getSelectCommand tableName keyPropertyName

            let param = readOnlyDict [ keyPropertyName, id ]

            return! querySingleRecord<'a> command param
        }

let loadRecords<'a> : LoadRecords<'a> =
    fun tableName ->
        async {
            use conn = new NpgsqlConnection(connectionString)

            let command = getAllCommand tableName
            return! queryManyRecords<'a> command
        }

let updateRecord<'a> : UpdateRecord<'a> =
    fun tableName keyPropertyName a ->
        async {
            use conn = new NpgsqlConnection(connectionString)

            let command =
                getUpdateCommand<'a> tableName keyPropertyName

            let param = getRecordParams a

            let! count =
                conn.ExecuteAsync(command, param)
                |> Async.AwaitTask

            if count = 0 then failwith "error updating record"
        }

let deleteRecord: DeleteRecord =
    fun tableName keyPropertyName id ->
        async {
            use conn = new NpgsqlConnection(connectionString)

            let command =
                getDeleteCommand tableName keyPropertyName

            let param = dict [ keyPropertyName, box id ]

            let! count =
                conn.ExecuteAsync(command, param)
                |> Async.AwaitTask
            if count = 0 then failwith "error deleting record"
        }


let removeDups is =
    let d = System.Collections.Generic.Dictionary()
    [ for i in is do match d.TryGetValue i with
                     | (false,_) -> d.[i] <- (); yield i
                     | _ -> () ]

let mergeList lista listb =
    lista @ listb
    |> removeDups
        