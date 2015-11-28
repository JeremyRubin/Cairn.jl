module Cairn
using FileServerNode
import Rock
import SQLite
@enum Op Put Append
abstract Command <: Rock.ExternalCriticalCommand
abstract View <: Rock.ExternalNonCriticalCommand

typealias Slaves Tuple{ASCIIString, Int64}
# function upload_targets(c::CairnInstance, size::Int64, hash::ASCIIString)
#     Base.Random.shuffe(c.slaves)[:c.replication]
# end


type AddStorageNode <: Command
    host::ASCIIString
    port::Int64
end
type FileZone <: Command
    name::ASCIIString
end
type AddedFile <: Command
    f::File
    servers::Array{Tuple{ASCIIString, Int64},1}
end
type DeleteFile <: Command
    f::File
    servers::Array{Tuple{ASCIIString, Int64},1}
end
type DeadStorageNode
    host::ASCIIString
    port::Int64
end
function get_random_file_nodes(d::SQLite.DB, size::Int64, n::Int64)
    SQLite.query(d, "select (host, port) from file_nodes order RANDOM() limit ?", [size, n])
end
function udf(px,d::SQLite.DB, t::AddedFile)
    for (host, port) = t.servers
        SQLite.query(d, "insert or replace into file_locations values (?, ?, ?)", [t.f.name, host, port])
    end
end
function udf(px,d::SQLite.DB, t::DeleteFile)
    for (host, port) = t.servers
        SQLite.query(d, "delete from file_locations where (file_name = ?,host = ?, port = ?)", [t.f.name, host, port])
    end
end
function udf(px,d::SQLite.DB, t::AddStorageNode)
    SQLite.query(d, "insert or replace into file_nodes values (?, ?)", [t.host, t.port])
end
function udf(px,d::SQLite.DB, t::FileZone)
    data = SQLite.query(d, "select (host, port) from file_locations where (file_name = ?) order by RANDOM() limit 1", [t.name]).data
    if length(data[1]) == 1
        (data[1].value, data[2].value)
    end
end
function udf(px,d::SQLite.DB, t::DeadStorageNode)
    data = SQLite.query(d, "select (file_name) from file_locations where (host = ?, port=?)", [t.host, t.port]).data
    SQLite.query(d, "delete from file_locations where (host = ?, port=?)", [t.host, t.port])

    for entry = data[1]
        if !isnull(entry)
            entry.value #TODO: Need to force servers to request this
        end
    end
end
function mk(port,id, peers::Array{Tuple{ASCIIString, Int64},1}, path::ASCIIString)
    d = SQLite.DB("$path/sqlite.db")
    SQLite.query(d, "create table if not exists file_nodes (host TEXT, port Int,  unique (host, port))")
    SQLite.query(d, "create table if not exists file_locations (file_name TEXT, host TEXT, port Int, unique (file_name, host, port))")
    ctx = Rock.Context(port, peers, udf, "$path/sqlite.db")
    Rock.Instance(ctx, id)
end


module Client
using Cairn.Command
import Rock
immutable t
    host
    port
end
make(host, port) = t(host, port)
function command(cli::t, c::Command)
    Rock.command(Rock.client(cli.host, cli.port), c)
end
function put(cli::t, k,file_name)
    Rock.command(cli.host,cli.port, Put( k, v))
end
function getReplicas(cli::t, n::Int64)

    end
end
end
