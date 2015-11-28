module Cairn
using SimpleFileServer.File
import Rock
import SQLite
using Logging
@Logging.configure(level=DEBUG)

abstract Command <: Rock.ExternalCriticalCommand
abstract View <: Rock.ExternalNonCriticalCommand

typealias Slave Tuple{ASCIIString, Int64}
typealias SlaveSet Array{Tuple{ASCIIString, Int64}, 1}



immutable AddStorageNode <: Command
    host::ASCIIString
    port::Int64
end
immutable FileZones <: Command
    name::ASCIIString
    all::Bool
end

immutable AddedFile <: Command
    f::File
    servers::SlaveSet
end
immutable DeleteFile <: Command
    name::ASCIIString
end
immutable DeadStorageNode
    host::ASCIIString
    port::Int64
end
immutable RandomReplicas <: View
    n::Int64
end
function fileAction(px, d::SQLite.DB, r::RandomReplicas)
    q = SQLite.query(d, "select host, port from file_nodes order by RANDOM() limit ?", [r.n]).data
    # Can't be null unless database corrupt
    [  (q[1][i].value, q[2][i].value) for i = 1:length(q[1])]
end
function fileAction(px,d::SQLite.DB, t::AddedFile)
    for (host, port) = t.servers
        SQLite.query(d, "insert or replace into file_locations values (?, ?, ?)", [t.f.name, host, port])
    end
end
function fileAction(px,d::SQLite.DB, t::DeleteFile)
    s = fileAction(px, d, FileZones(t.name, true))
    for (host, port) = t.servers
        SQLite.query(d, "delete from file_locations where (file_name = ?)", [t.f.name])
    end
    s
end
function fileAction(px,d::SQLite.DB, t::AddStorageNode)
    SQLite.query(d, "insert or replace into file_nodes values (?, ?)", [t.host, t.port])
end
function fileAction(px,d::SQLite.DB, t::FileZones)
    if t.all 
        data = SQLite.query(d, "select (host, port) from file_locations where (file_name = ?)", [t.name]).data
        zip(data[1] |> dropnull, data[2] |> dropnull) |> collect
    else
        data = SQLite.query(d, "select (host, port) from file_locations where (file_name = ?) order by RANDOM() limit 1", [t.name]).data
        zip(data[1] |> dropnull, data[2] |> dropnull) |> collect
    end
end
function fileAction(px,d::SQLite.DB, t::DeadStorageNode)
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
    ctx = Rock.Context(port, peers, fileAction, "$path/sqlite.db")
    Rock.Instance(ctx, id)
end


module Client
using Cairn
using Logging
@Logging.configure(level=DEBUG)
import Rock
import SimpleFileServer
import SHA
immutable t
    host
    port
end
make(host, port) = t(host, port)
function command(cli::t, c::Cairn.Command)
    Rock.command(Rock.client(cli.host, cli.port), c)
end
function put(cli::t, k,file_name)
    command(cli, Put( k, v))
end
function view(cli::t, c::Cairn.View)
    Rock.command(Rock.client(cli.host, cli.port), c)
end
function getReplicas(cli::t, n::Int64)
    view(cli, Cairn.RandomReplicas(n))
end
function delete(cli::t, name::ASCIIString)
    # First, un track the file, then delete it for real
    nodes  = Cairn.Client.command(cli, Cairn.DeleteFile(name))
    for (host, port) = nodes
        c = SimpleFileServer.Client.make(host, port)
        SimpleFileServer.Client.delete(c, name)
    end
end
function download(c::t, name::ASCIIString, to::ASCIIString)
    l= command(c, Cairn.FileZones(name, false))
    if length(l) < 1
        error("could not find file")
    end
    host, port = l[1]
    cli = SimpleFileServer.Client.make(host,port)
    SimpleFileServer.download(cli, name, to)
end
function upload(c::t, name::ASCIIString, m::Array{UInt8,1}, replication::Int64)
    file = SimpleFileServer.File(name, SHA.sha256(m), length(m))
    locations = Cairn.Client.getReplicas(c, replication)
    # if length(locations) != replication
    #     error("Could not get the requested number of servers")
    # end
    n = []
    @sync for (host, peer) = locations
        client = SimpleFileServer.Client.make(host, peer)
        @async begin
            try
                SimpleFileServer.Client.upload(client, file, test_data)
                push!(n, (host, peer))
            catch err
                @debug err
                # One Retry
                try
                    wait(Timer(3))
                    SimpleFileServer.Client.upload(client, file, test_data)
                    push!(n, (host, peer))
                catch err
                    @debug err
                end
                
            end
        end
    end
    Cairn.Client.command(c, Cairn.AddedFile(file, n))
    length(n)
end

end
end
