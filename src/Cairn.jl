module Cairn

################################
##        RPC API And Details ##
################################
module RPC
import Rock
using SimpleFileServer.File
abstract Command <: Rock.Server.ExternalCriticalCommand
abstract View <: Rock.Server.ExternalNonCriticalCommand

typealias Slave Tuple{ASCIIString, Int64}
typealias SlaveSet Array{Tuple{ASCIIString, Int64}, 1}



immutable AddStorageNode <: Command
    host::ASCIIString
    port::Int64
end
immutable FileRequest <: Command
    name::ASCIIString
    all::Bool
end
@enum FileStatus NotFound Ok
immutable FileLocation
    servers::SlaveSet
    error::FileStatus
end
immutable AddedFile <: Command
    f::File
    servers::SlaveSet
end
immutable DeleteFile <: Command
    name::ASCIIString
end
@enum CacheState Loading Ready Gone
immutable CacheResultOfAction <: Command
    view_on::Int64
    id::ASCIIString
    c::Command
    CacheResultOfAction(view_on::Int64, c::Command) = new(view_on, string(Base.Random.uuid4()), c)
end
immutable GetID <: View
end
immutable PrepareCache <: View
    id::ASCIIString
    PrepareCache(c::CacheResultOfAction) = new(c.id)
    PrepareCache(id::ASCIIString) = new(id)
end
immutable FetchCache <: View
    id::ASCIIString
    evict::Bool
    FetchCache(c::CacheResultOfAction, b::Bool) = new(c.id, b)
    FetchCache(id::ASCIIString, b::Bool) = new(id, b)
end
immutable CacheResult
    val
    state::CacheState
end
immutable DeadStorageNode
    host::ASCIIString
    port::Int64
end
immutable RandomReplicas <: View
    n::Int64
end
end

##############################################
##      Cairn Server Handlers and Interface ##
##############################################
module Server
import Cairn
import Rock
import SQLite
import NullableArrays
using Logging
@Logging.configure(level=DEBUG)


function CairnActionHandler(px::Rock.Server.Paxos, d::SQLite.DB, v::Cairn.RPC.GetID)
    Rock.Server.self(px)
end
function CairnActionHandler(px::Rock.Server.Paxos, d::SQLite.DB, v::Cairn.RPC.PrepareCache)
    SQLite.query(d, "insert or replace into results_cache (uuid, val, state) values  (?, ?, ?)", [v.id, Nullable(), Loading])
    true
end
function CairnActionHandler(px::Rock.Server.Paxos, d::SQLite.DB, v::Cairn.RPC.FetchCache)
    while true
        data = SQLite.query(d, "select val, state from results_cache where (uuid = ?) limit 1", [v.id]).data
        val = NullableArrays.dropnull(data[1])
        state = NullableArrays.dropnull(data[2])
        if length(val) == 1
            try
                if state[1] == Ready
                    return Cairn.RPC.CacheResult(val[1], state[1])
                end
            finally
                if v.evict && state[1] == Ready
                    SQLite.query(d, "delete from results_cache where (uuid = ?)", [v.id])
                end
            end
        else
            Cairn.RPC.CacheResult(0, Gone)
        end
        wait(Timer(1))
    end
end
function CairnActionHandler(px::Rock.Server.Paxos, d::SQLite.DB, v::Cairn.RPC.CacheResultOfAction)
    r = CairnActionHandler(px, d, v.c)
    if Rock.Server.self(px) == v.view_on
        vec = Vector(3)
        vec[1] = v.id
        vec[2] = r
        vec[3] = Ready
        SQLite.query(d, "insert or replace into results_cache (uuid, val, state) values (?,?,?)", vec)
    end
    ()
end
function CairnActionHandler(px::Rock.Server.Paxos, d::SQLite.DB, r::Cairn.RPC.RandomReplicas)
    q = SQLite.query(d, "select host, port from file_nodes order by RANDOM() limit ?", [r.n]).data
    # Can't be null unless database corrupt
    [  (q[1][i].value, q[2][i].value) for i = 1:length(q[1])]
end
function CairnActionHandler(px::Rock.Server.Paxos,d::SQLite.DB, t::Cairn.RPC.AddedFile)
    for (host, port) = t.servers
        SQLite.query(d, "insert or replace into file_locations values (?, ?, ?)", [t.f.name, host, port])
    end
end
function CairnActionHandler(px::Rock.Server.Paxos,d::SQLite.DB, t::Cairn.RPC.DeleteFile)
    s = CairnActionHandler(px, d, FileRequest(t.name, true))
    for (host, port) = t.servers
        SQLite.query(d, "delete from file_locations where file_name = ?", [t.name])
    end
    s
end
function CairnActionHandler(px::Rock.Server.Paxos,d::SQLite.DB, a::Cairn.RPC.AddStorageNode)
    SQLite.query(d, "insert or replace into file_nodes values (?, ?)", [a.host, a.port])
end
function CairnActionHandler(px::Rock.Server.Paxos,d::SQLite.DB, t::Cairn.RPC.FileRequest)
    @debug "In FileRequest"
    r = if t.all 
        data = SQLite.query(d, "select host, port from file_locations where file_name = ?", [t.name]).data
        zip(data[1] |> NullableArrays.dropnull, data[2] |> NullableArrays.dropnull) |> collect
    else
        @debug "1 Zone Requested"
        data = SQLite.query(d, "select host, port from file_locations where file_name = ? order by RANDOM() limit 1", [t.name]).data
        zip(data[1] |> NullableArrays.dropnull, data[2] |> NullableArrays.dropnull) |> collect
    end
    if length(r) < 1
        Cairn.RPC.FileLocation([], NotFound)
    else
        Cairn.RPC.FileLocation(r, Ok)
    end
end
function CairnActionHandler(px::Rock.Server.Paxos,d::SQLite.DB, t::Cairn.RPC.DeadStorageNode)
    data = SQLite.query(d, "select file_name from file_locations where (host = ?, port=?)", [t.host, t.port]).data
    SQLite.query(d, "delete from file_locations where (host = ?, port=?)", [t.host, t.port])

    for entry = data[1]
        if !isnull(entry)
            entry.value #TODO: Need to force servers to request this
        end
    end
end
immutable t
    rock::Rock.Server.t
end
function make(port,id, peers::Array{Tuple{ASCIIString, Int64},1}, path::ASCIIString)
    d = SQLite.DB("$path/sqlite.db")
    SQLite.query(d, "create table if not exists file_nodes (host TEXT, port Int,  unique (host, port))")
    SQLite.query(d, "create table if not exists file_locations (file_name TEXT, host TEXT, port Int, unique (file_name, host, port))")
    SQLite.query(d, "create table if not exists results_cache (uuid TEXT PRIMARY KEY, val BLOB, state INT)")
    ctx = Rock.Server.Context(port, peers, CairnActionHandler, "$path/sqlite.db")
    t(Rock.Server.t(ctx, id))
end
function start(cairn::t)
    Rock.Server.start(cairn.rock)
end
end

###################################
##    Cairn Client Functionality ##
###################################
module Client
import Cairn
using Logging
@Logging.configure(level=DEBUG)
import Rock
import SimpleFileServer
import SHA
immutable t
    host::ASCIIString
    port::Int64
end
make(host, port) = t(host, port)
function command(c::t, comm::Cairn.RPC.Command)
    r = Rock.Client.make(c.host, c.port)
    Rock.Client.command(r, comm)
end
function view(c::Cairn.Client.t, v::Cairn.RPC.View)
    r = Rock.Client.make(c.host, c.port)
    Rock.Client.command(r, v)
end
function getReplicas(cli::Cairn.Client.t, n::Int64)
    view(cli, Cairn.RPC.RandomReplicas(n))
end
function delete(cli::Cairn.Client.t, name::ASCIIString)
    # First, un track the file, then delete it for real
    nodes  = Cairn.Client.command(cli, Cairn.RPC.DeleteFile(name))
    for (host, port) = nodes
        c = SimpleFileServer.Client.make(host, port)
        SimpleFileServer.Client.delete(c, name)
    end
end
function download(c::Cairn.Client.t, name::ASCIIString, to::ASCIIString)
    s_id = view(c, Cairn.RPC.GetID())
    arg  = Cairn.RPC.CacheResultOfAction(s_id, Cairn.RPC.FileRequest(name, false))
    view(c, Cairn.RPC.PrepareCache(arg))
    l = command(c, arg)
    fetch_cache = Cairn.RPC.FetchCache(arg, false)
    cache_result = view(c, fetch_cache)
    @error cache_result
    if cache_result.state == Cairn.RPC.Ready && cache_result.val.error == Cairn.RPC.Ok && length(cache_result.val.servers) > 0
        host, port = cache_result.val.servers[1]
        cli = SimpleFileServer.Client.make(host,port)
        SimpleFileServer.Client.download(cli, name, to)
    else
        error("File Not Found")
    end
end
function upload(c::Cairn.Client.t, name::ASCIIString, m::Array{UInt8,1}, replication::Int64)
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
                SimpleFileServer.Client.upload(client, file, m)
                push!(n, (host, peer))
            catch err
                @error err
                # One Retry
                try
                    wait(Timer(3))
                    SimpleFileServer.Client.upload(client, file, m)
                    push!(n, (host, peer))
                catch err
                    @error err
                end
                
            end
        end
    end
    if length(n) > 0
        Cairn.Client.command(c, Cairn.RPC.AddedFile(file, n))
    end

    length(n)
end

end
end
