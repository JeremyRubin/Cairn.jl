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
    id::Int64
    host::ASCIIString
    port::Int64
    t(host::ASCIIString, port::Int64) = new(view(host, port, Cairn.RPC.GetID()), host, port)
end
make(host, port) = t(host, port)
function command(c::t, comm::Cairn.RPC.Command)
    r = Rock.Client.make(c.host, c.port)
    Rock.Client.command(r, comm)
end
function view(host, port, v::Cairn.RPC.View)
    r = Rock.Client.make(host, port)
    Rock.Client.command(r, v)
end
function view(c::Cairn.Client.t, v::Cairn.RPC.View)
    r = Rock.Client.make(c.host, c.port)
    Rock.Client.command(r, v)
end
function delete(cli::Cairn.Client.t, name::ASCIIString)
    # First, un track the file, then delete it for real
    resp = Cairn.Client.command(cli, Cairn.RPC.DeleteRequest(name))::DeleteResponse
    if !isnull(resp.err)
        throw(resp.err)
    end
    # for (host, port) = nodes
    #     c = SimpleFileServer.Client.make(host, port)
    #     SimpleFileServer.Client.delete(c, name)
    # end
end
function command_result(c::t, command::Cairn.RPC.Command)
    arg  = Cairn.RPC.CacheResultOfAction(c.id,command)
    @error arg
    Cairn.Client.view(c, Cairn.RPC.PrepareCache(arg))
    l = Cairn.Client.command(c, arg)
    @error l
    fetch_cache = Cairn.RPC.FetchCache(arg, true)
    while true
        cache_result = view(c, fetch_cache)
        if cache_result.state == Cairn.RPC.Ready
            return cache_result.val
        elseif cache_result.state == Cairn.RPC.Gone
            error("Nothing in Cache")
        elseif cache_result.state == Cairn.RPC.Loading
            wait(Timer(1))
        end
    end
end
function download(c::Cairn.Client.t, key_span::Cairn.RPC.KeySpan,
                  to_dir::ASCIIString, rename::Vector{ASCIIString} = [],
                  stream::Nullable{Channel{ASCIIString}} = Nullable())
    files = command_result(c, Cairn.RPC.GetKeySpanRequest(key_span)).files
    @sync for r::Cairn.RPC.GetKeySpanSubResponse = files
        if isnull(r.err)
            @async for (hash, sfs_client) = zip(r.hashes, map(x -> SimpleFileServer.Client.make(x...), r.replicas))
                try
                    # Download the chunk, appending it to the file
                    name = joinpath(to_dir, length(rename) > 0 ? pop!(rename): r.key )
                    SimpleFileServer.Client.download(sfs_client, hash, name, true)
                    if !isnull(stream)
                        put!(stream,(r.key, name))
                    end

                catch err
                    @debug err
                    # TODO Better story here?
                    error("Could not locate file")
                end
            end

        else
            throw(r.err.value)
        end
    end
end
function upload(c::Cairn.Client.t, name::ASCIIString, m::Vector{UInt8}, replication::Int64)
    r = command_result(c, Cairn.RPC.CreateRequest(name, replication))::Cairn.RPC.CreateResponse
    if isnull(r.err)

        name = SHA.sha256(m)
        file = SimpleFileServer.File(name,name, length(m))
        n = RemoteRef()
        put!(n, 0)
        @sync for client = map(a ->SimpleFileServer.Client.make(a...), r.replicas)
            @async begin
                try
                    SimpleFileServer.Client.upload(client, file, m)
                    
                    put!(n, take!(n)+1)
                catch err
                    @error err
                    # One Retry
                    try
                        wait(Timer(3))
                        SimpleFileServer.Client.upload(client, file, m)
                        put!(n, take!(n)+1)
                    catch err
                        @error err
                    end

                end
            end
        end
        v = take!(n)
        if v != replication
            @warn "Could not get the requested number of servers, got $n"
        end
        v
    else
        throw(r.err.value)
    end
end
function find_connection(cs::Vector{Cairn.Client.t})
    Base.Random.shuffle!(cs)
    cs[1]
    # TODO; Ping, find slave, etc
end

end
