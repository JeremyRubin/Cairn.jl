
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


function CairnActionHandler(px::Rock.Server.t, d::SQLite.DB, v::Cairn.RPC.GetID)
    Rock.Server.self(px)
end
function CairnActionHandler(px::Rock.Server.t, d::SQLite.DB, v::Cairn.RPC.PrepareCache)

    SQLite.query(d,
                 """
                 insert or replace into results_cache (uuid, val, state) values  (?, ?, ?)
                 """, [v.id, Nullable(), Cairn.RPC.Loading])
    true
end
function CairnActionHandler(px::Rock.Server.t, d::SQLite.DB, v::Cairn.RPC.FetchCache)
    while true
        data = SQLite.query(d,
                            """
                            select val, state from results_cache where (uuid = ?) limit 1
                            """, [v.id]).data
        val = NullableArrays.dropnull(data[1])
        state = NullableArrays.dropnull(data[2])
        if length(val) == 1
            try
                if state[1] == Cairn.RPC.Ready
                    return Cairn.RPC.CacheResult(val[1], state[1])
                end
            finally
                if v.evict && state[1] == Cairn.RPC.Ready
                    SQLite.query(d,
                                 """
                                 delete from results_cache where (uuid = ?)
                                 """, [v.id])
                end
            end
        else
            Cairn.RPC.CacheResult(0, Gone)
        end
        wait(Timer(1))
    end
end
function CairnActionHandler(px::Rock.Server.t, d::SQLite.DB, v::Cairn.RPC.CacheResultOfAction)
    r = CairnActionHandler(px, d, v.c)
    if Rock.Server.self(px) == v.view_on
        vec = Vector(3)
        vec[1] = v.id
        vec[2] = r
        vec[3] = Cairn.RPC.Ready
        SQLite.query(d,
                     """
                     insert or replace into results_cache (uuid, val, state) values (?,?,?)
                     """, vec)
    end
    ()
end


dropnull2(data)= zip(data[1] |> NullableArrays.dropnull, data[2] |> NullableArrays.dropnull) |> collect

function CairnActionHandler(px::Rock.Server.t, d::SQLite.DB, arg::Cairn.RPC.CreateRequest)
    try
        q = SQLite.query(d,
                         """
                         select host, port from server_nodes order by RANDOM() limit ?
                         """, [arg.replication]).data |> dropnull2
        SQLite.query(d,
                     """
                     insert into objects values (?,?)
                     """, [arg.name, arg.replication])
        s = SQLite.Stmt(d,
                        """
                        insert into object_replicas values (?,?,?)
                        """)
        for (host, port) = q
            SQLite.bind!(s, [arg.name, host, port])
            SQLite.execute!(s)
        end
        if length(q) < arg.replication
            Cairn.RPC.CreateResponse(q,Nullable(ErrorException("Could not grant desired replication factor")))
        else
            @debug q
            Cairn.RPC.CreateResponse(q, Nullable())
        end
    catch err
        if isa(err, SQLite.SQLiteException)
            if err.msg == "UNIQUE constraint failed: objects.name"
                SQLite.query(d,
                             """
                             select host, port from object_replicas where object_name = ?
                             """, [arg.name]).data |> dropnull2
                return Cairn.RPC.CreateResponse([], Nullable(ErrorException("File $(arg.name) Already Exists")))
            else
                rethrow(err)
            end
        else
            rethrow(err)
        end

    end

end
function CairnActionHandler(px::Rock.Server.t, d::SQLite.DB, arg::Cairn.RPC.GetKeySpanRequest)
    for key = arg.key_span
        # Expand
        items = if isa(key, Cairn.RPC.PrefixKey)
            SQLite.query(d,
                                """
                                select object_name from chunks where object_name LIKE ? || '%'
                                """, [key.prefix]).data[1] |> NullableArrays.dropnull
            
        else
            [key]
        end

        # Get
        response = Cairn.RPC.GetKeySpanResponse([])
        for key = items
            data = SQLite.query(d,
                                """
                                select host, port, hash from chunks where object_name = ?
                                group by hash order by id ASC
                                """, [key]).data
            # TODO: ^^^ Verify
            # TODO: ^^^ Return random replica?
            
            replicas = data |> dropnull2
            hashes = data[3] |> NullableArrays.dropnull
            put!(response.files, Cairn.RPC.GetKeySpanSubResponse(key, hashes, replicas, Nullable()))

        end
        response
    end
    
    
end
function CairnActionHandler(px::Rock.Server.t, d::SQLite.DB, arg::Cairn.RPC.DeleteRequest)
    # Select all the hashes such that per host:port, there is one
    # object depending on that Chunk
    deleteable =  SQLite.query(d,
                               """
                               WITH HASHES AS (SELECT *  FROM chunks GROUP BY hash, host, port HAVING
                               count(hash) = 1)
                               SELECT hash, host, port from HASHES where object_name = ?
                               """, [arg.name]).data
    hashes = deleteable[3] |> NullableArrays.dropnull
    replicas = deleteable |> dropnull2
    # TODO: ^^^ DELETE
    SQLite.query(d,
                 """
                 delete from objects where name = ?
                 """, [arg.name])
    Cairn.RPC.DeleteRequest(Nullable())
end
function CairnActionHandler(px::Rock.Server.t, d::SQLite.DB, arg::Cairn.RPC.AppendChunksRequest)
    data = SQLite.query(d,
                        """
                        select name, host, port from objects where name = ? limit 1
                        """, [arg.name]).data
    name = (data[1] |> NullableArrays.dropnull |> collect)[1]
    host = (data[2] |> NullableArrays.dropnull |> collect)[1]
    port = (data[3] |> NullableArrays.dropnull |> collect)[1]
                   
    s = SQLite.Stmt(d,
                    """
                    insert into chunks values (?,?,?,?)
                    """)
    for hash = arg.hashes
        SQLite.bind!(s, [name, hash, host, port])
        SQLite.execute!(s)
    end
    Cairn.RPC.AppendChunksResponse(Nullable())
end
function CairnActionHandler(px::Rock.Server.t, d::SQLite.DB, arg::Cairn.RPC.ReplaceRequest)
    CairnActionHandler(px, d, Cairn.RPC.DeleteRequest(arg.old))
    SQLite.query(d,
                 """
                 update objects set name = ? where  where name = ?
                 """, [arg.new, arg.old])
    
    Cairn.RPC.ReplaceRequest(Nullable())
end
function CairnActionHandler(px::Rock.Server.t, d::SQLite.DB, arg::Cairn.RPC.RenameRequest)
    SQLite.query(d,
                 """
                 update objects set name = ? where name = ?
                 """, [arg.new, arg.old])
    Cairn.RPC.RenameResponse(Nullable())

end
function CairnActionHandler(px::Rock.Server.t, d::SQLite.DB, arg::Cairn.RPC.TruncateRequest)
    TruncateResponse(Nullable(ErrorException("Not Implemented")))
end

function CairnActionHandler(px::Rock.Server.t,d::SQLite.DB, arg::Cairn.RPC.ManyCommandRequest)
    Cairn.RPC.ManyCommandResponse([CairnActionHandler(px, d, arg) for arg=args])
end

function CairnActionHandler(px::Rock.Server.t,d::SQLite.DB, a::Cairn.RPC.AddStorageNode)
    SQLite.query(d, "insert or replace into server_nodes values (?, ?)", [a.host, a.port])
end




immutable t
    user_defined_function::Function
    rock::Rock.Server.t
end
function make(port,id, peers::Vector{Tuple{ASCIIString, Int64}}, path::ASCIIString)
    d = SQLite.DB("$path/sqlite.db")
    SQLite.query(d,
                 """
                 create table if not exists server_nodes (host TEXT, port Int,  unique (host, port))
                 """)

    SQLite.query(d,
                 """
                 create table if not exists objects
                 (name TEXT PRIMARY KEY, replication INTEGER)
                 """)
                 
    SQLite.query(d,
                 """
                 create table if not exists object_replicas
                 (object_name TEXT, host TEXT, port Int,
                 unique (object_name, host, port), 
                 FOREIGN KEY (object_name) REFERENCES objects(name) ON DELETE CASCADE ON UPDATE CASCADE,
                 FOREIGN KEY (host, port) REFERENCES server_nodes(host, port) ON DELETE CASCADE ON UPDATE CASCADE)
                 """)

    SQLite.query(d,
                 """ create table if not exists chunks
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, object_name TEXT, hash TEXT,
                 host TEXT, port TEXT,
                 FOREIGN KEY (object_name) REFERENCES objects(name) ON DELETE CASCADE ON UPDATE CASCADE,
                 FOREIGN KEY (host, port) REFERENCES server_nodes(host, port) ON DELETE CASCADE ON UPDATE CASCADE)
                 """)

    SQLite.query(d,
                 """
                 create table if not exists results_cache (uuid TEXT PRIMARY KEY, val BLOB, state INT)
                 """)
    ctx = Rock.Server.Context(port, peers, CairnActionHandler, "$path/sqlite.db")
    t(()->(), Rock.Server.t(ctx, id))
end
function start(cairn::t)
    Rock.Server.start(cairn.rock)
end
end

