################################
##        RPC API And Details ##
################################
module RPC

import Rock
using SimpleFileServer.File

abstract Command <: Rock.Server.ExternalCriticalCommand
abstract View <: Rock.Server.ExternalNonCriticalCommand
typealias Slave Tuple{AbstractString, Int64}
typealias ReplicaSet Array{Tuple{AbstractString, Int64}, 1}
typealias Name AbstractString
typealias Hash AbstractString

immutable AddStorageNode <: Command
    host::ASCIIString
    port::Int64
end
# All Objects begin with a CreateObjectRequest
immutable CreateRequest <: Command
    name::Name
    replication::Int64
end
immutable CreateResponse
    replicas::ReplicaSet
    err::Nullable{ErrorException}
end
immutable GetChunksRequest <: Command
    name::Name
end
immutable GetChunksResponse
    replicas::ReplicaSet
    hashes::Array{Hash,1}
    err::Nullable{ErrorException}
end

# Objects _May_ end with a DeleteObjectRequest (may also simply fail)
immutable DeleteRequest <: Command
    name::Name
end
immutable DeleteResponse
    err::Nullable{ErrorException}
end

immutable AppendChunksRequest <: Command
    hashes::Array{Hash,1}
end
immutable AppendChunksResponse
    err::Nullable{ErrorException}
end
immutable ReplaceRequest <: Command
    old::Name
    new::Name
end
immutable ReplaceResponse
    err::Nullable{ErrorException}
end
immutable RenameRequest <: Command
    old::Name
    new::Name
end
immutable RenameResponse
    err::Nullable{ErrorException}
end
immutable TruncateRequest <: Command
    name::Name
end
immutable TruncateResponse
    err::Nullable{ErrorException}
end

immutable ManyCommandRequest <: Command
    args::Array{Command, 1}
end
immutable ManyCommandResponse
    errs
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
end

