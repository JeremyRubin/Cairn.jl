################################
##        RPC API And Details ##
################################
module RPC

using Rock: ExternalCriticalCommand, ExternalNonCriticalCommand
using SimpleFileServer: File
# Just for clearer reading
# TODO: Make Key check on construction for /
typealias Key ASCIIString
immutable PrefixKey
    prefix::Key
end
# A FileSpan is a set of spans of keys, which can be represented as a range (eg A-B) with a prefix, or a list of exact keys
typealias KeySpan Vector{Union{PrefixKey, Key}}

abstract Command <: ExternalCriticalCommand
abstract View <: ExternalNonCriticalCommand
typealias Slave Tuple{AbstractString, Int64}
typealias ReplicaSet Vector{Tuple{AbstractString, Int64}}
typealias Name Key
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
immutable GetKeySpanRequest <: Command
    key_span::KeySpan
end
#=
TODO:
immutable FindChunkRequest

end
=#
immutable GetKeySpanSubResponse
    key::Key
    hashes::Vector{Hash}
    replicas::Vector{Slave}
    err::Nullable{ErrorException}
end

immutable GetKeySpanResponse
    files::Vector{GetKeySpanResponse}
end

# Objects _May_ end with a DeleteObjectRequest (may also simply fail)
immutable DeleteRequest <: Command
    name::Name
end
immutable DeleteResponse
    err::Nullable{ErrorException}
end

immutable AppendChunksRequest <: Command
    name::Name
    hashes::Vector{Hash}
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
    args::Vector{Command}
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

