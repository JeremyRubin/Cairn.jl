using Cairn

using Base.Test
import SimpleFileServer
using Logging
@Logging.configure(level=DEBUG)

# write your own tests here


workers = addprocs(10)
@info "Added 10 Procs"
mktempdir() do tmpd
    upload_name = "TEST_FILE"
    name, handle = mktemp(tmpd)
    test_data = Mmap.mmap(handle, Array{UInt8, 1}, 1000)
    rand!(test_data)
    Mmap.sync!(test_data)
    # Launch Consensus Nodes
    peers = [("localhost", x) for x = 8001:8003 ]
    @everywhere using Cairn
    @sync for (_, port ) = peers
        @spawnat pop!(workers) begin
            @async mktempdir() do f
                Cairn.Server.start(Cairn.Server.make(port, port - 8000, peers, f))
            end
        end
    end
    @info "Cairn has started"

    # Launch Some FS Nodes
    fs_nodes = [("localhost", x) for x = 8004:8010]
    @sync for (_, port) = fs_nodes
        @spawnat pop!(workers) let port = port
            cli = Cairn.Client.make("localhost", 8001)
            add_comm = Cairn.RPC.AddStorageNode("localhost", port)
            setup = () -> Cairn.Client.command(cli, add_comm)
            @async mktempdir() do f
                SimpleFileServer.start(port, f, setup)
            end
            
        end
    end
    @info "FS Nodes started"

    # Connect to the cluster
    c = Cairn.Client.make("localhost", 8001)
    while true
        try
            n = Cairn.Client.upload(c, upload_name, test_data, 3)
            @debug "$n Replicas have file"
            if n > 0
                break
            end
        catch err
            @debug err
        end
        wait(Timer(1))
    end
    tmp = ASCIIString(mktemp()[1])
    while true
        try
            d  = Cairn.Client.download(c, upload_name, tmp)
            @test d == test_data
            break
        catch err
            @debug err
        end
        wait(Timer(1))
    end



end
