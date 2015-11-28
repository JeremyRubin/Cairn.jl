using Cairn

using Base.Test
import SimpleFileServer

# write your own tests here


test_data = Mmap.mmap("TESTDATA", Array{UInt8, 1})
# Launch Consensus Nodes
peers = [("localhost", x) for x = 8001:8003 ]
for (_, port ) = peers
    @async mktempdir() do f
        Rock.start(Cairn.mk(port, port - 8000, peers, f))
    end
end

# Launch Some FS Nodes
fs_nodes = [("localhost", x) for x = 8004:8010]
l  = ReentrantLock()
for (_, port) = fs_nodes
    setup = () -> Cairn.Client.command(Cairn.Client.make("localhost", 8001), Cairn.AddStorageNode("localhost", port))
    @async mktempdir() do f
        SimpleFileServer.start(port, f, setup)
    end
end

# Connect to the cluster
c = Cairn.Client.make("localhost", 8001)
n = 0
wait(Timer(10))
# while true
    # try
Cairn.Client.upload(c, "TESTDATA", test_data, 3)
        # break
    # catch
        # wait(Timer(3))
    # end
# end

tmp = ASCIIString(mktemp()[1])
@show tmp
@test Cairn.Client.download(c, "TESTDATA", tmp) == test_data

wait(Timer(10))
