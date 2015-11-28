using Cairn
using Base.Test
import FileServerNode

# write your own tests here

# Launch Consensus Nodes
peers = [("localhost", x) for x = 8001:8003 ]
for (_, port ) = peers
    @async mktempdir() do f
        Rock.start(Cairn.mk(port, port - 8000, peers, f))
    end
end

# Launch Some FS Nodes
fs_nodes = [("localhost", x) for x = 8004:8010]
for (_, port) = fs_nodes
    setup = () -> Cairn.Client.command(Cairn.Client.make("localhost", 8001), Cairn.AddStorageNode("localhost", port))
    @async mktempdir() do f
        FileServerNode.start(port, f, setup)
    end
end

# Connect to the cluster
Cairn.Client.make("localhost", 8000)
# command(c,

wait(Timer(30))
