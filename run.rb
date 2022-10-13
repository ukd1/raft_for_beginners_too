puts "cargo build --quiet"
log_level = ENV.fetch('RUST_LOG', "info")

number_of_peers = 15
peers = (0..number_of_peers-1).map {|i| 8000 + i}

puts "tmux new-session -s raft_for_beginners_too -d"
puts "tmux set -g remain-on-exit on"

(0..(peers.size - 1)).each do
    this_node = peers[0]
    peers_for_this_node = peers[1..-1]
    puts "tmux new-window -a -t raft_for_beginners_too:\\$ -n 127.0.0.1:#{this_node} -e 'RUST_LOG=#{log_level}' target/debug/raft_for_beginners_too -l 127.0.0.1:#{this_node} " + peers_for_this_node.map {|peer_port| "--peer 127.0.0.1:#{peer_port}" }.join(" ")
    peers.rotate!
end

puts "trap 'pkill raft_for_beginners_too; trap - INT' INT"
#puts "tmux attach -t raft_for_beginners_too"
#puts "wait"
