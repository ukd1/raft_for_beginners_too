require 'shellwords'

puts "cargo build --quiet --example simple"
log_level = ENV.fetch('RUST_LOG', "info")
number_of_peers = Integer(ENV.fetch("RAFT_TEST_PEERS", "3"))
listen_addr = "127.0.0.1"

peers = (0..number_of_peers-1).map {|i| 8000 + i}

puts "tmux new-session -s raft_for_beginners_too -d"
puts "tmux set -g remain-on-exit on"
puts "tmux set -g history-limit 100000"

(0..(peers.size - 1)).each do
    this_node = "#{listen_addr}:#{peers[0]}".shellescape
    peers_for_this_node = peers[1..-1]
    peer_args = peers_for_this_node.map {|peer_port| "--peer #{listen_addr.shellescape}:#{peer_port.to_s.shellescape}" }.join(" ")
    command = "target/debug/examples/simple -l #{this_node} " + peer_args
    shell_loop = "sh -c 'while :; do trap \"kill -INT %1\" INT; #{command}; printf \"\\nProcess exited.\\nPress any key to restart...\\n\"; trap - INT; read; done'"
    log_env = "RUST_LOG=#{log_level}".shellescape
    tmux_session = "raft_for_beginners_too:$".shellescape

        puts "tmux new-window -a -t #{tmux_session} -n #{this_node} -e #{log_env} #{shell_loop}"
    peers.rotate!
end

#puts "trap 'pkill simple; trap - INT' INT"
#puts "tmux attach -t raft_for_beginners_too"
#puts "wait"
