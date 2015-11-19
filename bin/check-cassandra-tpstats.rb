#! /usr/bin/env ruby
#
#   check-cassandra-tpstats 
#
# DESCRIPTION:
#   This plugin uses Apache Cassandra's `nodetool` to capture thread pool
#   statistics from an instance of Cassandra and trigger alerts for specified stage. 
#
# OUTPUT:
#   json stage statistics
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#   Cassandra's nodetool
#
# USAGE:
#     $ ./check-cassandra-tpstats.rb --threadpool CommitLogArchiver -w 10 -W 0 -c 15 -C 5
#     CheckTPStats CRITICAL: {"thread":{"stage":"CommitLogArchiver","active":"0","pending":"25","completed":"0","blocked":"0"}}
#
# NOTES:
#
# LICENSE:
#   Copyright 2015 Timothy Given https://github.com/tagiven
#   Released under the same terms as Sensu (the MIT license); see LICENSE
#   for details.
#

require 'sensu-plugin/check/cli'
require 'socket'
require 'json'

UNITS_FACTOR = {
  'bytes' => 1,
  'KB' => 1024,
  'MB' => 1024**2,
  'GB' => 1024**3,
  'TB' => 1024**4
}

THREADPOOLS = [
  'AntiEntropyStage',
  'CacheCleanupExecutor',
  'CommitLogArchiver',
  'CompactionExecutor',
  'CounterMutationStage',
  'GossipStage',
  'HintedHandoff',
  'InternalResponseStage',
  'MemtableFlushWriter',
  'MemtablePostFlush',
  'MemtableReclaimMemory',
  'MigrationStage',
  'MiscStage',
  'MutationStage',
  'PendingRangeCalculator',
  'ReadRepairStage',
  'ReadStage',
  'RequestResponseStage',
  'ValidationExecutor'
]

#
# Cassandra Metrics
#
class CheckCassandraTPStats < Sensu::Plugin::Check::CLI
  option :hostname,
         short: '-h HOSTNAME',
         long: '--host HOSTNAME',
         description: 'cassandra hostname',
         default: 'localhost'

  option :port,
         short: '-P PORT',
         long: '--port PORT',
         description: 'cassandra JMX port',
         default: '7199'

  option :threadpool,
         short: '-T THREADPOOL',
         long: '--threadpool THREADPOOL',
         description: 'Thread Pool Stage Name'

  option :warning_pending,
         short: '-w WARNING_PENDING',
         long:  '--warning_pending WARNING_PENDING',
         description: 'Warning level for Pending events',
         default: -1
  
  option :crit_pending,
         short: '-c CRIT_PENDING',
         long: '--crit_pending CRIT_PENDING',
         description: 'Critical level for Pending events',
         default: 15

  option :warning_blocked,
         short: '-w WARNING_BLOCKED',
         long:  '--warning_blocked WARNING_BLOCKED',
         description: 'Warning level for Blocked events',
         default: -1
  
  option :crit_blocked,
         short: '-C CRIT_BLOCKED',
         long: '--crit_blocked CRIT_BLOCKED',
         description: 'Critical level for Pending events',
         default: 0 

  # convert_to_bytes(512, 'KB') => 524288
  # convert_to_bytes(1, 'MB') => 1048576
  def convert_to_bytes(size, unit)
    size.to_f * UNITS_FACTOR[unit]
  end

  # execute cassandra's nodetool and return output as string
  def nodetool_cmd(cmd)
    `nodetool -h #{config[:hostname]} -p #{config[:port]} #{cmd}`
  end

  # nodetool -h localhost tpstats:
  # Pool Name                    Active   Pending      Completed   Blocked  All time blocked
  # ReadStage                         0         0         282971         0                 0
  # RequestResponseStage              0         0          32926         0                 0
  # MutationStage                     0         0        3216105         0                 0
  # ReadRepairStage                   0         0              0         0                 0
  # ReplicateOnWriteStage             0         0              0         0                 0
  # GossipStage                       0         0              0         0                 0
  # AntiEntropyStage                  0         0              0         0                 0
  # MigrationStage                    0         0            188         0                 0
  # MemtablePostFlusher               0         0            110         0                 0
  # StreamStage                       0         0              0         0                 0
  # FlushWriter                       0         0            110         0                 0
  # MiscStage                         0         0              0         0                 0
  # InternalResponseStage             0         0            179         0                 0
  # HintedHandoff                     0         0              0         0                 0
  #
  # Message type           Dropped
  # RANGE_SLICE                  0
  # READ_REPAIR                  0
  # BINARY                       0
  # READ                         0
  # MUTATION                     0
  # REQUEST_RESPONSE             0
  def parse_tpstats
    tpstats = nodetool_cmd('tpstats')
    tpstats.each_line do |line|
      next if line.match(/^Pool Name/)
      next if line.match(/^Message type/)

      if m = line.match(/^#{config[:threadpool]}\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)$/)
        (active, pending, completed, blocked) = m.captures

        # Build output hash
        thread_attr = {:thread => 
                        { :stage => config[:threadpool], 
                          :active => active, 
                          :pending => pending, 
                          :completed => completed, 
                          :blocked => blocked}} 

        critical thread_attr.to_json if (pending.to_i >= config[:crit_pending].to_i && config[:crit_pending].to_i >= 0) || (blocked.to_i > config[:crit_blocked].to_i && config[:crit_blocked].to_i >= 0)
        warning thread_attr.to_json if (pending.to_i >= config[:warning_pending].to_i && config[:warning_pending].to_i >= 0) || (blocked.to_i > config[:warning_blocked].to_i && config[:warning_blocked].to_i >= 0)
        ok thread_attr.to_json
      
      end
    end
  end


  def run
    @timestamp = Time.now.to_i

    unknown "ERROR: No threadpool specified" if config[:threadpool].nil?
    unknown "ERROR: Invalid Threadpool Specified" unless THREADPOOLS.include? config[:threadpool] 

    parse_tpstats 

    ok
  end
end
