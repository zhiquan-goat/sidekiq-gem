# frozen_string_literal: true

require 'digest'
require 'json'
require 'logger'
require 'goat_sidekiq_runtime/active_job'

class SidekiqRuntimeProcessAJ < ActiveJob::Base
  runtime_uniqueness enqueue_ttl: 30,
    runtime_ttl: Integer(ENV.fetch('SIDEKIQ_RUNTIME_PROCESS_RUNTIME_TTL', '1')),
    key: ->(job) { job.arguments.first.fetch('id') }
end

module SidekiqRuntimeRedisProbe
  module_function

  def call(action, payload)
    case action
    when 'cleanup'
      cleanup(payload)
    when 'enqueue'
      enqueue(payload)
    when 'rate-limit-update'
      rate_limit_update(payload)
    when 'ping'
      redis { |connection| {ping: connection.call('PING')} }
    when 'save'
      redis { |connection| {save: connection.call('SAVE')} }
    when 'server-config'
      server_config
    when 'snapshot'
      snapshot(payload)
    else
      raise ArgumentError, "unknown Redis probe action: #{action}"
    end
  end

  def config
    @config ||= Sidekiq::Config.new.tap do |value|
      value.logger = Logger.new(File::NULL)
      value.redis = {
        url: ENV.fetch('SIDEKIQ_REDIS_URL')
      }
    end
  end

  def redis(&block)
    config.redis(&block)
  end

  def enqueue(payload)
    queue = payload.fetch('queue')
    item = {
      'class' => 'SidekiqRuntimeProcessJob',
      'queue' => queue,
      'args' => [payload.fetch('arguments')],
      'retry' => false,
      'dead' => true
    }
    jid = if payload['active_job']
      SidekiqRuntimeProcessAJ.queue_adapter = ActiveJob::QueueAdapters::SidekiqRuntimeAdapter.new(config: config)
      SidekiqRuntimeProcessAJ.logger = Logger.new(File::NULL)
      SidekiqRuntime.install_producer!
      SidekiqRuntimeProcessAJ.set(queue: queue).perform_later(payload.fetch('arguments')).provider_job_id
    else
      Sidekiq::Client.new(config: config).push(item)
    end
    raw = redis do |connection|
      connection.call('LRANGE', "queue:#{queue}", 0, -1).find do |candidate|
        JSON.parse(candidate)['jid'] == jid
      end
    end
    {jid: jid, raw: raw}
  end

  def rate_limit_update(payload)
    queue = payload.fetch('queue')
    service = SidekiqRuntime::QueueRateLimit.new(config)
    change = if payload.fetch('enabled')
      service.enable(queue, rate: payload.fetch('rate'), burst: payload.fetch('burst'))
    else
      service.disable(queue)
    end
    {
      before: change.before&.to_h,
      after: change.after&.to_h
    }
  end

  def snapshot(payload)
    queues = payload.fetch('queues')
    rate_limits = SidekiqRuntime::QueueRateLimit.new(config).get_many(queues)
    redis do |connection|
      queue_states = queues.to_h do |queue|
        inflight_key = SidekiqRuntime::QueueKeys.inflight_key(queue)
        members = scored_members(connection.call('ZRANGE', inflight_key, 0, -1, 'WITHSCORES'))
        owners = connection.call('HGETALL', "#{inflight_key}:owners")
        [queue, {
          ready: connection.call('LRANGE', "queue:#{queue}", 0, -1),
          inflight: members.map do |member, score|
            {
              member: member,
              raw: member.split("\n", 2)[1],
              score: Float(score),
              owner: hash_value(owners, member)
            }
          end,
          owners: normalized_hash(owners),
          owner_heartbeats: normalized_hash(owners).values.uniq.to_h do |owner|
            [owner, {ttl_ms: connection.call('PTTL', owner), beat: connection.call('HGET', owner, 'beat')}]
          end,
          registered: connection.call('SISMEMBER', 'queues', queue) == 1,
          directory_registered: connection.call('SISMEMBER', SidekiqRuntime::QueueKeys.directory_key, queue) == 1,
          rate_limit: rate_limits.fetch(queue)&.to_h
        }]
      end
      sorted = %w[retry schedule dead].to_h do |key|
        values = connection.call('ZRANGE', key, 0, -1).select do |raw|
          queues.include?(JSON.parse(raw)['queue'])
        rescue JSON::ParserError, TypeError
          false
        end
        [key, values]
      end
      {
        queues: queue_states,
        sorted_sets: sorted,
        processes: relevant_processes(connection, queues)
      }
    end
  end

  def cleanup(payload)
    queues = payload.fetch('queues')
    redis do |connection|
      keys = queues.flat_map do |queue|
        inflight = SidekiqRuntime::QueueKeys.inflight_key(queue)
        ["queue:#{queue}", inflight, "#{inflight}:owners", SidekiqRuntime::QueueKeys.rate_limit_key(queue)]
      end
      connection.call('DEL', *keys) unless keys.empty?
      connection.call('SREM', 'queues', *queues) unless queues.empty?
      connection.call('SREM', SidekiqRuntime::QueueKeys.directory_key, *queues) unless queues.empty?
      %w[retry schedule dead].each do |key|
        connection.call('ZRANGE', key, 0, -1).each do |raw|
          job = JSON.parse(raw)
          connection.call('ZREM', key, raw) if queues.include?(job['queue'])
        rescue JSON::ParserError, TypeError
          nil
        end
      end
      relevant_processes(connection, queues).each do |process|
        identity = process.fetch(:identity)
        connection.call('SREM', 'processes', identity)
        connection.call('DEL', identity, "#{identity}:work", "#{identity}-signals")
      end
    end
    {cleaned: true}
  end

  def server_config
    redis do |connection|
      info = parse_info(connection.call('INFO', 'server'))
      {
        redis_version: info['redis_version'],
        process_id: Integer(info.fetch('process_id')),
        run_id: info.fetch('run_id'),
        save: config_value(connection.call('CONFIG', 'GET', 'save'), 'save'),
        appendonly: config_value(connection.call('CONFIG', 'GET', 'appendonly'), 'appendonly')
      }
    end
  end

  def relevant_processes(connection, queues)
    connection.call('SMEMBERS', 'processes').filter_map do |identity|
      raw = connection.call('HGET', identity, 'info')
      next unless raw

      info = JSON.parse(raw)
      next if (Array(info['queues']) & queues).empty?

      {identity: identity, info: info}
    rescue JSON::ParserError, TypeError
      nil
    end
  end

  def scored_members(values)
    return values if values.first.is_a?(Array)

    values.each_slice(2).to_a
  end

  def normalized_hash(value)
    value.is_a?(Hash) ? value : value.each_slice(2).to_h
  end

  def hash_value(value, key)
    normalized_hash(value)[key]
  end

  def config_value(value, key)
    normalized_hash(value)[key]
  end

  def parse_info(value)
    value.lines(chomp: true).filter_map do |line|
      next if line.empty? || line.start_with?('#')

      line.split(':', 2)
    end.to_h
  end
end

action = ARGV.fetch(0)
payload = ARGV[1] ? JSON.parse(ARGV.fetch(1)) : {}
puts JSON.generate(SidekiqRuntimeRedisProbe.call(action, payload))
