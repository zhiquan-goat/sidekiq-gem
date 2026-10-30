# frozen_string_literal: true

module SidekiqRuntimeSpecSupport
  PRIMARY_QUEUE = 'sneakers_spec_reliable_primary'
  PRIORITY_QUEUE = 'sneakers_spec_reliable_priority'
  JOB_CLASS_NAME = 'SpecReliableJob'

  public_constant :PRIMARY_QUEUE, :PRIORITY_QUEUE, :JOB_CLASS_NAME

  def register_runtime_owner(config, owner = Object.new.extend(Sidekiq::Component).identity)
    config.redis do |redis|
      redis.hset(owner, 'beat', Time.now.to_f)
      redis.expire(owner, 60)
    end
    owner
  end

  def stub_sidekiq_runtime_job
    stub_const(JOB_CLASS_NAME, Class.new do
      include Sidekiq::Job
    end)
  end

  def build_sidekiq_runtime_business_payload(**attributes)
    attributes = {
      chain_id: 'chain-runtime',
      logical_queue: 'ordinary',
      subject_id: 123,
      recovered: false
    }.merge(attributes)

    {
      'valid' => true,
      'recovered' => attributes.fetch(:recovered),
      'chain_id' => attributes.fetch(:chain_id),
      'logical_queue' => attributes.fetch(:logical_queue),
      'subject_id' => attributes.fetch(:subject_id)
    }
  end

  def build_sidekiq_runtime_job_payload(**attributes)
    attributes = {
      jid: 'jid-runtime',
      physical_queue: PRIMARY_QUEUE,
      business_payload: build_sidekiq_runtime_business_payload
    }.merge(attributes)

    {
      'class' => JOB_CLASS_NAME,
      'queue' => attributes.fetch(:physical_queue),
      'jid' => attributes.fetch(:jid),
      'retry' => 1,
      'dead' => true,
      'args' => [attributes.fetch(:business_payload)]
    }
  end

  def with_wrong_type_sidekiq_queue_directory(config)
    key = SidekiqRuntime::QueueKeys.directory_key
    previous_members = config.redis { |redis| redis.smembers(key) }
    config.redis do |redis|
      redis.del(key)
      redis.set(key, 'wrong-type')
    end
    yield
  ensure
    if key
      config.redis do |redis|
        redis.del(key)
        redis.sadd(key, previous_members) if previous_members&.any?
      end
    end
  end
end

RSpec.configure do |config|
  config.include SidekiqRuntimeSpecSupport
end
